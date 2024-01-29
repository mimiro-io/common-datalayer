package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	layer "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

// EnrichConfig is a function that can be used to enrich the config by reading additional files or environment variables
func EnrichConfig(config *layer.Config) error {
	config.NativeSystemConfig["env"] = "local"
	layer.BuildNativeSystemEnvOverrides(
		layer.Env("db_name", true),           // required env var. will fail if neiter "db_name" in json nor "DB_NAME" in env
		layer.Env("db_user", true, "dbUser"), // override jsonkey with "dbUser"
		layer.Env("db_pwd", true),
		layer.Env("db_timeout"), // optional env var. will not fail if missing in both json and ENV
	)(config)
	return nil
}

/*********************************************************************************************************************/

// SampleDataLayer is a sample implementation of the DataLayer interface
type SampleDataLayer struct {
	config   *layer.Config
	logger   layer.Logger
	metrics  layer.Metrics
	datasets map[string]*SampleDataset
}

func (dl *SampleDataLayer) Dataset(dataset string) (layer.Dataset, layer.LayerError) {
	ds, found := dl.datasets[dataset]
	if found {
		return ds, nil
	}
	return nil, layer.Errorf(layer.LayerErrorBadParameter, "dataset %s not found", dataset)
}

func (dl *SampleDataLayer) DatasetDescriptions() []*layer.DatasetDescription {
	var datasetDescriptions []*layer.DatasetDescription
	for key := range dl.datasets {
		datasetDescriptions = append(datasetDescriptions, &layer.DatasetDescription{Name: key})
	}
	return datasetDescriptions
}

// no shutdown required
func (dl *SampleDataLayer) Stop(_ context.Context) error { return nil }

// NewSampleDataLayer is a factory function that creates a new instance of the sample data layer
// In this example we use it to populate the sample dataset with some data
func NewSampleDataLayer(conf *layer.Config, logger layer.Logger, metrics layer.Metrics) (layer.DataLayerService, error) {
	sampleDataLayer := &SampleDataLayer{config: conf, logger: logger, metrics: metrics}

	// initialize the datasets
	sampleDataLayer.datasets = make(map[string]*SampleDataset)

	// iterate over the dataset definitions in the configuration
	for _, dsd := range conf.DatasetDefinitions {
		mapper := layer.NewMapper(logger, dsd.IncomingMappingConfig, dsd.OutgoingMappingConfig)
		sampleDataLayer.datasets[dsd.DatasetName] = &SampleDataset{dsName: dsd.DatasetName, mapper: mapper}
	}

	// loop to create 20 objects
	for i := 0; i < 20; i++ {
		// create a data object
		dataObject := &DataObject{ID: "ID" + strconv.Itoa(i), Props: make(map[string]any)}

		// add some properties to the data object
		dataObject.Props["name"] = "name" + strconv.Itoa(i)
		dataObject.Props["description"] = "description" + strconv.Itoa(i)

		// add the data object to the sample dataset
		sampleDataLayer.datasets["sample"].data = append(sampleDataLayer.datasets["sample"].data, dataObject)
	}

	logger.Info(fmt.Sprintf("Initialized sample layer with %v objects", len(sampleDataLayer.datasets["sample"].data)))
	err := sampleDataLayer.UpdateConfiguration(conf)
	if err != nil {
		return nil, err
	}
	return sampleDataLayer, nil
}

func (dl *SampleDataLayer) UpdateConfiguration(config *layer.Config) layer.LayerError {
	// just update mappings in this sample. no new dataset definitions are expected
	for k, v := range dl.datasets {
		for _, dsd := range config.DatasetDefinitions {
			if k == dsd.DatasetName {
				mapper := layer.NewMapper(dl.logger, dsd.IncomingMappingConfig, dsd.OutgoingMappingConfig)
				v.mapper = mapper
			}
		}
	}
	return nil
}

/*********************************************************************************************************************/

// SampleDataset is a sample implementation of the Dataset interface, it provides a simple in-memory dataset in this case
type SampleDataset struct {
	dsName string
	mapper *layer.Mapper
	data   []*DataObject
}

func (ds *SampleDataset) Name() string {
	return ds.dsName
}

func (ds *SampleDataset) Changes(since string, take int, _ bool) (layer.EntityIterator, layer.LayerError) {
	return &SampleEntityIterator{data: ds.data, mapper: ds.mapper}, nil
}

func (ds *SampleDataset) Entities(since string, take int) (layer.EntityIterator, layer.LayerError) {
	return ds.Changes(since, take, true)
}

func (ds *SampleDataset) MetaData() map[string]any {
	return nil
}

func (ds *SampleDataset) FullSync(_ context.Context, _ layer.BatchInfo) (layer.DatasetWriter, layer.LayerError) {
	return nil, layer.Err(errors.New("full sync not implemented"), layer.LayerNotSupported)
}

func (ds *SampleDataset) Incremental(ctx context.Context) (layer.DatasetWriter, layer.LayerError) {
	return NewSampleDatasetWriter(ds, ds.mapper, ctx, layer.BatchInfo{}), nil
}

type SampleEntityIterator struct {
	mapper *layer.Mapper
	data   []*DataObject
	index  int
}

func (sei *SampleEntityIterator) Next() (*egdm.Entity, layer.LayerError) {
	for sei.index < len(sei.data) {
		dataObject := sei.data[sei.index]
		sei.index++
		entity := &egdm.Entity{Properties: make(map[string]any)}
		err := sei.mapper.MapItemToEntity(dataObject, entity)
		if err != nil {
			return nil, layer.Errorf(layer.LayerErrorInternal, "error mapping data object %s", dataObject.ID)
		}
		return entity, nil
	}
	return nil, nil
}

func (sei *SampleEntityIterator) Context() *egdm.Context {
	return nil
}

func (sei *SampleEntityIterator) Token() (*egdm.Continuation, layer.LayerError) {
	return nil, nil
}

func (sei *SampleEntityIterator) Close() layer.LayerError {
	return nil
}

type SampleDatasetWriter struct {
	ds        *SampleDataset
	mapper    *layer.Mapper
	ctx       context.Context
	batchInfo layer.BatchInfo
}

func NewSampleDatasetWriter(ds *SampleDataset, mapper *layer.Mapper, ctx context.Context, batchInfo layer.BatchInfo) *SampleDatasetWriter {
	return &SampleDatasetWriter{ds: ds, mapper: mapper, ctx: ctx, batchInfo: batchInfo}
}

func (sdw *SampleDatasetWriter) Close() layer.LayerError {
	return nil
}

func (sdw *SampleDatasetWriter) Write(entity *egdm.Entity) layer.LayerError {
	// convert to DataObject
	dataObject := &DataObject{ID: entity.ID, Props: make(map[string]any)}
	err := sdw.mapper.MapEntityToItem(entity, dataObject)
	if err != nil {
		return layer.Err(err, layer.LayerErrorInternal)
	}
	sdw.ds.data = append(sdw.ds.data, dataObject)
	return nil
}

/*********************************************************************************************************************/

// DataObject is the row/item type for the sample data layer. it implements the Item interface
// In addition to the Item interface, it also has a dedicated ID field and AsBytes,
// which is used to serialize the item for this specific layer
type DataObject struct {
	ID    string
	Props map[string]any
}

func (do *DataObject) SetValue(name string, value any) {
	if name == "id" {
		do.ID = value.(string)
	} else {
		do.Props[name] = value
	}
}

func (do *DataObject) GetValue(name string) any {
	if name == "id" {
		return do.ID
	}
	return do.Props[name]
}

func (do *DataObject) NativeItem() any {
	return do
}

func (do *DataObject) GetPropertyNames() []string {
	var names []string
	for k := range do.Props {
		names = append(names, k)
	}
	return names
}

/*********************************************************************************************************************/
