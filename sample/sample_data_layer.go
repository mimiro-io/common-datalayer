package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	layer "github.com/mimiro-io/common-datalayer"
)

// EnrichConfig is a function that can be used to enrich the config by reading additional files or environment variables
func EnrichConfig(config *layer.Config) error {
	config.DatasetDefinitions = append(config.DatasetDefinitions, &layer.DatasetDefinition{
		DatasetName:  "sample",
		SourceConfig: map[string]any{"stripProps": "true"},
		Mappings: []*layer.EntityPropertyMapping{
			{
				EntityProperty:  "ID",
				Property:        "ID",
				UrlValuePattern: "http://sample/{id}",
				IsIdentity:      true,
			},
			{
				EntityProperty: "name",
				Property:       "name",
			},
		},
	})
	//config.ApplicationConfig["env"] = "local"
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

func (dl *SampleDataLayer) GetDataset(dataset string) layer.Dataset {
	ds, found := dl.datasets[dataset]
	if found {
		return ds
	}
	return nil
}

func (dl *SampleDataLayer) ListDatasetNames() []string {
	// create a slice of strings to hold the dataset names
	var datasetNames []string

	// add dataset names from the map to the slice
	for key := range dl.datasets {
		datasetNames = append(datasetNames, key)
	}
	return datasetNames
}

// no shutdown required
func (dl *SampleDataLayer) Stop(_ context.Context) error { return nil }

// NewSampleDataLayer is a factory function that creates a new instance of the sample data layer
// In this example we use it to populate the sample dataset with some data
func NewSampleDataLayer(conf *layer.Config, logger layer.Logger, metrics layer.Metrics) (layer.DataLayerService, error) {
	sampleDataLayer := &SampleDataLayer{config: conf, logger: logger, metrics: metrics}

	// initialize the datasets
	sampleDataLayer.datasets = make(map[string]*SampleDataset)

	// create a sample dataset
	sampleDataLayer.datasets["sample"] = &SampleDataset{Name: "sample"}
	// loop to create 20 objects
	for i := 0; i < 20; i++ {
		// create a data object
		dataObject := DataObject{ID: "ID" + strconv.Itoa(i), Props: make(map[string]any)}

		// add some properties to the data object
		dataObject.Props["name"] = "name" + strconv.Itoa(i)
		dataObject.Props["description"] = "description" + strconv.Itoa(i)

		// add the data object to the sample dataset
		sampleDataLayer.datasets["sample"].data = append(sampleDataLayer.datasets["sample"].data, dataObject.AsBytes())
	}
	logger.Info(fmt.Sprintf("Initialized sample layer with %v objects", len(sampleDataLayer.datasets["sample"].data)))
	sampleDataLayer.UpdateConfiguration(conf)
	return sampleDataLayer, nil
}

// Initialize is called by the core service when the configuration is loaded.
// can be called many times if the configuration is reloaded
func (dl *SampleDataLayer) UpdateConfiguration(config *layer.Config) error {
	// just update mappings in this sample. no new dataset definitions are expected
	for k, v := range dl.datasets {
		for _, dsd := range config.DatasetDefinitions {
			if k == dsd.DatasetName {
				v.mappings = dsd.Mappings
			}
		}
	}
	return nil
}

/*********************************************************************************************************************/

// SampleDataset is a sample implementation of the Dataset interface, it provides a simple in-memory dataset in this case
type SampleDataset struct {
	Name     string
	mappings []*layer.EntityPropertyMapping
	data     [][]byte
}

func (ds *SampleDataset) WriteItem(item layer.Item) error {
	do := &DataObject{}
	do.ID = item.GetValue("ID").(string)
	do.Props = item.GetRaw()
	ds.data = append(ds.data, do.AsBytes())
	return nil
}

func (ds *SampleDataset) GetName() string {
	return ds.Name
}

// GetChanges returns an iterator over the changes since the given timestamp,
// The implementation uses the provided MappingEntityIterator and a custom DataObjectIterator
// to map the data objects to entities
func (ds *SampleDataset) GetChanges(since string, take int, _ bool) (layer.EntityIterator, error) {
	data := ds.data
	entityIterator := layer.NewMappingEntityIterator(
		ds.mappings,
		NewDataObjectIterator(data),
		nil)
	return entityIterator, nil
}

func (ds *SampleDataset) GetEntities(since string, take int) (layer.EntityIterator, error) {
	return ds.GetChanges(since, take, true)
}

func (ds *SampleDataset) BeginFullSync() error {
	return nil
}

func (ds *SampleDataset) CompleteFullSync() error {
	return nil
}

func (ds *SampleDataset) CancelFullSync() error {
	return nil
}

func (ds *SampleDataset) Description() map[string]interface{} {
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

func (d *DataObject) AsBytes() []byte {
	b, _ := json.Marshal(d)
	return b
}

/*********************************************************************************************************************/

// DataObjectIterator is a sample implementation of the ItemIterator interface
// This is the glue between the data objects and the entity mapping
type DataObjectIterator struct {
	objects [][]byte
	pos     int
}

func (doi *DataObjectIterator) Token() string {
	//TODO implement me
	panic("implement me")
}

func (doi *DataObjectIterator) Close() {
	//TODO implement me
	panic("implement me")
}

func NewDataObjectIterator(objects [][]byte) *DataObjectIterator {
	doi := &DataObjectIterator{}
	doi.objects = objects
	doi.pos = 0
	return doi
}

func (doi *DataObjectIterator) Next() layer.Item {
	if doi.pos >= len(doi.objects) {
		return nil
	}
	b := doi.objects[doi.pos]
	doi.pos++
	obj := DataObject{}
	err := json.Unmarshal(b, &obj)
	if err != nil {
		panic(err)
	}
	res := &layer.DataItem{}
	res.PutRaw(obj.Props)
	res.SetValue("ID", obj.ID)
	return res
}