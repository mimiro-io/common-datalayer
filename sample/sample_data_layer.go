package main

import (
	"errors"
	. "mimiro.io/common-datalayer"
)

// EnrichConfig is a function that can be used to enrich the config by reading additional files or environment variables
func EnrichConfig(args []string, config *Config) {

}

type dataObject struct {
	id    string
	props map[string]string
}

func (d *dataObject) GetValue(name string) interface{} {
	if name == "id" {
		return d.id
	} else {
		return d.props[name]
	}
}

func (d *dataObject) SetValue(name string, value interface{}) {
	if name == "id" {
		d.id = value.(string)
	} else {
		d.props[name] = value.(string)
	}
}

type SampleDataLayer struct {
	config   Config
	logger   Logger
	metrics  string // todo: add metrics
	datasets map[string]*SampleDataset
}

type SampleDataset struct {
	Name     string
	Mappings []*EntityPropertyMapping
	data     []dataObject
}

// SampleDataset implements the Dataset interface
func (ds *SampleDataset) GetName() string {
	return ds.Name
}

func (ds *SampleDataset) WriteEntities(entities *EntityIterator) error {
	//TODO implement me
	panic("implement me")
}

func (ds *SampleDataset) GetChanges(since string, take int, latestOnly bool) (*EntityIterator, error) {
	data := ds.data
	entityIterator := NewEntityIterator(ds.Mappings, NewDataObjectIterator(since, take, data), nil)
	return entityIterator, nil
}

func (ds *SampleDataset) GetEntities(since string, take int) (*EntityIterator, error) {
	return nil, errors.New("not implemented")
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

func NewSampleDataLayer() DataLayerService {
	sampleDataLayer := &SampleDataLayer{}

	// initialize the datasets
	sampleDataLayer.datasets = make(map[string]*SampleDataset)

	// create a sample dataset
	sampleDataLayer.datasets["sample"] = &SampleDataset{Name: "sample"}
	// loop to create 20 objects
	for i := 0; i < 20; i++ {
		// create a data object
		dataObject := dataObject{id: "id" + string(i), props: make(map[string]string)}

		// add some properties to the data object
		dataObject.props["name"] = "name" + string(i)
		dataObject.props["description"] = "description" + string(i)

		// add the data object to the sample dataset
		sampleDataLayer.datasets["sample"].data = append(sampleDataLayer.datasets["sample"].data, dataObject)
	}
	return sampleDataLayer
}

func (dl *SampleDataLayer) Initialize(config Config, logger Logger) error {
	dl.config = config

	// validate the config is ok
	if dl.config == nil {
		return errors.New("config is nil")
	}

	dl.logger = logger
	return nil
}

func (dl *SampleDataLayer) GetDatasetManager() DatasetManager {
	return dl
}

func (dl *SampleDataLayer) GetDataset(dataset string) Dataset {
	return dl.datasets[dataset]
}

func (dl *SampleDataLayer) ListDatasets() []string {
	// create a slice of strings to hold the dataset names
	var datasetNames []string

	// add dataset names from the map to the slice
	for key := range dl.datasets {
		datasetNames = append(datasetNames, key)
	}
	return datasetNames
}

type DataObjectIterator struct {
	objects []dataObject
	pos     int
	take    int
	taken   int
}

func NewDataObjectIterator(since string, take int, objects []dataObject) *DataObjectIterator {
	doi := &DataObjectIterator{}
	doi.objects = objects
	doi.pos = 0
	return doi
}

func (doi *DataObjectIterator) Next() Item {
	if doi.pos >= len(doi.objects) {
		return nil
	}
	obj := doi.objects[doi.pos]
	doi.pos++

	return &obj
}
