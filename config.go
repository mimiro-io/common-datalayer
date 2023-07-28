package common_datalayer

import "io"

type SystemConfig struct {
	Properties []map[string]interface{}
}

type DatasetDefinition struct {
	SourceConfig map[string]interface{}
	Mappings     []*EntityPropertyMapping
}

type EntityPropertyMapping struct {
	// hang on to the raw config as it allows for local extensions
	Raw map[string]interface{}

	EntityProperty  string
	Property        string
	Datatype        string
	IsReference     bool
	UrlValuePattern string
	IsIdentity      bool
}

type Config interface {
	SystemConfig() *SystemConfig
	DatasetDefinitions() []*DatasetDefinition
	GetDatasetDefinition(dataset string) *DatasetDefinition
}

func ReadConfig(data io.Reader) Config {
	return nil
}
