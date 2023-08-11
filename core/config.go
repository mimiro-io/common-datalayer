package core

import "io"

type DatasetDefinition struct {
	DatasetName  string
	SourceConfig map[string]any
	Mappings     []*EntityPropertyMapping
}

func (d DatasetDefinition) StripProps() bool {
	val, exists := d.SourceConfig["stripProps"]
	if exists {
		return val.(bool)
	}
	return false
}

type EntityPropertyMapping struct {
	// hang on to the raw config as it allows for local extensions
	Raw map[string]any

	EntityProperty  string
	Property        string
	Datatype        string
	IsReference     bool
	UrlValuePattern string
	IsIdentity      bool
}

type DatasetDefinitions struct {
	List []*DatasetDefinition
}

type Config interface {
	SystemConfig() *SystemConfig
	DatasetDefinitions() *DatasetDefinitions
	GetDatasetDefinition(dataset string) *DatasetDefinition
}

type LoadableConfig interface {
	Config
	Load(args []string) error
}

type DefaultConfig struct {
	systemConfig       *SystemConfig
	datasetDefinitions *DatasetDefinitions
}

func (c *DefaultConfig) Load(args []string) error {
	for _, arg := range args {
		// TODO each arg is a file location? Load and merge?
		println("not loading ", arg)
		// for now, just add some values
		c.systemConfig.Properties["PORT"] = "8080"
		c.systemConfig.Properties["SERVICE_NAME"] = "UNKNOWN_SERVICE"
	}
	return nil
}

func (c *DefaultConfig) SystemConfig() *SystemConfig {
	return c.systemConfig
}
func (c *DefaultConfig) DatasetDefinitions() *DatasetDefinitions {
	return c.datasetDefinitions
}
func (c *DefaultConfig) GetDatasetDefinition(dataset string) *DatasetDefinition {
	for _, def := range c.datasetDefinitions.List {
		if def.DatasetName == dataset {
			return def
		}
	}
	return nil
}

func NewConfig() LoadableConfig {
	res := &DefaultConfig{}
	res.systemConfig = &SystemConfig{}
	res.systemConfig.Properties = make(map[string]any)
	res.datasetDefinitions = &DatasetDefinitions{make([]*DatasetDefinition, 0)}
	return res
}

type ReInitializable interface {
	Initialize(config Config, logger Logger) error
}

func ReadConfig(data io.Reader) Config {
	return nil
}

func stringOr(configMap map[string]any, key, defaultValue string) string {
	val, exists := configMap[key]
	if exists {
		return val.(string)
	}
	// default
	return defaultValue
}