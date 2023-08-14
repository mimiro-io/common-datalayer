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

type Config struct {
	SystemConfig       *SystemConfig
	DatasetDefinitions []*DatasetDefinition
}

func (c *Config) Load(args []string) error {
	for _, arg := range args {
		// TODO each arg is a file location? Load and merge?
		println("not loading ", arg)
		// for now, just add some values
		c.SystemConfig.Properties["PORT"] = "8080"
		c.SystemConfig.Properties["SERVICE_NAME"] = "UNKNOWN_SERVICE"
	}
	return nil
}

func (c *Config) GetDatasetDefinition(dataset string) *DatasetDefinition {
	for _, def := range c.DatasetDefinitions {
		if def.DatasetName == dataset {
			return def
		}
	}
	return nil
}

func NewConfig() *Config {
	res := &Config{}
	res.SystemConfig = &SystemConfig{}
	res.SystemConfig.Properties = make(map[string]any)
	res.DatasetDefinitions = make([]*DatasetDefinition, 0)
	return res
}

type Initialization interface {
	Initialize(config *Config, logger Logger) error
}

func ReadConfig(data io.Reader) *Config {
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