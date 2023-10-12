package common_datalayer

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

type Config struct {
	ConfigPath         string               // set by service runner
	NativeSystemConfig NativeSystemConfig   `json:"system_config"`
	LayerServiceConfig *LayerServiceConfig  `json:"layer_config"`
	DatasetDefinitions []*DatasetDefinition `json:"dataset_definitions"`
}

type NativeSystemConfig map[string]any

type LayerServiceConfig struct {
	ServiceName           string         `json:"service_name"`
	Port                  string         `json:"port"`
	ConfigRefreshInterval string         `json:"config_refresh_interval"`
	LogLevel              string         `json:"log_level"`
	LogFormat             string         `json:"log_format"`
	StatsdEnabled         bool           `json:"statsd_enabled"`
	StatsdAgentAddress    string         `json:"statsd_agent_address"`
	Custom                map[string]any `json:"custom"`
}

type DatasetDefinition struct {
	DatasetName           string                 `json:"name"`
	SourceConfig          map[string]any         `json:"source_config"`
	IncomingMappingConfig *IncomingMappingConfig `json:"incoming_mapping_config"`
	OutgoingMappingConfig *OutgoingMappingConfig `json:"outgoing_mapping_config"`
}

// the operations can be one of the following: concat, split, replace, trim, tolower, toupper, regex, slice
type PropertyConstructor struct {
	PropertyName string   `json:"property"`
	Operation    string   `json:"operation"`
	Arguments    []string `json:"args"`
}

type IncomingMappingConfig struct {
	MapNamed         bool                           `json:"map_named"` // if true then try and lookup entity properties based on the item property name and the BaseURI prefix
	PropertyMappings []*EntityToItemPropertyMapping `json:"property_mappings"`
	BaseURI          string                         `json:"base_uri"`
	Custom           map[string]any                 `json:"custom"`
}

type OutgoingMappingConfig struct {
	BaseURI          string                         `json:"base_uri"` // used when mapping all
	Constructions    []*PropertyConstructor         `json:"constructions"`
	PropertyMappings []*ItemToEntityPropertyMapping `json:"property_mappings"`
	MapAll           bool                           `json:"map_all"` // if true, all properties are mapped
	Custom           map[string]any                 `json:"custom"`
}

type EntityToItemPropertyMapping struct {
	Custom               map[string]any
	Required             bool   `json:"required"`
	EntityProperty       string `json:"entity_property"`
	Property             string `json:"property"`
	Datatype             string `json:"datatype"`
	IsReference          bool   `json:"is_reference"`
	IsIdentity           bool   `json:"is_identity"`
	DefaultValue         string `json:"default_value"`
	StripReferencePrefix bool   `json:"strip_ref_prefix"`
}

type ItemToEntityPropertyMapping struct {
	Custom          map[string]any
	Required        bool   `json:"required"`
	EntityProperty  string `json:"entity_property"`
	Property        string `json:"property"`
	Datatype        string `json:"datatype"`
	IsReference     bool   `json:"is_reference"`
	URIValuePattern string `json:"uri_value_pattern"`
	IsIdentity      bool   `json:"is_identity"`
	DefaultValue    any    `json:"default_value"`
}

/******************************************************************************/

func (c *Config) GetDatasetDefinition(dataset string) *DatasetDefinition {
	for _, def := range c.DatasetDefinitions {
		if def.DatasetName == dataset {
			return def
		}
	}
	return nil
}

func (c *Config) equals(conf *Config) bool {
	return reflect.DeepEqual(c, conf)
}

func newConfig() *Config {
	return &Config{}
}

func readConfig(data io.Reader) (*Config, error) {
	config := newConfig()
	s, err := io.ReadAll(data)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(s, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func loadConfig(configPath string) (*Config, error) {
	c := newConfig()
	c.ConfigPath = configPath

	// configPath must refer to a folder
	// iterate all files in the folder that ends with .json
	// load each file and merge into the config
	files, err := os.ReadDir(configPath)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".json") {
			var reader io.Reader
			// load from file
			var err error
			reader, err = os.Open(filepath.Join(configPath, file.Name()))
			if err != nil {
				return nil, err
			}
			config, err := readConfig(reader)
			if err != nil {
				return nil, err
			}
			addConfig(c, config)
		}
	}

	// Initialize any missing config components as some values may get set later
	// and the config is compared to see if it has changed, so need to make sure they exist
	if c.LayerServiceConfig == nil {
		c.LayerServiceConfig = &LayerServiceConfig{}
	}

	if c.NativeSystemConfig == nil {
		c.NativeSystemConfig = make(map[string]any)
	}

	if c.DatasetDefinitions == nil {
		c.DatasetDefinitions = make([]*DatasetDefinition, 0)
	}

	addEnvOverrides(c)
	return c, nil
}

func addEnvOverrides(c *Config) {
	val, found := os.LookupEnv("PORT")
	if found {
		c.LayerServiceConfig.Port = val
	}

	val, found = os.LookupEnv("CONFIG_REFRESH_INTERVAL")
	if found {
		c.LayerServiceConfig.ConfigRefreshInterval = val
	}

	val, found = os.LookupEnv("SERVICE_NAME")
	if found {
		c.LayerServiceConfig.ServiceName = val
	}

	val, found = os.LookupEnv("STATSD_ENABLED")
	if found {
		c.LayerServiceConfig.StatsdEnabled = val == "true"
	}

	val, found = os.LookupEnv("STATSD_AGENT_ADDRESS")
	if found {
		c.LayerServiceConfig.StatsdAgentAddress = val
	}

	val, found = os.LookupEnv("LOG_LEVEL")
	if found {
		c.LayerServiceConfig.LogLevel = val
	}

	val, found = os.LookupEnv("LOG_FORMAT")
	if found {
		c.LayerServiceConfig.LogFormat = val
	}
}

func addConfig(mainConfig *Config, partialConfig *Config) {
	// system config can only be defined once any repeats replace it
	if partialConfig.NativeSystemConfig != nil {
		mainConfig.NativeSystemConfig = partialConfig.NativeSystemConfig
	}

	// layer service config can only be defined once any repeats replace it
	if partialConfig.LayerServiceConfig != nil {
		mainConfig.LayerServiceConfig = partialConfig.LayerServiceConfig
	}

	// initialise if needed
	if mainConfig.DatasetDefinitions == nil {
		mainConfig.DatasetDefinitions = make([]*DatasetDefinition, 0)
	}

	if partialConfig.DatasetDefinitions != nil {
		for _, def := range partialConfig.DatasetDefinitions {
			var exists bool
			for _, existingDef := range mainConfig.DatasetDefinitions {
				if existingDef.DatasetName == def.DatasetName {
					exists = true
					existingDef.SourceConfig = def.SourceConfig
					existingDef.IncomingMappingConfig = def.IncomingMappingConfig
					existingDef.OutgoingMappingConfig = def.OutgoingMappingConfig
				}
				break
			}
			if !exists {
				mainConfig.DatasetDefinitions = append(mainConfig.DatasetDefinitions, def)
			}
		}
	}
}
