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
	NativeSystemConfig NativeSystemConfig   `json:"system_configuration"`
	LayerServiceConfig *LayerServiceConfig  `json:"layer_configuration"`
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
	DatasetName   string         `json:"dataset_name"`
	SourceConfig  map[string]any `json:"source_configuration"`
	MappingConfig *MappingConfig `json:"mapping_configuration"`
}

type MappingConfig struct {
	BaseURI          string                 `json:"base_uri"` // used when mapping all
	Constructions    []*PropertyConstructor `json:"constructions"`
	PropertyMappings []*PropertyMapping     `json:"mappings"`
	MapAllFromItem   bool                   `json:"map_all_from_item"` // if true, all properties are mapped from an item
	MapAllToItem     bool                   `json:"map_to_item"`       // if true, all properties are mapped into an item
}

// the operations can be one of the following: concat, split, replace, trim, tolower, toupper, regex, slice
type PropertyConstructor struct {
	PropertyName string   `json:"property"`
	Operation    string   `json:"operation"`
	Arguments    []string `json:"args"`
}

type PropertyMapping struct {
	Custom               map[string]any
	EntityProperty       string `json:"entity_property"`
	Property             string `json:"property"`
	Datatype             string `json:"datatype"`
	IsReference          bool   `json:"is_reference"`
	UrlValuePattern      string `json:"url_value_pattern"`
	IsIdentity           bool   `json:"is_identity"`
	DefaultPropertyValue string `json:"default_value"`
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
					existingDef.MappingConfig = def.MappingConfig
				}
				break
			}
			if !exists {
				mainConfig.DatasetDefinitions = append(mainConfig.DatasetDefinitions, def)
			}
		}
	}
}
