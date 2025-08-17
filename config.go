package common_datalayer

import (
	"encoding/json"
	"fmt"
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
	Custom                map[string]any `json:"custom"`
	ServiceName           string         `json:"service_name"`
	Port                  json.Number    `json:"port"`
	ConfigRefreshInterval string         `json:"config_refresh_interval"`
	LogLevel              string         `json:"log_level"`
	LogFormat             string         `json:"log_format"`
	StatsdAgentAddress    string         `json:"statsd_agent_address"`
	StatsdEnabled         bool           `json:"statsd_enabled"`
}

type DatasetDefinition struct {
	SourceConfig          map[string]any         `json:"source_config"`
	IncomingMappingConfig *IncomingMappingConfig `json:"incoming_mapping_config"`
	OutgoingMappingConfig *OutgoingMappingConfig `json:"outgoing_mapping_config"`
	DatasetName           string                 `json:"name"`
}

// the operations can be one of the following: concat, split, replace, trim, tolower, toupper, regex, slice
type PropertyConstructor struct {
	PropertyName string   `json:"property"`
	Operation    string   `json:"operation"`
	Arguments    []string `json:"args"`
}

type IncomingMappingConfig struct {
	Custom           map[string]any                 `json:"custom"`
	BaseURI          string                         `json:"base_uri"`
	PropertyMappings []*EntityToItemPropertyMapping `json:"property_mappings"`
	MapNamed         bool                           `json:"map_named"`
}

type OutgoingMappingConfig struct {
	Custom           map[string]any                 `json:"custom"`
	BaseURI          string                         `json:"base_uri"`
	Constructions    []*PropertyConstructor         `json:"constructions"`
	PropertyMappings []*ItemToEntityPropertyMapping `json:"property_mappings"`
	MapAll           bool                           `json:"map_all"`
	DefaultType      string                         `json:"default_type"` // the default rdf type if none is specified
}

type EntityToItemPropertyMapping struct {
	Custom               map[string]any
	EntityProperty       string `json:"entity_property"`
	Property             string `json:"property"`
	Datatype             string `json:"datatype"`
	DefaultValue         string `json:"default_value"`
	StripReferencePrefix bool   `json:"strip_ref_prefix"`
	Required             bool   `json:"required"`
	IsIdentity           bool   `json:"is_identity"`
	IsReference          bool   `json:"is_reference"`
	IsDeleted            bool   `json:"is_deleted"`
	IsRecorded           bool   `json:"is_recorded"`
}

type ItemToEntityPropertyMapping struct {
	DefaultValue    any `json:"default_value"`
	Custom          map[string]any
	EntityProperty  string `json:"entity_property"`
	Property        string `json:"property"`
	Datatype        string `json:"datatype"`
	URIValuePattern string `json:"uri_value_pattern"`
	Required        bool   `json:"required"`
	IsIdentity      bool   `json:"is_identity"`
	IsReference     bool   `json:"is_reference"`
	IsDeleted       bool   `json:"is_deleted"`
	IsRecorded      bool   `json:"is_recorded"`
}

/******************************************************************************/
type EnvOverride struct {
	EnvVar   string
	ConfKey  string
	Required bool
}

// Env function to conveniently construct EnvOverride instances
func Env(key string, specs ...any) EnvOverride {
	e := EnvOverride{EnvVar: key}
	for _, spec := range specs {
		switch v := spec.(type) {
		case bool:
			e.Required = v
		case string:
			e.ConfKey = v
		}
	}
	return e
}

// BuildNativeSystemEnvOverrides can be plugged into `WithEnrichConfig`
//
//	it takes a variadic parameter list, each of which declares an environment variable
//	that the layer will try to look up at start, and add to system_config.
func BuildNativeSystemEnvOverrides(envOverrides ...EnvOverride) func(config *Config) error {
	return func(config *Config) error {
		for _, envOverride := range envOverrides {
			upper := strings.ToUpper(envOverride.EnvVar)
			key := strings.ToLower(envOverride.EnvVar)
			if envOverride.ConfKey != "" {
				key = envOverride.ConfKey
			}
			if v, ok := os.LookupEnv(upper); ok {
				config.NativeSystemConfig[key] = v
			} else if envOverride.Required {
				_, confFound := config.NativeSystemConfig[key]
				if !confFound {
					return fmt.Errorf("required system_config variable %s not found in config nor LookupEnv(%s)", key, upper)
				}
			}

		}
		return nil
	}
}

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

func loadConfig(configPath string, logger Logger) (*Config, error) {
	c := newConfig()
	c.ConfigPath = configPath

	logger.Info("Loading configuration", "path", configPath)

	// configPath must refer to a folder
	// iterate all files in the folder that ends with .json
	// load each file and merge into the config
	files, err := os.ReadDir(configPath)
	if err != nil {
		logger.Error("Failed to read config directory", "error", err.Error())
		return nil, err
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".json") {
			logger.Debug("Reading config file", "file", file.Name())
			var reader io.Reader
			// load from file
			var err error
			reader, err = os.Open(filepath.Join(configPath, file.Name()))
			if err != nil {
				logger.Error("Failed to open config file", "file", file.Name(), "error", err.Error())
				return nil, err
			}
			config, err := readConfig(reader)
			if err != nil {
				logger.Error("Failed to read config file", "file", file.Name(), "error", err.Error())
				return nil, err
			}
			addConfig(c, config, logger)
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

	addEnvOverrides(c, logger)
	logger.Info("Configuration loaded", "datasets", len(c.DatasetDefinitions))
	return c, nil
}

func addEnvOverrides(c *Config, logger Logger) {
	val, found := os.LookupEnv("PORT")
	if found {
		logger.Debug("Env override applied", "key", "PORT", "value", val)
		c.LayerServiceConfig.Port = json.Number(val)
	}

	val, found = os.LookupEnv("CONFIG_REFRESH_INTERVAL")
	if found {
		logger.Debug("Env override applied", "key", "CONFIG_REFRESH_INTERVAL", "value", val)
		c.LayerServiceConfig.ConfigRefreshInterval = val
	}

	val, found = os.LookupEnv("SERVICE_NAME")
	if found {
		logger.Debug("Env override applied", "key", "SERVICE_NAME", "value", val)
		c.LayerServiceConfig.ServiceName = val
	}

	val, found = os.LookupEnv("STATSD_ENABLED")
	if found {
		logger.Debug("Env override applied", "key", "STATSD_ENABLED", "value", val)
		c.LayerServiceConfig.StatsdEnabled = val == "true"
	}

	val, found = os.LookupEnv("STATSD_AGENT_ADDRESS")
	if found {
		logger.Debug("Env override applied", "key", "STATSD_AGENT_ADDRESS", "value", val)
		c.LayerServiceConfig.StatsdAgentAddress = val
	}

	val, found = os.LookupEnv("LOG_LEVEL")
	if found {
		logger.Debug("Env override applied", "key", "LOG_LEVEL", "value", val)
		c.LayerServiceConfig.LogLevel = val
	}

	val, found = os.LookupEnv("LOG_FORMAT")
	if found {
		logger.Debug("Env override applied", "key", "LOG_FORMAT", "value", val)
		c.LayerServiceConfig.LogFormat = val
	}
}

func addConfig(mainConfig *Config, partialConfig *Config, logger Logger) {
	// system config can only be defined once any repeats replace it
	if partialConfig.NativeSystemConfig != nil {
		logger.Debug("Merging native system config")
		mainConfig.NativeSystemConfig = partialConfig.NativeSystemConfig
	}

	// layer service config can only be defined once any repeats replace it
	if partialConfig.LayerServiceConfig != nil {
		logger.Debug("Merging layer service config")
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
					logger.Info("Updating dataset definition", "dataset", def.DatasetName)
					existingDef.SourceConfig = def.SourceConfig
					existingDef.IncomingMappingConfig = def.IncomingMappingConfig
					existingDef.OutgoingMappingConfig = def.OutgoingMappingConfig
				}
				break
			}
			if !exists {
				logger.Info("Adding dataset definition", "dataset", def.DatasetName)
				mainConfig.DatasetDefinitions = append(mainConfig.DatasetDefinitions, def)
			}
		}
	}
}
