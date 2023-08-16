package common_datalayer

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

var requiredProperties = []string{"port", "service_name"}

// the system we are a layer for
type SystemConfig struct {
	Properties map[string]any
}

func (c *SystemConfig) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}
	c.Properties = raw
	return nil
}

type AuthConfig struct {
	ClientId     string
	ClientSecret string
	Endpoint     string
	Audience     string
	GrantType    string
	Wellknown    string
}

type DatasetDefinition struct {
	DatasetName  string
	SourceConfig map[string]any
	Mappings     []*EntityPropertyMapping
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
	SystemConfig       *SystemConfig        `json:"system_configuration"`
	DatasetDefinitions []*DatasetDefinition `json:"dataset_definitions"`
}

/******************************************************************************/

func (c SystemConfig) Verify() error {
	for _, key := range requiredProperties {
		if _, exists := c.Properties[key]; !exists {
			return fmt.Errorf("required property %s is missing", key)
		}
	}
	return nil
}

func (c SystemConfig) AuthConfig() AuthConfig {
	return AuthConfig{
		ClientId:     stringOr(c.Properties, "AUTH0_CLIENT_ID", ""),
		ClientSecret: stringOr(c.Properties, "AUTH0_CLIENT_SECRET", ""),
		Endpoint:     stringOr(c.Properties, "AUTH0_ENDPOINT", ""),
		Audience:     stringOr(c.Properties, "AUTH0_AUDIENCE", ""),
		GrantType:    stringOr(c.Properties, "AUTH0_GRANT_TYPE", "client_credentials"),
		Wellknown:    stringOr(c.Properties, "TOKEN_WELL_KNOWN", ""),
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

func newConfig() *Config {
	res := &Config{}
	res.SystemConfig = &SystemConfig{}
	res.SystemConfig.Properties = make(map[string]any)
	res.DatasetDefinitions = make([]*DatasetDefinition, 0)
	return res
}

type Initialization interface {
	Initialize(datasetDefinitions []*DatasetDefinition) error
}

func readConfig(data io.Reader) (*Config, error) {
	config := newConfig()
	s, err := io.ReadAll(data)
	//err := json.NewDecoder(data).Decode(config)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(s, config)
	return config, nil
}

func loadConfig(args []string) (*Config, error) {
	c := newConfig()
	// set some defaults
	c.SystemConfig.Properties["port"] = "8080"
	c.SystemConfig.Properties["service_name"] = "UNKNOWN_SERVICE"

	for _, arg := range args {
		var reader io.Reader
		if strings.HasPrefix(arg, "http") {
			// load from url
			response, err := http.Get(arg)
			if err != nil {
				return nil, err
			}
			reader = response.Body
		} else {
			// load from file
			var err error
			reader, err = os.Open(arg)
			if err != nil {
				return nil, err
			}
		}
		config, err := readConfig(reader)
		if err != nil {
			return nil, err
		}
		addConfig(c, config)
	}

	addEnvOverrides(c)

	return c, nil
}

func addEnvOverrides(c *Config) {
	val := func(name string) {
		val, found := os.LookupEnv(name)
		if found {
			c.SystemConfig.Properties[strings.ToLower(name)] = val
		}
	}
	val("PORT")
	val("ENVIRONMENT")
	val("STATSD_ENABLED")
	val("STATSD_AGENT_ADDRESS")
}

func addConfig(c *Config, config *Config) {
	for key, value := range config.SystemConfig.Properties {
		c.SystemConfig.Properties[key] = value
	}
	for _, def := range config.DatasetDefinitions {
		var exists bool
		for _, existingDef := range c.DatasetDefinitions {
			if existingDef.DatasetName == def.DatasetName {
				exists = true
				for _, mapping := range def.Mappings {
					var mappingExists bool
					for _, existingMapping := range existingDef.Mappings {
						if existingMapping.EntityProperty == mapping.EntityProperty {
							mappingExists = true
							break
						}
					}
					if !mappingExists {
						existingDef.Mappings = append(existingDef.Mappings, mapping)
					}
				}
			}
			break
		}
		if !exists {
			c.DatasetDefinitions = append(c.DatasetDefinitions, def)
		}

	}
}

/******************************************************************************/
// helpers

func (d DatasetDefinition) StripProps() bool {
	return boolOr(d.SourceConfig, "stripProps", false)
}

func (c SystemConfig) HttpPort() string {
	return stringOr(c.Properties, "port", "8080")
}

func (c SystemConfig) ServiceName() string {
	return stringOr(c.Properties, "service_name", "UNKNOWN_SERVICE")
}
func (c SystemConfig) StatsdEnabled() bool {
	return boolOr(c.Properties, "statsd_enabled", false)
}

func (c SystemConfig) StatsdAgentAddress() string {
	return stringOr(c.Properties, "statsd_agent_address", "localhost:8125")
}

func stringOr(configMap map[string]any, key, defaultValue string) string {
	val, exists := configMap[key]
	if exists {
		return val.(string)
	}
	// default
	return defaultValue
}

func boolOr(configMap map[string]any, key string, defaultValue bool) bool {
	val, exists := configMap[key]
	if exists {
		return strings.ToLower(val.(string)) == "true"
	}
	// default
	return defaultValue
}