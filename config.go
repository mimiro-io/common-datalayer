package common_datalayer

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
)

type SystemConfig map[string]any
type ApplicationConfig map[string]any
type DatasetDefinition struct {
	DatasetName   string                   `json:"dataset_name"`
	SourceConfig  map[string]any           `json:"source_configuration"`
	Constructions []*PropertyConstructor   `json:"constructions"`
	Mappings      []*EntityPropertyMapping `json:"mappings"`
}

// the operations can be one of the following: concat, split, replace, trim, tolower, toupper, regex, slice
type PropertyConstructor struct {
	PropertyName string   `json:"property"`
	Operation    string   `json:"operation"`
	Arguments    []string `json:"args"`
}

type EntityPropertyMapping struct {
	// hang on to the raw config as it allows for local extensions
	Raw             map[string]any
	EntityProperty  string `json:"entity_property"`
	Property        string `json:"property"`
	Datatype        string `json:"datatype"`
	IsReference     bool   `json:"is_reference"`
	UrlValuePattern string `json:"url_value_pattern"`
	IsIdentity      bool   `json:"is_identity"`
}

type Config struct {
	ApplicationConfig  ApplicationConfig    `json:"application_configuration"`
	SystemConfig       SystemConfig         `json:"system_configuration"`
	DatasetDefinitions []*DatasetDefinition `json:"dataset_definitions"`
}

func (epm *EntityPropertyMapping) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}

	epm.Raw = raw
	epm.EntityProperty = raw["entity_property"].(string)
	epm.Property = raw["property"].(string)
	if raw["datatype"] != nil {
		epm.Datatype = raw["datatype"].(string)
	}
	epm.IsReference = raw["is_reference"] == true
	if raw["url_value_pattern"] != nil {
		epm.UrlValuePattern = raw["url_value_pattern"].(string)
	}
	epm.IsIdentity = raw["is_identity"] == true
	return nil
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
	res := &Config{}
	res.SystemConfig = make(SystemConfig)
	res.ApplicationConfig = make(ApplicationConfig)
	res.DatasetDefinitions = make([]*DatasetDefinition, 0)
	return res
}

func readConfig(data io.Reader) (*Config, error) {
	config := newConfig()
	s, err := io.ReadAll(data)
	//err := json.NewDecoder(data).Decode(config)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(s, config)
	if err != nil {
		return nil, err
	}
	lowerKeys(config.SystemConfig)
	for _, def := range config.DatasetDefinitions {
		lowerKeys(def.SourceConfig)
		for _, mapping := range def.Mappings {
			lowerKeys(mapping.Raw)
		}
	}
	return config, nil
}

func lowerKeys(properties map[string]any) {
	for key, value := range properties {
		delete(properties, key)
		properties[strings.ToLower(key)] = value
	}
}

func loadConfig(args []string) (*Config, error) {
	c := newConfig()

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
			c.ApplicationConfig[strings.ToLower(name)] = val
		}
	}
	val("PORT")
	val("ENV")
	val("CONFIG_REFRESH_INTERVAL")
	val("SERVICE_NAME")
	val("STATSD_ENABLED")
	val("STATSD_AGENT_ADDRESS")
	val("LOG_LEVEL")
}

func addConfig(c *Config, config *Config) {
	for key, value := range config.SystemConfig {
		c.SystemConfig[key] = value
	}
	for key, value := range config.ApplicationConfig {
		c.ApplicationConfig[key] = value
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
	return boolOr(d.SourceConfig, "strip_props", false)
}

func (c ApplicationConfig) HttpPort() string {
	return stringOr(c, "port", "8080")
}

func (c ApplicationConfig) ServiceName() string {
	return stringOr(c, "service_name", "UNKNOWN_SERVICE")
}
func (c ApplicationConfig) StatsdEnabled() bool {
	return boolOr(c, "statsd_enabled", false)
}

func (c ApplicationConfig) StatsdAgentAddress() string {
	return stringOr(c, "statsd_agent_address", "localhost:8125")
}

func (c ApplicationConfig) Environment() string {
	return stringOr(c, "env", "dev")
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
		boolVal, ok := val.(bool)
		if ok {
			return boolVal
		}
		return strings.ToLower(val.(string)) == "true"
	}
	// default
	return defaultValue
}
