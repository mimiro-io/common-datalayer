package common_datalayer

import (
	"fmt"
	"io"
	"strings"
)

var requiredProperties = []string{"PORT", "SERVICE_NAME"}

// server port etc
type ApplicationConfig interface {
}

// the system we are a layer for
type SystemConfig struct {
	Properties map[string]any
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
	SystemConfig       *SystemConfig
	DatasetDefinitions []*DatasetDefinition
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
	Initialize(config *Config) error
}

func ReadConfig(data io.Reader) *Config {
	return nil
}

/******************************************************************************/
// helpers

func (d DatasetDefinition) StripProps() bool {
	return boolOr(d.SourceConfig, "stripProps", false)
}

func (c SystemConfig) HttpPort() string {
	return stringOr(c.Properties, "PORT", "8080")
}

func (c SystemConfig) ServiceName() string {
	return stringOr(c.Properties, "SERVICE_NAME", "UNKNOWN_SERVICE")
}
func (c SystemConfig) StatsdEnabled() bool {
	return boolOr(c.Properties, "STATSD_ENABLED", false)
}

func (c SystemConfig) StatsdAgentAddress() string {
	return stringOr(c.Properties, "STATSD_AGENT_ADDRESS", "localhost:8125")
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