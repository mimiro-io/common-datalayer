package core

import "fmt"

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

func (c SystemConfig) Verify() error {
	for _, key := range requiredProperties {
		if _, exists := c.Properties[key]; !exists {
			return fmt.Errorf("required property %s is missing", key)
		}
	}
	return nil
}

func (c SystemConfig) HttpPort() string {
	return stringOr(c.Properties, "PORT", "8080")
}

func (c SystemConfig) ServiceName() string {
	return stringOr(c.Properties, "SERVICE_NAME", "UNKNOWN_SERVICE")
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

// TODO: do we need CORS in a layer?
func (c SystemConfig) CorsOrigins() []string {
	return []string{"https://api.mimiro.io", "https://platform.mimiro.io"}
}