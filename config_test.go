package common_datalayer

import "testing"

func TestConfig(t *testing.T) {
	config, err := loadConfig("./testdata")
	if err != nil {
		t.Error(err)
	}
	if config.LayerServiceConfig.ServiceName != "sample" {
		t.Error("ServiceName should be sample")
	}
}

func TestConfig_Merge(t *testing.T) {

}

func TestConfig_AddEnvOverrides(t *testing.T) {

}
