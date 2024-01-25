package common_datalayer

import (
	"context"
	"fmt"
	"time"
)

type configUpdater struct {
	ticker *time.Ticker
	logger Logger
	config *Config
}

func (u *configUpdater) Stop(ctx context.Context) error {
	u.logger.Info("Stopping config updater")
	u.ticker.Stop()
	return nil
}

func newConfigUpdater(
	config *Config,
	enrichConfig func(config *Config) error,
	l Logger,
	listeners ...DataLayerService,
) (*configUpdater, error) {
	u := &configUpdater{logger: l}
	u.ticker = time.NewTicker(5 * time.Second)
	u.config = config

	go func() {
		for range u.ticker.C {
			u.checkForUpdates(enrichConfig, l, listeners...)
		}
	}()
	return u, nil
}

func (u *configUpdater) checkForUpdates(enrichConfig func(config *Config) error, logger Logger, listeners ...DataLayerService) {
	logger.Debug("checking config for updates in " + u.config.ConfigPath + ".")
	loadedConf, err := loadConfig(u.config.ConfigPath)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to load config: %v", err.Error()))
		return
	}
	if enrichConfig != nil {
		err = enrichConfig(loadedConf)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to enrich config: %v", err.Error()))
			return
		}
	}
	if !u.config.equals(loadedConf) {
		logger.Info("Config changed, updating...")
		for _, listener := range listeners {
			err = listener.UpdateConfiguration(loadedConf)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to update config: %v", err.Error()))
				return
			}
		}
		// set config to the new loaded config
		u.config = loadedConf
	}
}
