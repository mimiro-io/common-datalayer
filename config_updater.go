package common_datalayer

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type configUpdater struct {
	ticker *time.Ticker
	logger Logger
}

func (u *configUpdater) Stop(ctx context.Context) error {
	u.logger.Info("Stopping config updater")
	u.ticker.Stop()
	return nil
}

func newConfigUpdater(
	config *Config,
	args []string,
	enrichConfig func(config *Config) error,
	l Logger,
	listeners ...DataLayerService) (*configUpdater, error) {
	u := &configUpdater{logger: l}
	u.ticker = time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-u.ticker.C:
				checkForUpdates(config, args, enrichConfig, l, listeners...)
			}
		}
	}()
	return u, nil
}

func checkForUpdates(config *Config, args []string, enrichConfig func(config *Config) error, logger Logger, listeners ...DataLayerService) {
	logger.Debug("checking config for updates in " + strings.Join(args, ", ") + ".")
	loadedConf, err := loadConfig(args)
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
	if !config.equals(loadedConf) {
		logger.Info("Config changed, updating...")
		for _, listener := range listeners {
			err = listener.UpdateConfiguration(loadedConf)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to update config: %v", err.Error()))
				return
			}
		}
	}
}