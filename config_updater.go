package common_datalayer

import (
	"context"
	"time"
)

type configUpdater struct {
	timer *time.Timer
}

func (u *configUpdater) Stop(ctx context.Context) error {
	u.timer.Stop()
	return nil
}

func newConfigUpdater(
	config *Config,
	args []string,
	enrichConfig func(config *Config) error,
	l Logger,
	listeners ...ConfigUpdateListener) (*configUpdater, error) {
	u := &configUpdater{}
	u.timer = time.NewTimer(5 * time.Second)
	go func() {

		for {
			select {
			case <-u.timer.C:
				checkForUpdates(config, args, enrichConfig, l, listeners...)
			}
		}
	}()
	return u, nil
}

func checkForUpdates(config *Config, args []string, enrichConfig func(config *Config) error, l Logger, listeners ...ConfigUpdateListener) {
	loadedConf, _ := loadConfig(args)
	enrichConfig(loadedConf)
	if !config.equals(loadedConf) {
		l.Info("Config changed, reloading")
		for _, listener := range listeners {
			listener.UpdateConfiguration(loadedConf)
		}
	}
}