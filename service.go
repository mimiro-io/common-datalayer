package common_datalayer

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func (serviceRunner *ServiceRunner) WithEnrichConfig(enrichConfig func(config *Config) error) *ServiceRunner {
	serviceRunner.enrichConfig = enrichConfig
	return serviceRunner
}

func (serviceRunner *ServiceRunner) WithConfigLocation(configLocation string) *ServiceRunner {
	serviceRunner.configLocation = configLocation
	return serviceRunner
}

func NewServiceRunner(newLayerService func(config *Config, logger Logger, metrics Metrics) (DataLayerService, error)) *ServiceRunner {
	runner := &ServiceRunner{}

	configPath, found := os.LookupEnv("DATALAYER_CONFIG_PATH")
	if found {
		runner.configLocation = configPath
	} else {
		runner.configLocation = "./config"
	}

	config, err := loadConfig(configPath)
	if err != nil {
		panic(err)
	}

	// enrich config specific for layer
	if runner.enrichConfig != nil {
		err = runner.enrichConfig(config)
		if err != nil {
			panic(err)
		}
	}

	// initialise l
	l := newLogger(config)

	metrics, err := newMetrics(config)
	if err != nil {
		panic(err)
	}

	layerService, err := newLayerService(config, l, metrics)
	if err != nil {
		panic(err)
	}

	// create and start config updater
	runner.configUpdater, err = newConfigUpdater(config, runner.enrichConfig, l, layerService)
	if err != nil {
		panic(err)
	}

	// create web service hook up with the service core
	runner.webService, err = newDataLayerWebService(config, l, metrics, layerService)
	if err != nil {
		panic(err)
	}

	return runner
}

type ServiceRunner struct {
	stoppable      []Stoppable
	logger         Logger
	configLocation string
	enrichConfig   func(config *Config) error
	webService     *dataLayerWebService
	configUpdater  *configUpdater
}

func (serviceRunner *ServiceRunner) Start() error {
	// start the service
	err := serviceRunner.webService.Start()
	if err != nil {
		return err
	}
	err = serviceRunner.andWait()
	if err != nil {
		return err
	}
	return nil
}

func (serviceRunner *ServiceRunner) Stop() error {
	ctx := context.Background()
	for _, stoppable := range serviceRunner.stoppable {
		err := stoppable.Stop(ctx)
		if err != nil {
			return err
		}
	}

	// also stop config updater
	err := serviceRunner.configUpdater.Stop(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (serviceRunner *ServiceRunner) andWait() error {
	// handle shutdown, this call blocks and keeps the application running
	waitForStop(serviceRunner.logger, serviceRunner.stoppable...)
	return nil
}

//	 waitForStop listens for SIGINT (Ctrl+C) and SIGTERM (graceful docker stop).
//		It accepts a list of stoppables that will be stopped when a signal is received.
func waitForStop(logger Logger, stoppable ...Stoppable) {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	logger.Info("Application stopping!")

	shutdownCtx := context.Background()
	wg := sync.WaitGroup{}
	for _, s := range stoppable {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Stop(shutdownCtx)
			if err != nil {
				logger.Error("Stopping Application failed: %+v", err)
				os.Exit(2)
			}
		}()
	}
	wg.Wait()
	logger.Info("Application stopped!")
	os.Exit(0)
}
