package common_datalayer

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
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
	runner.createService = newLayerService
	return runner
}

func (serviceRunner *ServiceRunner) configure() {
	if serviceRunner.logger == nil {
		// bootstrap logger before config is available
		serviceRunner.logger = NewLogger("bootstrap", "text", "info")
	}

	if serviceRunner.configLocation == "" {
		configPath, found := os.LookupEnv("DATALAYER_CONFIG_PATH")
		if found {
			serviceRunner.configLocation = configPath
		} else {
			serviceRunner.configLocation = "./config"
		}
	}

	serviceRunner.logger.Debug("Loading configuration", "path", serviceRunner.configLocation)
	config, err := loadConfig(serviceRunner.configLocation, serviceRunner.logger)
	if err != nil {
		serviceRunner.logger.Error("Failed to load configuration", "error", err.Error())
		panic(err)
	}
	serviceRunner.logger.Info("Configuration loaded")

	// enrich config specific for layer
	if serviceRunner.enrichConfig != nil {
		serviceRunner.logger.Debug("Enriching configuration")
		err = serviceRunner.enrichConfig(config)
		if err != nil {
			serviceRunner.logger.Error("Failed to enrich configuration", "error", err.Error())
			panic(err)
		}
	}

	// initialise logger
	logger := NewLogger(
		config.LayerServiceConfig.ServiceName,
		config.LayerServiceConfig.LogFormat,
		config.LayerServiceConfig.LogLevel,
	)
	serviceRunner.logger = logger
	serviceRunner.logger.Info("Logger initialised", "level", config.LayerServiceConfig.LogLevel, "format", config.LayerServiceConfig.LogFormat)

	metrics, err := newMetrics(config)
	if err != nil {
		serviceRunner.logger.Error("Failed to initialise metrics", "error", err.Error())
		panic(err)
	}
	serviceRunner.logger.Info("Metrics initialised")

	serviceRunner.layerService, err = serviceRunner.createService(config, logger, metrics)
	if err != nil {
		serviceRunner.logger.Error("Failed to create data layer service", "error", err.Error())
		panic(err)
	}
	serviceRunner.logger.Info("Data layer service created")

	// create and start config updater
	serviceRunner.configUpdater, err = newConfigUpdater(config, serviceRunner.enrichConfig, logger, serviceRunner.layerService)
	if err != nil {
		serviceRunner.logger.Error("Failed to start config updater", "error", err.Error())
		panic(err)
	}
	serviceRunner.logger.Info("Config updater started")

	// create web service hook up with the service core
	serviceRunner.webService, err = newDataLayerWebService(config, logger, metrics, serviceRunner.layerService)
	if err != nil {
		serviceRunner.logger.Error("Failed to create web service", "error", err.Error())
		panic(err)
	}
	serviceRunner.logger.Info("Web service created")

	serviceRunner.stoppable = append(
		serviceRunner.stoppable,
		serviceRunner.layerService,
		serviceRunner.configUpdater,
		serviceRunner.webService)
	serviceRunner.logger.Debug("Service configuration complete")
}

type ServiceRunner struct {
	logger         Logger
	enrichConfig   func(config *Config) error
	webService     *dataLayerWebService
	configUpdater  *configUpdater
	createService  func(config *Config, logger Logger, metrics Metrics) (DataLayerService, error)
	configLocation string
	layerService   DataLayerService
	stoppable      []Stoppable
}

func (serviceRunner *ServiceRunner) LayerService() DataLayerService {
	return serviceRunner.layerService
}

func (serviceRunner *ServiceRunner) Start() error {
	if serviceRunner.logger == nil {
		serviceRunner.logger = NewLogger("bootstrap", "text", "info")
	}
	serviceRunner.logger.Info("Starting service")
	// configure the service
	serviceRunner.configure()

	// start the service
	err := serviceRunner.webService.Start()
	if err != nil {
		serviceRunner.logger.Error("Failed to start web service", "error", err.Error())
		return err
	}
	serviceRunner.logger.Info("Service started")

	return nil
}

func (serviceRunner *ServiceRunner) StartAndWait() {
	if serviceRunner.logger == nil {
		serviceRunner.logger = NewLogger("bootstrap", "text", "info")
	}
	serviceRunner.logger.Info("Starting service and waiting for shutdown")
	// configure the service
	serviceRunner.configure()

	// start the service
	err := serviceRunner.webService.Start()
	if err != nil {
		serviceRunner.logger.Error("Failed to start web service", "error", err.Error())
		panic(err)
	}
	serviceRunner.logger.Info("Service started, entering wait state")

	// and wait for ctrl-c
	serviceRunner.andWait()
}

func (serviceRunner *ServiceRunner) Stop() error {
	if serviceRunner.logger == nil {
		serviceRunner.logger = NewLogger("bootstrap", "text", "info")
	}
	serviceRunner.logger.Info("Stopping service")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, stoppable := range serviceRunner.stoppable {
		err := stoppable.Stop(ctx)
		if err != nil {
			serviceRunner.logger.Error("Failed to stop component", "error", err.Error())
			return err
		}
	}

	serviceRunner.logger.Info("Service stopped")
	return nil
}

func (serviceRunner *ServiceRunner) andWait() {
	// handle shutdown, this call blocks and keeps the application running
	waitForStop(serviceRunner.logger, serviceRunner.stoppable...)
}

//	 waitForStop listens for SIGINT (Ctrl+C) and SIGTERM (graceful docker stop).
//		It accepts a list of stoppables that will be stopped when a signal is received.
func waitForStop(logger Logger, stoppable ...Stoppable) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	logger.Info("Data Layer stopping")

	shutdownCtx := context.Background()
	wg := sync.WaitGroup{}
	for _, s := range stoppable {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Stop(shutdownCtx)
			if err != nil {
				logger.Error("Stopping Data Layer failed: %+v", err)
				os.Exit(2)
			}
		}()
	}
	wg.Wait()
	logger.Info("Data Layer stopped")
	os.Exit(0)
}
