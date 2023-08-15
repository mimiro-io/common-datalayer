package common_datalayer

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func loadConfig(args []string) (*Config, error) {
	config := NewConfig()
	err := config.Load(args)
	if err != nil {
		return nil, err
	}
	return config, nil
}

type Stoppable interface {
	Stop(ctx context.Context) error
}
type Service struct {
	stoppables []Stoppable
	logger     Logger
}

func (s *Service) Stop() {
	ctx := context.Background()
	for _, stoppable := range s.stoppables {
		stoppable.Stop(ctx)
	}
}

// StartService call this from main to get things started
func StartService(
	args []string,
	newLayerService func(core *CoreService) (DataLayerService, error),
	enrichConfig func(args []string, config *Config) error,
) {
	s := CreateService(args, newLayerService, enrichConfig)
	// handle shutdown, this call blocks and keeps the application running
	waitForStop(s.logger, s.stoppables...)
}

func CreateService(
	args []string,
	newLayerService func(core *CoreService) (DataLayerService, error),
	enrichConfig func(args []string, config *Config) error,
) Service {
	// create core layer service
	// read config
	config, err := loadConfig(args)
	if err != nil {
		panic(err)
	}
	err = config.SystemConfig.Verify()
	if err != nil {
		panic(err)
	}

	// enrich config specific for layer
	err = enrichConfig(args, config)

	if err != nil {
		panic(err)
	}
	// initialise logger
	logger := newLogger()

	metrics, err := newMetrics(config)
	if err != nil {
		panic(err)
	}

	cs := &CoreService{
		Config:  config,
		Logger:  logger,
		Metrics: metrics,
	}

	layerService, err := newLayerService(cs)
	if err != nil {
		panic(err)
	}

	err = layerService.Initialize(config, logger)
	if err != nil {
		panic(err)
	}
	// TODO: hook up config updater which calls layerService.Initialize on change

	// create web service hook up with the service core
	webService, err := NewDataLayerWebService(cs, layerService)
	if err != nil {
		panic(err)
	}

	// start the service
	err = webService.Start()
	if err != nil {
		panic(err)
	}
	return Service{stoppables: []Stoppable{layerService, webService}, logger: logger}
}

// waitForStop listens for SIGINT (Ctrl+C) and SIGTERM (graceful docker stop).
//
//	It accepts a list of stoppables that will be stopped when a signal is received.
func waitForStop(logger Logger, stoppables ...Stoppable) {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	logger.Info("Application stopping!")

	shutdownCtx := context.Background()
	wg := sync.WaitGroup{}
	for _, s := range stoppables {
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