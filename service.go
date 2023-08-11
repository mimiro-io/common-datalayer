package common_datalayer

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"mimiro.io/common-datalayer/core"
	"mimiro.io/common-datalayer/layer"
)

func loadConfig(args []string) (core.Config, error) {
	config := core.NewConfig()
	err := config.Load(args)
	if err != nil {
		return nil, err
	}
	return config, nil
}

type Service struct {
	stoppables []layer.Stoppable
	logger     core.Logger
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
	newLayerService func(core *core.Service) (layer.DataLayerService, error),
	enrichConfig func(args []string, config core.Config) error,
) {
	s := CreateService(args, newLayerService, enrichConfig)
	// handle shutdown, this call blocks and keeps the application running
	waitForStop(s.logger, s.stoppables...)
}

func CreateService(
	args []string,
	newLayerService func(core *core.Service) (layer.DataLayerService, error),
	enrichConfig func(args []string, config core.Config) error,
) Service {
	// create core layer service
	// read config
	config, err := loadConfig(args)
	if err != nil {
		panic(err)
	}
	err = config.SystemConfig().Verify()
	if err != nil {
		panic(err)
	}

	// enrich config specific for layer
	err = enrichConfig(args, config)

	if err != nil {
		panic(err)
	}
	// initialise logger
	logger := core.NewLogger()

	metrics := core.NewMetrics(config)

	cs := &core.Service{
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
	return Service{stoppables: []layer.Stoppable{layerService, webService}, logger: logger}
}

// waitForStop listens for SIGINT (Ctrl+C) and SIGTERM (graceful docker stop).
//
//	It accepts a list of stoppables that will be stopped when a signal is received.
func waitForStop(logger core.Logger, stoppables ...layer.Stoppable) {
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