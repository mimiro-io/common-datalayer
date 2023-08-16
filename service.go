package common_datalayer

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Stoppable interface {
	Stop(ctx context.Context) error
}
type Service struct {
	stoppables []Stoppable
	logger     Logger
}

func (s *Service) Stop() error {
	ctx := context.Background()
	for _, stoppable := range s.stoppables {
		err := stoppable.Stop(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) AndWait() error {
	// handle shutdown, this call blocks and keeps the application running
	waitForStop(s.logger, s.stoppables...)
	return nil
}

type StartOptions struct {
	enrichConfig func(config *Config) error
	configFiles  []string
}
type Option func(*StartOptions)

func EnrichConfigOption(enrichConfig func(config *Config) error) Option {
	return func(o *StartOptions) {
		o.enrichConfig = enrichConfig
	}
}
func ConfigFileOption(configFile string) Option {
	return func(o *StartOptions) {
		o.configFiles = append(o.configFiles, configFile)
	}
}
func Start(
	newLayerService func(core *CoreService) (DataLayerService, error),
	options ...Option,
) *Service {
	// create core layer service
	// read config
	so := &StartOptions{}
	for _, option := range options {
		option(so)
	}
	args := []string{os.Getenv("DATALAYER_CONFIG_PATH")}
	args = append(args, so.configFiles...)
	config, err := loadConfig(args)
	if err != nil {
		panic(err)
	}
	err = config.SystemConfig.Verify()
	if err != nil {
		panic(err)
	}

	// enrich config specific for layer
	if so.enrichConfig != nil {
		err = so.enrichConfig(config)
		if err != nil {
			panic(err)
		}
	}

	// initialise l
	l := newLogger()

	metrics, err := newMetrics(config)
	if err != nil {
		panic(err)
	}

	cs := &CoreService{
		config:  config,
		Logger:  l,
		Metrics: metrics,
	}

	layerService, err := newLayerService(cs)
	if err != nil {
		panic(err)
	}

	err = layerService.Initialize(config.DatasetDefinitions)
	if err != nil {
		panic(err)
	}
	// TODO: hook up config updater which calls layerService.Initialize on change

	// create web service hook up with the service core
	webService, err := newDataLayerWebService(cs, layerService)
	if err != nil {
		panic(err)
	}

	// start the service
	err = webService.Start()
	if err != nil {
		panic(err)
	}
	return &Service{
		stoppables: []Stoppable{layerService, webService},
		logger:     l}
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