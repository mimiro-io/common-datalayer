package common_datalayer

type DataLayerWebService struct {
	// echo web handler

	// service specific service core
	datalayerService DataLayerService
}

func (ws *DataLayerWebService) Start() {

}

func (ws *DataLayerWebService) Stop() {

}

func (ws *DataLayerWebService) Restart() {

}

func NewDataLayerWebService(dataLayerService DataLayerService) *DataLayerWebService {
	dataLayerWebService := &DataLayerWebService{}
	dataLayerWebService.datalayerService = dataLayerService
	return dataLayerWebService
}

func loadConfig(args []string) Config {
	return nil
}

// StartService call this from main to get things started
func StartService(args []string, newDataLayerService func() DataLayerService, enrichConfig func(config Config)) {
	serviceCore := newDataLayerService()

	// read config
	config := loadConfig(args)

	// enrich config specific for layer
	enrichConfig(config)

	// initialise logger
	logger := NewLogger()

	// initialise service core
	serviceCore.Initialize(config, logger)

	// create web service hook up with the service core
	webService := NewDataLayerWebService(serviceCore)

	// start the service
	webService.Start()
}
