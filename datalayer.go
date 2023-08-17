package common_datalayer

import "context"

type Stoppable interface {
	Stop(ctx context.Context) error
}
type DataLayerService interface {
	Stoppable
	UpdateConfiguration(config *Config) LayerError
	Dataset(dataset string) (Dataset, LayerError)
	DatasetNames() []string
}

type Dataset interface {
	MetaData() map[string]any
	Name() string
	Write(item Item) LayerError
	BeginFullSync() LayerError
	CompleteFullSync() LayerError
	CancelFullSync() LayerError
	Changes(since string, take int, latestOnly bool) (EntityIterator, LayerError)
	Entities(since string, take int) (EntityIterator, LayerError)
}