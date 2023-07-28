package common_datalayer

type DataLayerService interface {
	Initialize(config Config, logger Logger) error
	GetDatasetManager() DatasetManager
}

type DatasetManager interface {
	GetDataset(dataset string) Dataset
	ListDatasets() []string
}

type Dataset interface {
	GetChanges(since string, take int, latestOnly bool) (*EntityIterator, error)
	GetEntities(since string, take int) (*EntityIterator, error)
	WriteEntities(entities *EntityIterator) error
	BeginFullSync() error
	CompleteFullSync() error
	CancelFullSync() error
	Description() map[string]interface{}
}
