package common_datalayer

type DataLayerService interface {
	Stoppable
	ConfigUpdateListener
	GetDataset(dataset string) Dataset
	ListDatasetNames() []string
}

type Dataset interface {
	Description() map[string]interface{}
	GetName() string
	WriteItem(item Item) error
	BeginFullSync() error
	CompleteFullSync() error
	CancelFullSync() error
	GetChanges(since string, take int, latestOnly bool) (EntityIterator, error)
	GetEntities(since string, take int) (EntityIterator, error)
}