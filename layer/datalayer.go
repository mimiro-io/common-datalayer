package layer

import (
	"mimiro.io/common-datalayer/core"
)

type DataLayerService interface {
	Stoppable
	DatasetManager
	core.ReInitializable
}

type DatasetManager interface {
	GetDataset(dataset string) Dataset
	ListDatasetNames() []string
}

type EntityReader interface {
	GetChanges(since string, take int, latestOnly bool) (EntityIterator, error)
	GetEntities(since string, take int) (EntityIterator, error)
}

type RowWriter interface {
	WriteRows(items ItemIterator) error
	WriteItem(item Item) error
}

type FullSyncRowWriter interface {
	RowWriter
	BeginFullSync() error
	CompleteFullSync() error
	CancelFullSync() error
}

type Dataset interface {
	EntityReader
	FullSyncRowWriter
	Description() map[string]interface{}
	GetName() string
}