package common_datalayer

import (
	"context"

	egdm "github.com/mimiro-io/entity-graph-data-model"
)

type Stoppable interface {
	Stop(ctx context.Context) error
}

type DataLayerServiceFactory interface {
	Build(config *Config, logger Logger, metrics Metrics) (DataLayerService, error)
}

type DataLayerService interface {
	Stoppable
	UpdateConfiguration(config *Config) LayerError
	Dataset(dataset string) (Dataset, LayerError)
	DatasetDescriptions() []*DatasetDescription
}

type BatchInfo struct {
	SyncId       string
	IsLastBatch  bool
	IsStartBatch bool
}

type EntityIterator interface {
	// Context returns the context for the current iteration. This context
	// contains namespace mappings for the whole dataset as per UDA spec
	Context() *egdm.Context
	// Next moves the iterator to the next entity. returns nil when exhausted
	Next() (*egdm.Entity, LayerError)
	// Token returns the continuation token for the next iteration, if applicable
	Token() (*egdm.Continuation, LayerError)
	// Close releases underlying data source objects
	Close() LayerError
}

type Dataset interface {
	MetaData() map[string]any
	Name() string

	// FullSync produces a DatasetWriter, which depending on fullsync state in batchInfo
	// starts, continues or ends a fullsync operation spanning over multiple requests.
	// Layers should also remove stale entities after a fullsync finishes.
	FullSync(ctx context.Context, batchInfo BatchInfo) (DatasetWriter, LayerError)
	// Incremental produces a DatasetWriter, which appends changes to the dataset when
	// written to.
	Incremental(ctx context.Context) (DatasetWriter, LayerError)

	// Changes retrieves changes in a dataset. Use since parameter to
	// continue consumption of changes in succesive requests
	Changes(since string, limit int, latestOnly bool) (EntityIterator, LayerError)
	// Entities retrieves all current entities in a dataset. Use from+limit parameters
	// to page through large datasets in batches.
	Entities(from string, limit int) (EntityIterator, LayerError)
}

type DatasetWriter interface {
	Write(entity *egdm.Entity) LayerError
	Close() LayerError
}

type DatasetDescription struct {
	Metadata    map[string]any `json:"metadata"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
}
