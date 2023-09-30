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
	Context() *egdm.Context
	Next() (*egdm.Entity, LayerError)
	Token() (*egdm.Continuation, LayerError)
	Close() LayerError
}

type Dataset interface {
	MetaData() map[string]any
	Name() string

	FullSync(ctx context.Context, batchInfo BatchInfo) (DatasetWriter, LayerError)
	Incremental(ctx context.Context) (DatasetWriter, LayerError)

	Changes(since string, take int, latestOnly bool) (EntityIterator, LayerError)
	Entities(since string, take int) (EntityIterator, LayerError)
}

type DatasetWriter interface {
	Write(entity *egdm.Entity) LayerError
	Close() LayerError
}

type DatasetDescription struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Metadata    map[string]any `json:"metadata"`
}
