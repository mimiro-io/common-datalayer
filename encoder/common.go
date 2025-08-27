package encoder

import (
	"errors"
	cdl "github.com/mimiro-io/common-datalayer"
	"io"
)

func NewItemIterator(sourceConfig map[string]any, logger cdl.Logger, data io.ReadCloser) (ItemIterator, error) {
	encoding, ok := sourceConfig["encoding"]
	if !ok {
		logger.Error("no encoding specified in source config")
		return nil, errors.New("no encoding specified in source config")
	}
	logger.Debug("Creating item iterator", "encoding", encoding)
	if encoding == "json" {
		return NewJsonItemIterator(sourceConfig, logger, data)
	}
	if encoding == "csv" {
		return NewCSVItemIterator(sourceConfig, logger, data)
	}
	if encoding == "parquet" {
		return NewParquetItemIterator(sourceConfig, data)
	}

	return nil, nil
}

func NewItemWriter(sourceConfig map[string]any, logger cdl.Logger, data io.WriteCloser, batchInfo *cdl.BatchInfo) (ItemWriter, error) {
	encoding, ok := sourceConfig["encoding"]
	if !ok {
		logger.Error("no encoding specified in source config")
		return nil, errors.New("no encoding specified in source config")
	}
	logger.Debug("Creating item writer", "encoding", encoding)

	if encoding == "json" {
		return NewJsonItemWriter(sourceConfig, logger, data, batchInfo)
	}
	if encoding == "csv" {
		return NewCSVItemWriter(sourceConfig, logger, data, batchInfo)
	}
	if encoding == "parquet" {
		return NewParquetItemWriter(sourceConfig, data, batchInfo)
	}
	if encoding == "flatfile" {
		return NewFlatFileItemWriter(sourceConfig, data, batchInfo)
	}

	return nil, nil
}

func NewItemFactory(sourceConfig map[string]any) (ItemFactory, error) {
	encoding, ok := sourceConfig["encoding"]
	if !ok {
		return nil, errors.New("no encoding specified in source config")
	}

	if encoding == "json" {
		return &JsonItemFactory{}, nil
	}
	if encoding == "csv" {
		return &CSVItemFactory{}, nil

	}
	if encoding == "flatfile" {
		return &FlatFileItemFactory{}, nil
	}
	if encoding == "parquet" {
		return &ParquetItemFactory{}, nil
	}

	return nil, nil
}

func NewConcatenatingWriter(sourceConfig map[string]any, output io.WriteCloser) (ConcatenatingWriter, error) {
	encoding, ok := sourceConfig["encoding"]
	if !ok {
		return nil, errors.New("no encoding specified in source config")
	}

	if encoding == "json" {
		return NewJSONConcatenatingWriter(output), nil
	}

	if encoding == "csv" {
		hasHeader := sourceConfig["has_header"].(bool)
		return NewCSVConcatenatingWriter(output, hasHeader), nil
	}

	if encoding == "flatfile" {
		return NewGenericConcatenatingWriter(output), nil
	}

	if encoding == "parquet" {
		return NewParquetConcatenatingWriter(output), nil
	}

	return nil, nil
}

type ItemFactory interface {
	NewItem() cdl.Item
}

type ItemIterator interface {
	Read() (cdl.Item, error)
	Close() error
}

type ItemWriter interface {
	Write(item cdl.Item) error
	Close() error
}
