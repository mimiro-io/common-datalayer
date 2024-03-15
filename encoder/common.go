package encoder

import (
	"errors"
	cdl "github.com/mimiro-io/common-datalayer"
	"io"
)

func NewItemIterator(sourceConfig map[string]any, logger cdl.Logger, data io.ReadCloser) (ItemIterator, error) {
	encoding, ok := sourceConfig["encoding"]
	if !ok {
		return nil, errors.New("no encoding specified in source config")
	}

	if encoding == "json" {
		return NewJsonItemIterator(sourceConfig, logger, data)
	}

	if encoding == "csv" {
		return NewCSVItemIterator(sourceConfig, logger, data)
	}

	return nil, nil
}

func NewItemWriter(sourceConfig map[string]any, logger cdl.Logger, data io.WriteCloser, batchInfo *cdl.BatchInfo) (ItemWriter, error) {
	encoding, ok := sourceConfig["encoding"]
	if !ok {
		return nil, errors.New("no encoding specified in source config")
	}

	if encoding == "json" {
		return NewJsonItemWriter(sourceConfig, logger, data, batchInfo)
	}
	if encoding == "csv" {
		return NewCSVItemWriter(sourceConfig, logger, data, batchInfo)
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
