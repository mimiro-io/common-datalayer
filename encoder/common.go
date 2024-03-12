package encoder

import (
	"errors"
	common_datalayer "github.com/mimiro-io/common-datalayer"
	"io"
)

func NewItemIterator(sourceConfig map[string]any, data io.ReadCloser) (ItemIterator, error) {
	encoding, ok := sourceConfig["encoding"]
	if !ok {
		return nil, errors.New("no encoding specified in source config")
	}

	if encoding == "json" {
		return NewJsonItemIterator(sourceConfig, data)
	}

	return nil, nil
}

func NewItemWriter(sourceConfig map[string]any, data io.WriteCloser, batchInfo *common_datalayer.BatchInfo) (ItemWriter, error) {
	encoding, ok := sourceConfig["encoding"]
	if !ok {
		return nil, errors.New("no encoding specified in source config")
	}

	if encoding == "json" {
		return NewJsonItemWriter(sourceConfig, data, batchInfo)
	}
	if encoding == "csv" {
		return NewCSVItemWriter(sourceConfig, data, batchInfo)
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
	NewItem() common_datalayer.Item
}

type ItemIterator interface {
	Read() (common_datalayer.Item, error)
	Close() error
}

type ItemWriter interface {
	Write(item common_datalayer.Item) error
	Close() error
}
