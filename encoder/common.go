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

func NewItemWriter(sourceConfig map[string]any, data io.WriteCloser) (ItemWriter, error) {
	encoding, ok := sourceConfig["encoding"]
	if !ok {
		return nil, errors.New("no encoding specified in source config")
	}

	if encoding == "json" {
		return NewJsonItemWriter(sourceConfig, data)
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
