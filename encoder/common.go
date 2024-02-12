package encoder

import (
	"errors"
	common_datalayer "github.com/mimiro-io/common-datalayer"
	"io"
)

func NewItemReadCloser(sourceConfig map[string]any, data io.ReadCloser) (ItemReadCloser, error) {
	encoding, ok := sourceConfig["encoding"]
	if !ok {
		return nil, errors.New("no encoding specified in source config")
	}

	if encoding == "json" {
		return NewJsonItemReadCloser(sourceConfig, data)
	}

	return nil, nil
}

type ItemReadCloser interface {
	Read() (common_datalayer.Item, error)
	Close() error
}

type ItemWriterCloser interface {
	Write(item common_datalayer.Item) error
}
