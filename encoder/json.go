package encoder

import (
	"encoding/json"
	"errors"
	common_datalayer "github.com/mimiro-io/common-datalayer"
	"io"
)

type JsonItemFactory struct{}

func (j *JsonItemFactory) NewItem() common_datalayer.Item {
	return &JsonItem{data: make(map[string]any)}
}

type JsonItemWriterCloser struct {
	data    io.WriteCloser
	encoder *json.Encoder
}

func NewJsonItemWriterCloser(sourceConfig map[string]any, data io.WriteCloser) (*JsonItemWriterCloser, error) {
	enc := json.NewEncoder(data)
	return &JsonItemWriterCloser{data: data, encoder: enc}, nil
}

func (j *JsonItemWriterCloser) Close() error {
	return j.data.Close()
}

func (j *JsonItemWriterCloser) Write(item common_datalayer.Item) error {
	return j.encoder.Encode(item.NativeItem())
}

type JsonItemReadCloser struct {
	data    io.ReadCloser
	decoder *json.Decoder
}

func NewJsonItemReadCloser(sourceConfig map[string]any, data io.ReadCloser) (*JsonItemReadCloser, error) {
	// different ways the data can be encoded
	// 1. array of objects
	// 2. object with a key that is an array of objects
	// 3. Just an object
	// assume 1. for now.

	dec := json.NewDecoder(data)

	// check the start is an array token
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}

	if token != json.Delim('[') {
		return nil, errors.New("expected [ at start of data stream")
	}

	return &JsonItemReadCloser{data: data, decoder: dec}, nil
}

func (j *JsonItemReadCloser) Close() error {
	return j.data.Close()
}

func (j *JsonItemReadCloser) Read() (common_datalayer.Item, error) {
	if j.decoder.More() {
		var obj map[string]interface{}
		err := j.decoder.Decode(&obj)
		if err != nil {
			return nil, err
		}

		return &JsonItem{data: obj}, nil
	}
	return nil, nil
}

type JsonItem struct {
	data map[string]any
}

func (item *JsonItem) GetValue(key string) any {
	return item.data[key]
}

func (item *JsonItem) SetValue(key string, value any) {
	item.data[key] = value
}

func (item *JsonItem) GetPropertyNames() []string {
	keys := make([]string, 0, len(item.data))
	for k := range item.data {
		keys = append(keys, k)
	}
	return keys
}

func (item *JsonItem) NativeItem() any {
	return item.data
}
