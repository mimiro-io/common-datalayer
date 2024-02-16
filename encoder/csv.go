package encoder

import (
	common_datalayer "github.com/mimiro-io/common-datalayer"
	"io"
)

func NewCSVItemFactory() ItemFactory {
	return &CSVItemFactory{}
}

type CSVItemFactory struct{}

func (j *CSVItemFactory) NewItem() common_datalayer.Item {
	return &CSVItem{data: make(map[string]any)}
}

type CSVItemWriter struct {
	data      io.WriteCloser
	batchInfo *common_datalayer.BatchInfo
	columns   []string
	hasHeader bool
	separator string
}

func NewCSVItemWriter(sourceConfig map[string]any, data io.WriteCloser, batchInfo *common_datalayer.BatchInfo) (*CSVItemWriter, error) {
	writer := &CSVItemWriter{data: data, batchInfo: batchInfo}

	columnsNames, ok := sourceConfig["columns"]
	if ok {
		writer.columns = columnsNames.([]string)
	}

	hasHeader, ok := sourceConfig["hasHeader"]
	if ok {
		writer.hasHeader = hasHeader.(bool)
	}

	columnSeparator, ok := sourceConfig["columnSeparator"]
	if ok {
		writer.separator = columnSeparator.(string)
	}

	return writer, nil
}

func (j *CSVItemWriter) Close() error {
	return j.data.Close()
}

func (j *CSVItemWriter) Write(item common_datalayer.Item) error {
	// TODO: fix me
	return nil
}

type CSVItemIterator struct {
	data io.ReadCloser
}

func NewCSVItemIterator(sourceConfig map[string]any, data io.ReadCloser) (*CSVItemIterator, error) {
	// TODO: fix me

	return &CSVItemIterator{data: data}, nil
}

func (j *CSVItemIterator) Close() error {
	return j.data.Close()
}

func (j *CSVItemIterator) Read() (common_datalayer.Item, error) {
	// TODO: fix me
	return nil, nil
}

type CSVItem struct {
	data map[string]any
}

func (item *CSVItem) GetValue(key string) any {
	return item.data[key]
}

func (item *CSVItem) SetValue(key string, value any) {
	item.data[key] = value
}

func (item *CSVItem) GetPropertyNames() []string {
	keys := make([]string, 0, len(item.data))
	for k := range item.data {
		keys = append(keys, k)
	}
	return keys
}

func (item *CSVItem) NativeItem() any {
	return item.data
}
