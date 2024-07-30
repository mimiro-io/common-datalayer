package encoder

import (
	"bufio"
	"encoding/json"
	"errors"
	cdl "github.com/mimiro-io/common-datalayer"
	"io"
)

func NewJsonItemFactory() ItemFactory {
	return &JsonItemFactory{}
}

type JsonItemFactory struct{}

func (j *JsonItemFactory) NewItem() cdl.Item {
	return &JsonItem{data: make(map[string]any)}
}

type JsonItemWriter struct {
	data             io.WriteCloser
	encoder          *json.Encoder
	firstItemWritten bool
	batchInfo        *cdl.BatchInfo
	logger           cdl.Logger
}

func NewJsonItemWriter(sourceConfig map[string]any, logger cdl.Logger, data io.WriteCloser, batchInfo *cdl.BatchInfo) (*JsonItemWriter, error) {
	enc := json.NewEncoder(data)
	writer := &JsonItemWriter{data: data, encoder: enc, batchInfo: batchInfo, logger: logger}

	if batchInfo != nil {
		if batchInfo.IsStartBatch {
			_, err := data.Write([]byte("[")) // write the start of the array
			if err != nil {
				return nil, err
			}
		} else {
			writer.firstItemWritten = true
		}
	} else {
		_, err := data.Write([]byte("[")) // write the start of the array
		if err != nil {
			return nil, err
		}
	}

	return writer, nil
}

func (j *JsonItemWriter) Close() error {
	if j.batchInfo != nil {
		if j.batchInfo.IsLastBatch {
			_, err := j.data.Write([]byte("]")) // write the end of the array
			if err != nil {
				return err
			}
		}
	} else {
		_, err := j.data.Write([]byte("]")) // write the end of the array
		if err != nil {
			return err
		}
	}
	return j.data.Close()
}

func (j *JsonItemWriter) Write(item cdl.Item) error {
	// if first item written, write a comma
	if j.firstItemWritten {
		_, err := j.data.Write([]byte(","))
		if err != nil {
			return err
		}
	} else {
		j.firstItemWritten = true
	}

	return j.encoder.Encode(item.NativeItem())
}

type JsonItemIterator struct {
	data    io.ReadCloser
	decoder *json.Decoder
	logger  cdl.Logger
}

func NewJsonItemIterator(sourceConfig map[string]any, logger cdl.Logger, data io.ReadCloser) (*JsonItemIterator, error) {
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

	return &JsonItemIterator{data: data, decoder: dec, logger: logger}, nil
}

func (j *JsonItemIterator) Close() error {
	return j.data.Close()
}

func (j *JsonItemIterator) Read() (cdl.Item, error) {
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

// JSONConcatenatingWriter implements the ConcatenatingWriter interface for JSON arrays.
type JSONConcatenatingWriter struct {
	output         io.WriteCloser
	firstObject    bool
	bufferedWriter *bufio.Writer
	headerWritten  bool
}

func NewJSONConcatenatingWriter(output io.WriteCloser) *JSONConcatenatingWriter {
	return &JSONConcatenatingWriter{
		output:         output,
		firstObject:    true,
		bufferedWriter: bufio.NewWriter(output),
	}
}

// Write writes a part of a JSON array to the target output.
func (m *JSONConcatenatingWriter) Write(reader io.ReadCloser) (err error) {
	defer func() {
		closeErr := reader.Close()
		if err == nil {
			err = closeErr
		}
	}()

	// write the opening of the array if not already done
	if !m.headerWritten {
		m.bufferedWriter.WriteString("[")
		m.headerWritten = true
	}

	decoder := json.NewDecoder(reader)
	// Read the opening bracket of the JSON array
	t, err := decoder.Token()
	if err != nil {
		return err
	}
	if t != json.Delim('[') {
		return io.ErrUnexpectedEOF
	}

	for decoder.More() {
		var obj json.RawMessage
		if err := decoder.Decode(&obj); err != nil {
			return err
		}
		if !m.firstObject {
			if _, err := m.bufferedWriter.WriteString(","); err != nil {
				return err
			}
		} else {
			m.firstObject = false
		}
		if _, err := m.bufferedWriter.Write(obj); err != nil {
			return err
		}
	}

	// Read the closing bracket of the JSON array
	t, err = decoder.Token()
	if err != nil {
		return err
	}
	if t != json.Delim(']') {
		return io.ErrUnexpectedEOF
	}

	return nil
}

// Close finalizes the JSON writing process.
func (m *JSONConcatenatingWriter) Close() error {
	m.bufferedWriter.WriteString("]")
	m.bufferedWriter.Flush()
	return m.output.Close()
}
