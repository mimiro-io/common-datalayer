package encoder

import (
	"encoding/csv"
	"errors"
	"fmt"
	common_datalayer "github.com/mimiro-io/common-datalayer"
	"io"
	"strconv"
	"strings"
)

func NewCSVItemFactory() ItemFactory {
	return &CSVItemFactory{}
}

type CSVItemFactory struct{}

func (c *CSVItemFactory) NewItem() common_datalayer.Item {
	return &CSVItem{data: make(map[string]any)}
}

// TODO: fix me
// differentiate between ENCODER and ENCODING with better naming
type CSVItemWriter struct {
	data             io.WriteCloser
	batchInfo        *common_datalayer.BatchInfo
	encoder          *csv.Writer
	columns          []string
	hasHeader        bool
	separator        string
	encoding         string
	validateFields   bool
	headerWritten    bool
	firstItemWritten bool
}

func NewCSVItemWriter(sourceConfig map[string]any, data io.WriteCloser, batchInfo *common_datalayer.BatchInfo) (*CSVItemWriter, error) {
	enc := csv.NewWriter(data)
	writer := &CSVItemWriter{data: data, encoder: enc, batchInfo: batchInfo}

	// this can be implicitly creating the order of the columns
	// OR we add the order field to the config to specify the order of the columns
	// trying implicit for now, add to docs.
	columnNames, ok := sourceConfig["columns"]
	if ok {
		writer.columns = columnNames.([]string)
	}
	hasHeader, ok := sourceConfig["hasHeader"]
	if ok {
		writer.hasHeader = hasHeader.(bool)
		// Should this check be here?
		if writer.hasHeader {
			if batchInfo != nil {
				if batchInfo.IsStartBatch {
					// create comma separated header row from writer.columns
					headerRow := strings.Join(writer.columns, writer.separator)
					_, err := data.Write([]byte(headerRow)) // write the header for the csv-file
					// set headerWritten to true, so we don't write the header again
					writer.headerWritten = true
					if err != nil {
						return nil, err
					}
				} else {
					writer.firstItemWritten = true
				}
			}
		}
	}
	columnSeparator, ok := sourceConfig["columnSeparator"]
	if ok {
		writer.separator = columnSeparator.(string)
	}
	encoding, ok := sourceConfig["encoding"]
	if ok {
		writer.encoding = encoding.(string)
	}
	validateFields, ok := sourceConfig["validateFields"]
	if ok {
		writer.validateFields = validateFields.(bool)
	}
	return writer, nil
}

func (c *CSVItemWriter) Close() error {
	return c.data.Close()
}

func (c *CSVItemWriter) Write(item common_datalayer.Item) error {
	// TODO: fix me
	// should this check be here?
	written := 0
	if c.headerWritten {
		if c.firstItemWritten {
			var r []string

			row := make(map[string]interface{})

			for _, h := range c.columns {
				if _, ok := row[h]; ok {
					switch v := row[h].(type) {
					case float64:
						r = append(r, strconv.FormatFloat(v, 'f', 0, 64))
					case string:
						r = append(r, v)
					case bool:
						r = append(r, strconv.FormatBool(v))
					}
				} else {
					r = append(r, "")
				}
			}
			for _, col := range r {
				written += len([]byte(col))
			}
			err := c.Write(item)
			if err != nil {
				return err
			}
			_, err = c.data.Write([]byte(","))
			if err != nil {
				return err
			}
		} else {
			c.firstItemWritten = true
		}
	} else {
		// write the header
		headerRow := strings.Join(c.columns, c.separator)
		_, err := c.data.Write([]byte(headerRow)) // write the header for the csv-file
		// set headerWritten to true, so we don't write the header again
		c.headerWritten = true
		if err != nil {
			return err
		}
	}
	/*	if writer.hasHeader {
		if batchInfo != nil {
			if batchInfo.IsStartBatch {
				// create comma separated header row from writer.columns
				headerRow := strings.Join(writer.columns, writer.separator)
				_, err := data.Write([]byte(headerRow)) // write the header for the csv-file
				// set headerWritten to true, so we don't write the header again
				writer.headerWritten = true
				if err != nil {
					return nil, err
				}
			}
		}
	}*/
	return nil
}

type CSVItemIterator struct {
	data           io.ReadCloser
	decoder        *csv.Reader
	validateFields bool
	hasHeader      bool
	columns        []string
	separator      string
	encoding       string
}

func NewCSVItemIterator(sourceConfig map[string]any, data io.ReadCloser) (*CSVItemIterator, error) {
	// TODO: fix me
	dec := csv.NewReader(data)
	reader := &CSVItemIterator{data: data, decoder: dec}

	columnSeparator, ok := sourceConfig["columnSeparator"]
	if ok {
		reader.separator = columnSeparator.(string)
	}
	encoding, ok := sourceConfig["encoding"]
	if ok {
		reader.encoding = encoding.(string)
	}
	columnNames, ok := sourceConfig["columns"]
	if ok {
		reader.columns = columnNames.([]string)
	}
	// check if header is present and if so, validate amount fields
	// do we want to do this here?
	header, err := dec.Read()
	fmt.Sprintf(strings.Join(header, reader.separator))

	if err != nil {
		return nil, err
	}
	//
	if len(header) > len(reader.columns) {
		return nil, errors.New("header row does not match columns in source config")
	}

	return &CSVItemIterator{data: data, decoder: dec}, nil
}

func (c *CSVItemIterator) Close() error {
	return c.data.Close()
}

func (c *CSVItemIterator) Read() (common_datalayer.Item, error) {
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
