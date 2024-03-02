package encoder

import (
	"encoding/csv"
	"errors"
	common_datalayer "github.com/mimiro-io/common-datalayer"
	"io"
	"slices"
	"strconv"
)

func NewCSVItemFactory() ItemFactory {
	return &CSVItemFactory{}
}

type CSVItemFactory struct{}

func (c *CSVItemFactory) NewItem() common_datalayer.Item {
	return &CSVItem{data: make(map[string]any)}
}

type CSVItemWriter struct {
	data             io.WriteCloser
	batchInfo        *common_datalayer.BatchInfo
	writer           *csv.Writer
	columns          []string
	hasHeader        bool
	separator        string
	encoding         string
	validateFields   bool
	firstItemWritten bool
}

func NewCSVItemWriter(sourceConfig map[string]any, data io.WriteCloser, batchInfo *common_datalayer.BatchInfo) (*CSVItemWriter, error) {
	enc := csv.NewWriter(data)
	writer := &CSVItemWriter{data: data, writer: enc, batchInfo: batchInfo}

	// this can be implicitly creating the order of the columns
	// OR we add the order field to the config to specify the order of the columns
	// trying implicit for now, add to docs.
	columnNames, ok := sourceConfig["columns"]
	if ok {
		writer.columns = columnNames.([]string)
	}
	columnSeparator, ok := sourceConfig["columnSeparator"]
	if ok {
		var err error
		writer.separator = columnSeparator.(string)
		writer.writer.Comma, err = stringToRune(columnSeparator.(string))
		if err != nil {
			return nil, errors.New("input string does not match allowed characters")
		}
	}

	encoding, ok := sourceConfig["encoding"]
	if ok {
		writer.encoding = encoding.(string)
	}
	validateFields, ok := sourceConfig["validateFields"]
	if ok {
		writer.validateFields = validateFields.(bool)
	}
	hasHeader, ok := sourceConfig["hasHeader"]
	// if no hasHeader still want to write data based on columns

	if ok {
		writer.hasHeader = hasHeader.(bool)
		if writer.hasHeader {
			if batchInfo != nil {
				if batchInfo.IsStartBatch {
					err := writer.writer.Write(writer.columns) // write the header for the csv-file
					if err != nil {
						return nil, err
					}
				}
			} else {
				err := writer.writer.Write(writer.columns) // write the header for the csv-file
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return writer, nil
}

func (c *CSVItemWriter) Close() error {
	c.writer.Flush()
	return c.data.Close()
}

func (c *CSVItemWriter) Write(item common_datalayer.Item) error {
	written := 0
	var r []string

	row := item.NativeItem().(map[string]any)
	for _, h := range c.columns {
		if _, ok := row[h]; ok {
			switch v := row[h].(type) {
			case float64:
				r = append(r, strconv.FormatFloat(v, 'f', 0, 64))
			case string:
				r = append(r, v)
			case bool:
				r = append(r, strconv.FormatBool(v))
			case int:
				r = append(r, strconv.Itoa(v))
			case int64:
				r = append(r, strconv.FormatInt(v, 10))
			case int32:
				r = append(r, strconv.FormatInt(int64(v), 10))
			case int16:
				r = append(r, strconv.FormatInt(int64(v), 10))
			case int8:
				r = append(r, strconv.FormatInt(int64(v), 10))
			case uint:
				r = append(r, strconv.FormatUint(uint64(v), 10))
			case uint64:
				r = append(r, strconv.FormatUint(v, 10))
			case uint32:
				r = append(r, strconv.FormatUint(uint64(v), 10))
			case uint16:
				r = append(r, strconv.FormatUint(uint64(v), 10))
			case uint8:
				r = append(r, strconv.FormatUint(uint64(v), 10))
			default:
				r = append(r, "")
			}
		} else {
			r = append(r, "")
		}
	}
	for _, col := range r {
		written += len([]byte(col))
	}
	err := c.writer.Write(r)
	if err != nil {
		return err
	}
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
	ignoreColumns  []string
}

func NewCSVItemIterator(sourceConfig map[string]any, data io.ReadCloser) (*CSVItemIterator, error) {
	dec := csv.NewReader(data)
	reader := &CSVItemIterator{data: data, decoder: dec}

	columnSeparator, ok := sourceConfig["columnSeparator"]
	if ok {
		// only working with characters for now, tabs doesn't work
		//TODO: add support for tabs
		//reader.decoder.Comma = rune(columnSeparator.(string)[0])
		err := errors.New("input string does not match allowed characters")
		reader.decoder.Comma, err = stringToRune(columnSeparator.(string))
		if err != nil {
			return nil, err
		}
	}
	encoding, ok := sourceConfig["encoding"]
	if ok {
		reader.encoding = encoding.(string)
	}
	columnNames, ok := sourceConfig["columns"]
	if ok {
		reader.columns = columnNames.([]string)
	}
	ignoreColumns, ok := sourceConfig["ignoreColumns"]
	if ok {
		reader.ignoreColumns = ignoreColumns.([]string)
	}
	validateFields, ok := sourceConfig["validateFields"]
	if ok {
		if !validateFields.(bool) {
			reader.decoder.FieldsPerRecord = -1
		} else {
			//checks the first field and sees how many columns there is, uses that as validation going forward.
			//another option is to explicity set the number of columns in the config
			reader.decoder.FieldsPerRecord = len(reader.columns)
		}
	} else {
		// default is not to validate fields
		reader.decoder.FieldsPerRecord = -1
	}
	header, err := reader.decoder.Read()
	if err != nil {
		return nil, err
	}
	if len(header) > len(reader.columns) {
		return nil, errors.New("header row does not match columns in source config")
	}

	return reader, nil
}

func (c *CSVItemIterator) Close() error {
	return c.data.Close()
}

func (c *CSVItemIterator) Read() (common_datalayer.Item, error) {
	record, err := c.decoder.Read()
	// care about data types here? look at sourceConfig for that if needed the config might be extended as:
	// columns: [{name: "name", type: "string"}, {name: "age", type: "int"}]
	// this means a change to how we are reading out columns in NewCSVItemIterator and NewCSVItemWriter

	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	var entityProps = make(map[string]interface{})
	for j, key := range c.columns {
		if slices.Contains(c.ignoreColumns, key) {
			continue
		}
		entityProps[key] = record[j]
	}

	return &CSVItem{data: entityProps}, nil
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

func stringToRune(input string) (rune, error) {
	switch input {
	case ",":
		return ',', nil
	case "\t":
		return '\t', nil
	case " ":
		return ' ', nil
	default:
		return 0, errors.New("input string does not match allowed characters")
	}
}
