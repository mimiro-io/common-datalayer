package encoder

import (
	"encoding/csv"
	"errors"
	common_datalayer "github.com/mimiro-io/common-datalayer"
	"io"
	"strconv"
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
	hasHeader, ok := sourceConfig["hasHeader"]
	if ok {
		writer.hasHeader = hasHeader.(bool)
		// Should this check be here?
		if writer.hasHeader {
			if batchInfo != nil {
				if batchInfo.IsStartBatch {
					// create comma separated header row from writer.columns
					//headerRow := strings.Join(writer.columns, writer.separator)
					err := writer.encoder.Write(writer.columns) // write the header for the csv-file
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}

	return writer, nil
}

func (c *CSVItemWriter) Close() error {
	return c.data.Close()
}

func (c *CSVItemWriter) Write(item common_datalayer.Item) error {
	// TODO: fix me
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
	err := c.encoder.Write(r)
	if err != nil {
		return err
	}
	c.encoder.Flush()
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
		// how to make this a rune in best way possible '' <- seems to be a way to say it's a rune, can we do that on a string?
		reader.decoder.Comma = rune(columnSeparator.(string)[0])
	}
	encoding, ok := sourceConfig["encoding"]
	if ok {
		reader.encoding = encoding.(string)
	}
	columnNames, ok := sourceConfig["columns"]
	if ok {
		reader.columns = columnNames.([]string)
	}
	validateFields, ok := sourceConfig["validateFields"]
	if ok {
		if !validateFields.(bool) {
			// if false
			reader.decoder.FieldsPerRecord = -1
		} else {
			//checks the first field and sees how many columns there is, uses that as validation going forward.
			//another option is to explicity set the number of columns in the config
			reader.decoder.FieldsPerRecord = 0
		}
	}
	header, err := dec.Read()
	if err != nil {
		return nil, err
	}
	//probably obsolete
	if len(header) > len(reader.columns) {
		return nil, errors.New("header row does not match columns in source config")
	}

	return reader, nil
}

func (c *CSVItemIterator) Close() error {
	return c.data.Close()
}

func (c *CSVItemIterator) Read() (common_datalayer.Item, error) {
	// TODO: fix me
	record, err := c.decoder.Read()

	// create item from record
	// care about data types here? look at sourceConfig for that

	// columns to ignore
	/*
		ignoreColums := backend.DecodeConfig.IgnoreColumns
		for k, v := range line {
			if slices.Contains(ignoreColums, k) {
				continue
			}
	*/
	// if err is EOF, return nil, nil then file contains no more records
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	// convert csv lines to array of structs
	var entityProps = make(map[string]interface{})
	for j, key := range c.columns {
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
