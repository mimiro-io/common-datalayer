package encoder

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	cdl "github.com/mimiro-io/common-datalayer"
	"io"
	"slices"
	"strconv"
)

func NewCSVItemFactory() ItemFactory {
	return &CSVItemFactory{}
}

type CSVItemFactory struct{}

func (c *CSVItemFactory) NewItem() cdl.Item {
	return &CSVItem{data: make(map[string]any)}
}

type CSVEncoderConfig struct {
	Columns        []string `json:"columns"`
	HasHeader      bool     `json:"has_header"`
	Separator      string   `json:"separator"`
	FileEncoding   string   `json:"file_encoding"`
	ValidateFields bool     `json:"validate_fields"`
	IgnoreColumns  []string `json:"ignore_columns"`
}

func NewCSVEncoderConfig(sourceConfig map[string]any) (*CSVEncoderConfig, error) {
	// json it first
	data, err := json.Marshal(sourceConfig)
	if err != nil {
		return nil, err
	}

	config := &CSVEncoderConfig{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

type CSVItemWriter struct {
	data             io.WriteCloser
	batchInfo        *cdl.BatchInfo
	writer           *csv.Writer
	firstItemWritten bool
	config           *CSVEncoderConfig
	logger           cdl.Logger
}

func NewCSVItemWriter(sourceConfig map[string]any, logger cdl.Logger, data io.WriteCloser, batchInfo *cdl.BatchInfo) (*CSVItemWriter, error) {
	config, err := NewCSVEncoderConfig(sourceConfig)
	if err != nil {
		return nil, err
	}

	enc := csv.NewWriter(data)
	writer := &CSVItemWriter{data: data, writer: enc, batchInfo: batchInfo, config: config, logger: logger}

	if config.Separator != "" {
		writer.writer.Comma, err = stringToRune(config.Separator)
		if err != nil {
			return nil, errors.New("input string does not match allowed characters")
		}
	}

	if config.HasHeader {
		if batchInfo != nil {
			if batchInfo.IsStartBatch {
				err := writer.writer.Write(config.Columns)
				if err != nil {
					return nil, err
				}
			}
		} else {
			err := writer.writer.Write(config.Columns)
			if err != nil {
				return nil, err
			}
		}
	}

	return writer, nil
}

func (c *CSVItemWriter) Close() error {
	c.writer.Flush()
	return c.data.Close()
}

func (c *CSVItemWriter) Write(item cdl.Item) error {
	written := 0
	var r []string

	row := item.NativeItem().(map[string]any)
	for _, h := range c.config.Columns {
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
	data    io.ReadCloser
	decoder *csv.Reader
	config  *CSVEncoderConfig
	logger  cdl.Logger
}

func NewCSVItemIterator(sourceConfig map[string]any, logger cdl.Logger, data io.ReadCloser) (*CSVItemIterator, error) {
	config, err := NewCSVEncoderConfig(sourceConfig)
	if err != nil {
		return nil, err
	}

	dec := csv.NewReader(data)
	reader := &CSVItemIterator{data: data, decoder: dec, config: config, logger: logger}

	if config.Separator != "" {
		err := errors.New("input string does not match allowed characters")
		reader.decoder.Comma, err = stringToRune(config.Separator)
		if err != nil {
			return nil, err
		}
	}

	reader.decoder.FieldsPerRecord = -1
	if config.ValidateFields {
		reader.decoder.FieldsPerRecord = len(reader.config.Columns)
	}

	if reader.config.HasHeader {
		header, err := reader.decoder.Read()
		if err != nil {
			return nil, err
		}

		if len(header) > len(reader.config.Columns) {
			return nil, errors.New("header row does not match columns in source config")
		}
	}

	return reader, nil
}

func (c *CSVItemIterator) Close() error {
	return c.data.Close()
}

func (c *CSVItemIterator) Read() (cdl.Item, error) {
	record, err := c.decoder.Read()

	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	var entityProps = make(map[string]interface{})
	for j, key := range c.config.Columns {
		if slices.Contains(c.config.IgnoreColumns, key) {
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
