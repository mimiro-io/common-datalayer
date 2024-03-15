package encoder

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	cdl "github.com/mimiro-io/common-datalayer"
	"io"
	"strings"
	"time"
)

func NewParquetItemFactory() ItemFactory {
	return &ParquetItemFactory{}
}

type ParquetItemFactory struct{}

func (c *ParquetItemFactory) NewItem() cdl.Item {
	return &ParquetItem{data: make(map[string]any)}
}

type ParquetEncoderConfig struct {
	SchemaDef      *parquetschema.SchemaDefinition `json:"schema"`
	IgnoreColumns  []string                        `json:"ignore_columns"`
	FlushThreshold int64                           `json:"flush_threshold"`
}

func NewParquetEncoderConfig(sourceConfig map[string]any) (*ParquetEncoderConfig, error) {
	if sourceConfig["schema"] == nil {
		return nil, errors.New("no schema specified in source config")
	}
	var err error
	sourceConfig["schema"], err = parquetschema.ParseSchemaDefinition(sourceConfig["schema"].(string))
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(sourceConfig)
	if err != nil {
		return nil, err
	}
	config := &ParquetEncoderConfig{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func NewParquetDecoderConfig(sourceConfig map[string]any) (*ParquetEncoderConfig, error) {
	if sourceConfig["schema"] == nil {
		return nil, errors.New("no schema specified in source config")
	}
	var err error
	sourceConfig["schema"], err = parquetschema.ParseSchemaDefinition(sourceConfig["schema"].(string))
	if err != nil {
		return nil, err
	}

	data, err := json.Marshal(sourceConfig)
	if err != nil {
		return nil, err
	}
	config := &ParquetEncoderConfig{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}
	if config.FlushThreshold == 0 {
		config.FlushThreshold = 1 * 1024 * 1024
	}
	// parse json to parquet schema string and pass that to schema-parser
	return config, nil
}

type ParquetItemWriter struct {
	data             io.WriteCloser // don't need this any further, no?
	batchInfo        *cdl.BatchInfo
	writer           *goparquet.FileWriter
	firstItemWritten bool
	config           *ParquetEncoderConfig
}

func NewParquetItemWriter(sourceConfig map[string]any, data io.WriteCloser, batchInfo *cdl.BatchInfo) (*ParquetItemWriter, error) {
	config, err := NewParquetEncoderConfig(sourceConfig)
	if err != nil {
		return nil, err
	}

	enc := goparquet.NewFileWriter(data, goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY), goparquet.WithSchemaDefinition(config.SchemaDef))
	writer := &ParquetItemWriter{data: data, writer: enc, batchInfo: batchInfo, config: config}

	return writer, nil
}

func (c *ParquetItemWriter) Close() error {
	err := c.writer.Close()
	if err != nil {
		return err
	}
	// log size here if needed
	//size: = c.writer.CurrentRowGroupSize()
	return c.data.Close()
}

func (c *ParquetItemWriter) Write(item cdl.Item) error {
	row := item.NativeItem().(map[string]any)
	for _, h := range c.config.SchemaDef.RootColumn.Children {
		val, ok := row[h.SchemaElement.Name]
		if ok && val != nil {
			i, err := convertType(val, h.SchemaElement.Type, h.SchemaElement.LogicalType)
			if err != nil {
				return err
			}
			row[h.SchemaElement.Name] = i
		}
	}
	err := c.writer.AddData(row)
	if err != nil {
		return err
	}
	written := c.writer.CurrentRowGroupSize()
	if written > c.config.FlushThreshold {
		err := c.writer.FlushRowGroup()
		if err != nil {
			return err
		}
	}
	return nil
}
func convertType(val interface{}, t *parquet.Type, logicalType *parquet.LogicalType) (interface{}, error) {
	switch *t {
	case parquet.Type_BOOLEAN:
		r, ok := val.(bool)
		if !ok {
			return nil, errors.New(fmt.Sprintf("could not convert %+v to bool", val))
		}
		return r, nil
	case parquet.Type_INT32:
		if logicalType == nil {
			return int32(val.(int)), nil
		}
		if logicalType.IsSetDATE() {
			d := val.(time.Time)
			duration := d.Sub(time.Unix(0, 0))
			return int32(duration.Hours() / 24), nil
		}
		return nil, errors.New(fmt.Sprintf("unsupported logical type for base type %+v: %+v", t, logicalType))
	case parquet.Type_INT64:
		if logicalType == nil {
			switch val.(type) {
			case float64:
				return int64(val.(float64)), nil
			default:
				return int64(val.(int)), nil
			}
		}
		if logicalType.IsSetTIME() {
			d := val.(time.Time)
			return d.UnixNano(), nil
		}
		return nil, errors.New(fmt.Sprintf("unsupported logical type for base type %+v: %+v", t, logicalType))
	case parquet.Type_FLOAT:
		return float32(val.(float64)), nil
	case parquet.Type_DOUBLE:
		return val.(float64), nil
	case parquet.Type_BYTE_ARRAY:
		if logicalType == nil {
			return val.([]byte), nil
		}
		if logicalType.IsSetSTRING() {
			r, ok := concatStringSlice(val)
			if !ok {
				return nil, errors.New(fmt.Sprintf("could not convert %+v to string", val))
			}
			return []byte(r), nil
		}
		return nil, errors.New(fmt.Sprintf("unsupported logical type for base type %+v: %+v", t, logicalType))
	default:
		return nil, errors.New(fmt.Sprintf("unsupported datatype: %+v", t))
	}
}
func concatStringSlice(value any) (string, bool) {
	var output string
	success := true
	var values []string
	switch value.(type) {
	case []string:
		for _, val := range value.([]string) {
			values = append(values, val)
		}
		output = strings.Join(values, ",")
	case []any:
		for _, val := range value.([]any) {
			values = append(values, val.(string))
		}
		output = strings.Join(values, ",")
	default:
		output, success = value.(string)
	}
	return output, success
}

type ParquetItemIterator struct {
	data   io.ReadCloser // probably don't need this, but can we do something about the readseeker?
	reader *goparquet.FileReader
	config *ParquetEncoderConfig
}

func NewParquetItemIterator(sourceConfig map[string]any, data io.ReadCloser) (*ParquetItemIterator, error) {
	config, err := NewParquetDecoderConfig(sourceConfig)
	if err != nil {
		return nil, err
	}
	// create slice of all columns we want to read based on schemadef
	var columns []string
	for _, h := range config.SchemaDef.RootColumn.Children {
		columns = append(columns, h.SchemaElement.Name)
	}
	dataBytes, err := io.ReadAll(data)
	dec, _ := goparquet.NewFileReader(bytes.NewReader(dataBytes), columns...)
	// don't need data anymore after this
	reader := &ParquetItemIterator{data: data, reader: dec, config: config}

	return reader, nil
}

func (c *ParquetItemIterator) Close() error {
	return c.data.Close()
}

func (c *ParquetItemIterator) Read() (cdl.Item, error) {
	record, err := c.reader.NextRow()
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	var entityProps = make(map[string]interface{})
	// needs to convert to correct types based on schema
	for _, key := range c.config.SchemaDef.RootColumn.Children {
		if key.SchemaElement.LogicalType != nil {
			value := fmt.Sprintf("%s", record[key.SchemaElement.Name])
			entityProps[key.SchemaElement.Name] = value
		} else {
			entityProps[key.SchemaElement.Name] = record[key.SchemaElement.Name]
		}
	}
	return &ParquetItem{data: entityProps}, nil
}

type ParquetItem struct {
	data map[string]any
}

func (item *ParquetItem) GetValue(key string) any {
	return item.data[key]
}

func (item *ParquetItem) SetValue(key string, value any) {
	item.data[key] = value
}

func (item *ParquetItem) GetPropertyNames() []string {
	keys := make([]string, 0, len(item.data))
	for k := range item.data {
		keys = append(keys, k)
	}
	return keys
}

func (item *ParquetItem) NativeItem() any {
	return item.data
}
