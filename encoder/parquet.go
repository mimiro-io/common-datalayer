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

type ParquetJsonSchema struct {
	Columns []ParquetJsonSchemaColumn `json:"schema"`
}

type ParquetJsonSchemaColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
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
	//TODO: create JSON-parser for parquet schema
	schemaString, err := createParquetSchema(sourceConfig["schema"], sourceConfig["parquet_name"].(string))

	if err != nil {
		return nil, err
	}

	//sourceConfig["schema"], err = parquetschema.ParseSchemaDefinition(sourceConfig["schema"].(string))
	sourceConfig["schema"], err = parquetschema.ParseSchemaDefinition(schemaString)
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

func createParquetSchema(schema any, parquetName string) (string, error) {
	byteSchema, err := json.Marshal(schema)
	if err != nil {
		return "", err
	}
	columns := &[]ParquetJsonSchemaColumn{}
	json.Unmarshal(byteSchema, columns)
	jsonSchema := &ParquetJsonSchema{}
	if err != nil {
		return "", err
	}
	jsonSchema.Columns = *columns
	parquetSchema := fmt.Sprintf("message %s {", parquetName)
	for _, h := range jsonSchema.Columns {
		if h.Type == "string" {
			if h.Required {
				parquetSchema += fmt.Sprintf("required binary %s (STRING);", h.Name)
			} else {
				parquetSchema += fmt.Sprintf("optional binary %s (STRING);", h.Name)
			}
		} else {
			if h.Required {
				parquetSchema += fmt.Sprintf("required %s %s;", h.Type, h.Name)
			} else {
				parquetSchema += fmt.Sprintf("optional %s %s;", h.Type, h.Name)
			}

		}
	}
	parquetSchema += "}"
	return parquetSchema, nil
}

type ParquetItemWriter struct {
	data             io.WriteCloser
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

// ParquetConcatenatingWriter implements the ConcatenatingWriter interface for Parquet files.
type ParquetConcatenatingWriter struct {
	output    io.WriteCloser
	writer    *goparquet.FileWriter
	schema    *parquetschema.SchemaDefinition
	schemaSet bool
}

// NewParquetConcatenatingWriter creates a new ParquetConcatenatingWriter.
func NewParquetConcatenatingWriter(output io.WriteCloser) *ParquetConcatenatingWriter {
	return &ParquetConcatenatingWriter{
		output:    output,
		schemaSet: false,
	}
}

// Write writes a part of a Parquet file to the target output.
func (m *ParquetConcatenatingWriter) Write(reader io.ReadCloser) (err error) {
	defer func() {
		closeErr := reader.Close()
		if err == nil {
			err = closeErr
		}
	}()

	// Read the entire content of the reader into memory
	content, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read parquet file: %w", err)
	}

	// Create a new bytes.Reader from the content, which implements io.ReadSeeker
	memReader := bytes.NewReader(content)

	fr, err := goparquet.NewFileReader(memReader)
	if err != nil {
		return fmt.Errorf("failed to create parquet file reader: %w", err)
	}

	if !m.schemaSet {
		m.schema = fr.GetSchemaDefinition()
		m.writer = goparquet.NewFileWriter(m.output, goparquet.WithSchemaDefinition(m.schema))
		m.schemaSet = true
	}

	numRows := fr.NumRows()
	for i := 0; i < int(numRows); i++ {
		row, err := fr.NextRow()
		if err != nil {
			return fmt.Errorf("failed to read row %d: %w", i, err)
		}
		if err := m.writer.AddData(row); err != nil {
			return fmt.Errorf("failed to write row %d: %w", i, err)
		}
	}

	// Flush the row group at the end of each part
	if err := m.writer.FlushRowGroup(); err != nil {
		return fmt.Errorf("failed to flush row group: %w", err)
	}

	return nil
}

// Close finalizes the Parquet writing process.
func (m *ParquetConcatenatingWriter) Close() error {
	if err := m.writer.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}
	return m.output.Close()
}
