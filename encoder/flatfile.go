package encoder

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	common_datalayer "github.com/mimiro-io/common-datalayer"
	"io"
	"strings"
)

func NewFlatFileItemFactory() ItemFactory {
	return &FlatFileItemFactory{}
}

type FlatFileItemFactory struct{}

func (c *FlatFileItemFactory) NewItem() common_datalayer.Item {
	return &FlatFileItem{data: make(map[string]any)}
}

type FlatFileItemWriter struct {
	writer    io.WriteCloser
	batchInfo *common_datalayer.BatchInfo
	config    *FlatFileConfig
}

type FlatFileConfig struct {
	Fields []FlatFileField `json:"fields"`
}

// use position of field in the list to order fieldOrder
type FlatFileField struct {
	Name      string `json:"name"`
	Length    int    `json:"length"`
	Ignore    bool   `json:"ignore"`
	NumberPad bool   `json:"number_pad"`
}

func NewFlatFileItemWriter(sourceConfig map[string]any, data io.WriteCloser, batchInfo *common_datalayer.BatchInfo) (*FlatFileItemWriter, error) {
	writer := &FlatFileItemWriter{writer: data, batchInfo: batchInfo}
	config, err := NewFlatFileWriteConfig(sourceConfig)
	if err != nil {
		return nil, err
	}
	writer.config = config
	return writer, nil
}

func NewFlatFileWriteConfig(sourceConfig map[string]any) (*FlatFileConfig, error) {
	var config FlatFileConfig
	jsonData, err := json.Marshal(sourceConfig)
	err = json.Unmarshal(jsonData, &config)
	if err != nil {
		return nil, err
	}
	if config.Fields == nil {
		return nil, errors.New("missing field config for flat file")
	}
	return &config, nil
}

func NewFlatFileReadConfig(sourceConfig map[string]any) (*FlatFileConfig, error) {
	var config FlatFileConfig
	jsonData, err := json.Marshal(sourceConfig)
	err = json.Unmarshal(jsonData, &config)
	if err != nil {
		return nil, err
	}
	if config.Fields == nil {
		return nil, errors.New("missing field config for flat file")
	}
	return &config, nil
}
func appendSpaces(value string, amount int) string {
	for i := 0; i < amount; i++ {
		value += " "
	}
	return value
}

func prependZeros(value string, amount int) string {
	prefix := ""
	for i := 0; i < amount; i++ {
		prefix += "0"
	}
	return prefix + value
}

func (c *FlatFileItemWriter) Close() error {
	return c.writer.Close()
}
func (c *FlatFileItemWriter) Write(item common_datalayer.Item) error {
	buf := new(bytes.Buffer)
	row := item.NativeItem().(map[string]any)
	line := make([]string, 0)
	var preppedValue string
	var fieldValue interface{}
	fieldsWithData := 0
	for i, field := range c.config.Fields {

		if _, ok := row[field.Name]; ok {
			fieldConfig := c.config.Fields[i]
			fieldValue = row[field.Name]
			fieldSize := fieldConfig.Length
			if fieldValue == nil {
				//	Need to add spaces according to field length config
				preppedValue = appendSpaces(preppedValue, fieldSize)
			} else {
				//	cast to string, then cut or append spaces to value according to config
				var valueLength int
				switch fieldValue.(type) {
				case float64:
					fieldValue = fmt.Sprintf("%f", fieldValue)
					fieldValue = strings.Replace(fieldValue.(string), ".", "", -1)
					valueLength = len(fieldValue.(string))
				default:
					fieldValue = fmt.Sprintf("%v", fieldValue)
				}
				valueLength = len(fieldValue.(string))
				if valueLength < fieldSize {
					diff := fieldSize - valueLength
					if fieldConfig.NumberPad {
						preppedValue = prependZeros(fieldValue.(string), diff)
					} else {
						preppedValue = appendSpaces(fieldValue.(string), diff)
					}
				} else if valueLength > fieldSize {
					preppedValue = fieldValue.(string)[:fieldSize]
				} else {
					preppedValue = fieldValue.(string)
				}
				fieldsWithData += 1
			}
		}
		line = append(line, preppedValue)
	}
	if fieldsWithData != 0 {
		buf.WriteString(fmt.Sprintf("%s\n", strings.Join(line, "")))
	}
	_, err := c.writer.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

type FlatFileItemIterator struct {
	reader  io.ReadCloser
	scanner *bufio.Scanner
	config  *FlatFileConfig
}

func NewFlatFileItemIterator(sourceConfig map[string]any, data io.ReadCloser) (*FlatFileItemIterator, error) {
	scanner := bufio.NewScanner(data)
	reader := &FlatFileItemIterator{reader: data, scanner: scanner}
	config, err := NewFlatFileReadConfig(sourceConfig)
	if err != nil {
		return nil, err
	}
	reader.config = config
	return reader, nil
}

func (c *FlatFileItemIterator) Close() error {
	return c.reader.Close()
}

func (c *FlatFileItemIterator) Read() (common_datalayer.Item, error) {
	var entityProps = make(map[string]interface{})
	for c.scanner.Scan() {
		line := c.scanner.Text()

		var step = 0
		for _, field := range c.config.Fields {
			if field.Ignore {
				step += field.Length
				continue
			} else {
				entityProps[field.Name] = strings.TrimSpace(line[step : step+field.Length])
				step += field.Length
			}
		}
		return &FlatFileItem{data: entityProps}, nil
	}

	return nil, nil
}

type FlatFileItem struct {
	data map[string]any
}

func (item *FlatFileItem) GetValue(key string) any {
	return item.data[key]
}

func (item *FlatFileItem) SetValue(key string, value any) {
	item.data[key] = value
}

func (item *FlatFileItem) GetPropertyNames() []string {
	keys := make([]string, 0, len(item.data))
	for k := range item.data {
		keys = append(keys, k)
	}
	return keys
}

func (item *FlatFileItem) NativeItem() any {
	return item.data
}
