package encoder

import (
	"bufio"
	"bytes"
	"encoding/json"
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
	writer     io.WriteCloser
	batchInfo  *common_datalayer.BatchInfo
	fields     []FlatFileField
	fieldOrder []string
}
type SourceConfig struct {
	Fields     []FlatFileField `json:"fields"`
	FieldOrder []string        `json:"fieldOrder"`
}

// use position of field in the list to order fieldOrder
type FlatFileField struct {
	Name   string `json:"name"`
	Length int    `json:"length"`
}

func NewFlatFileItemWriter(sourceConfig map[string]any, data io.WriteCloser, batchInfo *common_datalayer.BatchInfo) (*FlatFileItemWriter, error) {
	writer := &FlatFileItemWriter{writer: data, batchInfo: batchInfo}
	// do this in function or here?
	itemWriter, err2 := NewFlatFileConfig(sourceConfig, writer)
	if err2 != nil {
		return itemWriter, err2
	}

	//fields NewFlatFileField()
	/*for k, v := range fields {
		field := v.(map[string]interface{})
		substring := field["substring"].([]interface{})
		sub := make([][]int, 0)
		for _, s := range substring {
			sub = append(sub, s.([]int))
		}
		fields[k] = FlatFileField{
			Name:   field["name"].(string),
			Length: field["length"].(int),
			//Dont talk about data type in the encoding/decoding. Lift it up to the mapper level
		}
	}*/

	return writer, nil
}

func NewFlatFileConfig(sourceConfig map[string]any, writer *FlatFileItemWriter) (*FlatFileItemWriter, error) {
	var config SourceConfig
	jsonData, err := json.Marshal(sourceConfig)
	err = json.Unmarshal(jsonData, &config)
	if err != nil {
		fmt.Println("Error:", err)
		return nil, err
	}
	if config.Fields == nil {
		return nil, fmt.Errorf("missing field config for flat file")
	} else {
		writer.fields = config.Fields
	}
	if config.FieldOrder == nil {
		return nil, fmt.Errorf("missing fieldOrder config for flat file write operation")
	} else {
		writer.fieldOrder = config.FieldOrder
	}
	return nil, nil
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
	for i, fieldName := range c.fieldOrder {

		if _, ok := row[fieldName]; ok {

			fieldConfig := c.fields[i]
			fieldValue = row[fieldName]
			fieldSize := fieldConfig.Length
			if fieldValue == nil {
				//	Need to add spaces according to field length config
				preppedValue = appendSpaces(preppedValue, fieldSize)
			} else {
				//	cast to string, then cut or append spaces to value according to substring config
				var valueLength int
				switch fieldValue.(type) {
				case float64:
					fieldValue = fmt.Sprintf("%f", fieldValue)
				default:
					fieldValue = fmt.Sprintf("%v", fieldValue)
				}
				valueLength = len(fieldValue.(string))
				// do this in mapper?
				if valueLength < fieldSize {
					diff := fieldSize - valueLength
					preppedValue = appendSpaces(fieldValue.(string), diff)
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
	data       io.ReadCloser
	reader     io.ReadCloser
	scanner    *bufio.Scanner
	fields     map[string]FlatFileField
	fieldOrder []string
}

func NewFlatFileItemIterator(sourceConfig map[string]any, data io.ReadCloser) (*FlatFileItemIterator, error) {
	// TODO: fix me
	reader := &FlatFileItemIterator{data: data, reader: data}
	fieldNames, ok := sourceConfig["fields"].(interface{})
	if ok {
		reader.fields = fieldNames.(map[string]FlatFileField)
	}
	if fieldNames == nil {
		return nil, fmt.Errorf("missing field config for flat file")
	}
	fieldOrder, ok := sourceConfig["fieldOrder"].([]string)
	if ok {
		reader.fieldOrder = fieldOrder
	}
	return reader, nil
}

func (c *FlatFileItemIterator) Close() error {
	return c.data.Close()
}

/*
	func (c *FlatFileItemIterator) Read(p []byte) (common_datalayer.Item, error) {
		// TODO: fix me
		buf := make([]byte, 0, len(p))
		c.scanner = bufio.NewScanner(c.reader)

		// append one entity per line, comma separated
		for c.scanner.Scan() {

			line := c.scanner.Text()
			//d.logger.Debugf("Got line : '%s'", line)
			d.ParseLine(line, d.backend.FlatFileConfig)
			if err != nil {
				d.logger.Errorf("Failed to parse line: '%s'", line)
				if d.backend.FlatFileConfig.ContinueOnParseError {
					continue
				} else {
					return
				}
			}

			var entityBytes []byte
			entityBytes, err = toEntityBytes(entityProps, d.backend)
			if err != nil {
				return
			}
			buf = append(buf, append([]byte(","), entityBytes...)...)
			if n, err, done = d.flush(p, buf); done {
				return
			}
		}
		var token string
		if d.fullSync {
			token = ""
		} else {
			token = d.since
		}
		// Add continuation token
		entity := map[string]interface{}{
			"id":    "@continuation",
			"token": token,
		}
		sinceBytes, err := json.Marshal(entity)
		buf = append(buf, append([]byte(","), sinceBytes...)...)

		// close json array
		if !d.closed {
			buf = append(buf, []byte("]")...)
			d.closed = true
			if n, err, done = d.flush(p, buf); done {
				return
			}
		}
		n = copy(p, buf)
		return n, io.EOF
		return &FlatFileItem{data: entityProps}, nil
	}
*/
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
