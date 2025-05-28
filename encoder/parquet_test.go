package encoder

import (
	cdl "github.com/mimiro-io/common-datalayer"
	"os"
	"testing"
)

func TestParquetRead(t *testing.T) {
	// open file
	filename := "./testdata/example.parquet"
	file, err := os.Open(filename)
	if err != nil {
		t.Error(err)
	}
	var flush int64 = 2097152
	sourceConfig := make(map[string]any)
	sourceConfig["encoding"] = "parquet"
	sourceConfig["schema"] = []map[string]interface{}{{"name": "id", "type": "int64", "required": true}, {"name": "name", "type": "string", "required": false}, {"name": "age", "type": "int64", "required": false}, {"name": "worksfor", "type": "string", "required": false}}
	sourceConfig["parquet_name"] = "example"

	sourceConfig["flush_threshold"] = flush
	reader, err := NewParquetItemIterator(sourceConfig, file)

	item, err := reader.Read()
	if err != nil {
		t.Error(err)
	}

	if item == nil {
		t.Error("Expected item")
	}
	if item.GetValue("name") != "John Smith" {
		t.Error("Expected John Smith")
	}

	item, err = reader.Read()
	if err != nil {
		t.Error(err)
	}

	if item == nil {
		t.Error("Expected item")
	}

	if item.GetValue("name") != "Jane Doe" {
		t.Error("Expected Jane Doe")
	}

	item, err = reader.Read()
	if err != nil {
		t.Error(err)
	}

	if item != nil {
		t.Error("Expected no item")
	}

	err = reader.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestParquetWrite(t *testing.T) {
	filename := "./testdata/data_write.parquet"
	file, err := os.Create(filename)
	if err != nil {
		t.Error(err)
	}
	// defer delete file
	defer os.Remove(filename)

	itemFactory := NewParquetItemFactory()
	var flush int64 = 2097152
	sourceConfig := make(map[string]any)
	sourceConfig["encoding"] = "parquet"
	sourceConfig["schema"] = `message example { required int64 id; optional binary name (STRING); optional int64 age; optional binary worksfor (STRING); }`
	sourceConfig["flush_threshold"] = flush
	batchInfo := &cdl.BatchInfo{SyncId: "1", IsLastBatch: false, IsStartBatch: true}
	writer, err := NewParquetItemWriter(sourceConfig, file, batchInfo)
	if err != nil {
		t.Error(err)
	}
	// read json-file to use as test?

	item := itemFactory.NewItem()
	item.SetValue("id", 3)
	item.SetValue("name", "John")
	item.SetValue("age", 25.000)
	item.SetValue("worksfor", "Google")
	err = writer.Write(item)
	if err != nil {
		t.Error(err)
	}

	item = itemFactory.NewItem()
	item.SetValue("id", 4)
	item.SetValue("name", "Jane")
	item.SetValue("age", 89)
	err = writer.Write(item)

	if err != nil {
		t.Error(err)
	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	// read back these items
	file, err = os.Open(filename)
	if err != nil {
		t.Error(err)
	}
	sourceConfig["schema"] = `message example { required int64 id; optional binary name (STRING); optional int64 age; optional binary worksfor (STRING); }`
	reader, err := NewParquetItemIterator(sourceConfig, file)
	if err != nil {
		t.Error(err)
	}

	item, err = reader.Read()
	if err != nil {

		t.Error(err)
	}

	if item == nil {
		t.Error("Expected item")
	}

	if item.GetValue("name") != "John" {
		t.Error("Expected John")
	}

	item, err = reader.Read()
	if err != nil {
		t.Error(err)
	}

	if item == nil {
		t.Error("Expected item")
	}

	if item.GetValue("name") != "Jane" {
		t.Error("Expected Jane")
	}

	item, err = reader.Read()
	if err != nil {
		t.Error(err)
	}

	if item != nil {
		t.Error("Expected no item")
	}

	err = reader.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestParquetConcatenatingWriter(t *testing.T) {
	// open file
	filename := "./testdata/example.parquet"
	file, err := os.Open(filename)
	if err != nil {
		t.Error(err)
	}

	// create concatenating writer
	outputFile, err := os.Create("./testdata/combined.parquet")
	if err != nil {
		t.Error(err)
	}
	defer os.Remove("./testdata/combined.parquet")

	concatenatingWriter := NewParquetConcatenatingWriter(outputFile)

	// write to file
	err = concatenatingWriter.Write(file)
	if err != nil {
		t.Error(err)
	}

	// open file a second time and write 2nd part
	file, err = os.Open(filename)
	if err != nil {
		t.Error(err)
	}

	err = concatenatingWriter.Write(file)
	if err != nil {
		t.Error(err)
	}

	// close writer
	err = concatenatingWriter.Close()

	// read back the combined file
	file, err = os.Open("./testdata/combined.parquet")
	if err != nil {
		t.Error(err)
	}

	sourceConfig := make(map[string]any)
	sourceConfig["encoding"] = "parquet"
	sourceConfig["schema"] = `message example { required int64 id; optional binary name (STRING); optional int64 age; optional binary worksfor (STRING); }`
	sourceConfig["flush_threshold"] = 2097152
	reader, err := NewParquetItemIterator(sourceConfig, file)

	item, err := reader.Read()
	if err != nil {
		t.Error(err)
	}

	if item == nil {
		t.Error("Expected item")
	}

	if item.GetValue("name") != "John Smith" {
		t.Error("Expected John Smith")
	}

	item, err = reader.Read()
	if err != nil {
		t.Error(err)
	}

	if item == nil {
		t.Error("Expected item")
	}

	if item.GetValue("name") != "Jane Doe" {
		t.Error("Expected Jane Doe")
	}

	item, err = reader.Read()
	if err != nil {
		t.Error(err)
	}

	if item == nil {
		t.Error("Expected item")
	}

	if item.GetValue("name") != "John Smith" {
		t.Error("Expected John Smith")
	}

	item, err = reader.Read()
	if err != nil {
		t.Error(err)
	}

	if item == nil {
		t.Error("Expected item")
	}

	if item.GetValue("name") != "Jane Doe" {
		t.Error("Expected Jane Doe")
	}

	item, err = reader.Read()
	if err != nil {
		t.Error(err)
	}

	if item != nil {
		t.Error("Expected no item")
	}
}
