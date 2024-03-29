package encoder

import (
	cdl "github.com/mimiro-io/common-datalayer"
	"os"
	"testing"
)

func TestCSVRead(t *testing.T) {
	// open file
	filename := "./testdata/data.csv"
	file, err := os.Open(filename)
	if err != nil {
		t.Error(err)
	}
	sourceConfig := make(map[string]any)
	sourceConfig["separator"] = ","
	sourceConfig["encoding"] = "csv"
	sourceConfig["columns"] = []string{"id", "name", "age", "worksfor"}
	sourceConfig["has_header"] = true

	logger := cdl.NewLogger("test", "text", "debug")
	reader, err := NewCSVItemIterator(sourceConfig, logger, file)

	item, err := reader.Read()
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

	if item == nil {
		t.Error("Expected item")
	}

	if item.GetValue("name") != "Jim" {
		t.Error("Expected Jim")
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

func TestCSVWrite(t *testing.T) {
	filename := "./testdata/data_write.csv"
	file, err := os.Create(filename)
	if err != nil {
		t.Error(err)
	}
	// defer delete file
	defer os.Remove(filename)

	itemFactory := NewCSVItemFactory()
	sourceConfig := make(map[string]any)
	sourceConfig["separator"] = ","
	sourceConfig["encoding"] = "csv"
	sourceConfig["columns"] = []string{"id", "name", "age", "worksfor"}
	sourceConfig["has_header"] = true
	batchInfo := &cdl.BatchInfo{SyncId: "1", IsLastBatch: false, IsStartBatch: true}
	logger := cdl.NewLogger("test", "text", "debug")
	writer, err := NewCSVItemWriter(sourceConfig, logger, file, batchInfo)
	if err != nil {
		t.Error(err)
	}
	// read json-file to use as test?

	item := itemFactory.NewItem()
	item.SetValue("id", "3")
	item.SetValue("name", "John")
	item.SetValue("age", 25.000)
	item.SetValue("worksfor", "Google")
	err = writer.Write(item)
	if err != nil {
		t.Error(err)
	}

	item = itemFactory.NewItem()
	item.SetValue("id", "4")
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

	reader, err := NewCSVItemIterator(sourceConfig, logger, file)
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

func TestTABDelimiter(t *testing.T) {
	filename := "./testdata/data_tab.csv"
	file, err := os.Open(filename)
	if err != nil {
		t.Error(err)
	}
	sourceConfig := make(map[string]any)
	sourceConfig["separator"] = "\t"
	sourceConfig["encoding"] = "csv"
	sourceConfig["columns"] = []string{"id", "name", "age", "worksfor"}
	sourceConfig["has_header"] = true
	logger := cdl.NewLogger("test", "text", "debug")
	reader, err := NewCSVItemIterator(sourceConfig, logger, file)

	item, err := reader.Read()
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

	if item != nil {
		t.Error("Expected no item")
	}

	err = reader.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestTABDelimiterWriting(t *testing.T) {
	filename := "./testdata/data_tab1.csv"
	file, err := os.Create(filename)
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(filename)

	sourceConfig := make(map[string]any)
	sourceConfig["separator"] = "\t"
	sourceConfig["encoding"] = "csv"
	sourceConfig["columns"] = []string{"id", "name", "age", "worksfor"}
	sourceConfig["has_header"] = true

	// create item and writer, then write the item
	itemFactory := NewCSVItemFactory()
	logger := cdl.NewLogger("test", "text", "debug")
	writer, err := NewCSVItemWriter(sourceConfig, logger, file, nil)
	if err != nil {
		t.Error(err)
	}

	item := itemFactory.NewItem()
	item.SetValue("id", "3")
	item.SetValue("name", "John")
	item.SetValue("age", 25)
	item.SetValue("worksfor", "Google")
	err = writer.Write(item)
	if err != nil {
		t.Error(err)
	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	file, err = os.Open(filename)
	if err != nil {
		t.Error(err)
	}
	reader, err := NewCSVItemIterator(sourceConfig, logger, file)

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

	if item != nil {
		t.Error("Expected no item")
	}

	err = reader.Close()
	if err != nil {
		t.Error(err)
	}
}
