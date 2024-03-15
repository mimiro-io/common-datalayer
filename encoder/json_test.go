package encoder

import (
	cdl "github.com/mimiro-io/common-datalayer"
	"os"
	"testing"
)

func TestJsonRead(t *testing.T) {
	// open file
	filename := "./testdata/data.json"
	file, err := os.Open(filename)
	if err != nil {
		t.Error(err)
	}
	logger := cdl.NewLogger("test", "text", "debug")
	reader, err := NewJsonItemIterator(make(map[string]any), logger, file)

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

func TestJsonWrite(t *testing.T) {
	filename := "./testdata/data_write.json"
	file, err := os.Create(filename)
	if err != nil {
		t.Error(err)
	}
	// defer delete file
	defer os.Remove(filename)

	itemFactory := NewJsonItemFactory()

	logger := cdl.NewLogger("test", "text", "debug")
	writer, err := NewJsonItemWriter(make(map[string]any), logger, file, nil)
	if err != nil {
		t.Error(err)
	}

	item := itemFactory.NewItem()
	item.SetValue("id", "3")
	item.SetValue("name", "John")
	err = writer.Write(item)
	if err != nil {
		t.Error(err)
	}

	item = itemFactory.NewItem()
	item.SetValue("id", "4")
	item.SetValue("name", "Jane")
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

	reader, err := NewJsonItemIterator(make(map[string]any), logger, file)
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
