package encoder

import (
	common_datalayer "github.com/mimiro-io/common-datalayer"
	"os"
	"testing"
)

//comment because it's unfinished for now.
/*
func TestFlatFileRead(t *testing.T) {
	// open file
	filename := "./testdata/data.csv"
	file, err := os.Open(filename)
	if err != nil {
		t.Error(err)
	}
	fieldOrder := []string{"foo", "bar"}
	sourceConfig := make(map[string]any)
	sourceConfig["fieldOrder"] = fieldOrder
	sourceConfig["fields"] = &FlatFileField{Substring: [][]int{{0, 1}, {1, 2}}, Type: "string"}
	reader, err := NewFlatFileItemIterator(sourceConfig, file)

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
*/
func TestFlatFileWrite(t *testing.T) {
	filename := "./testdata/data_write.txt"
	file, err := os.Create(filename)
	if err != nil {
		t.Error(err)
	}
	// defer delete file
	defer os.Remove(filename)

	itemFactory := NewFlatFileItemFactory()
	fieldOrder := []string{"id", "name", "age", "worksfor"}
	fields := []map[string]interface{}{{"name": "id", "length": 1, "type": "string"}, {"name": "name", "length": 6, "type": "string"}, {"name": "age", "length": 2, "type": "string"}, {"name": "worksfor", "length": 10, "type": "string"}}
	sourceConfig := make(map[string]any)
	sourceConfig["fieldOrder"] = fieldOrder
	sourceConfig["fields"] = fields
	batchInfo := &common_datalayer.BatchInfo{SyncId: "1", IsLastBatch: false, IsStartBatch: true}
	writer, err := NewFlatFileItemWriter(sourceConfig, file, batchInfo)
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
	item.SetValue("age", 89.000)
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
	/*
		reader, err := NewFlatFileItemIterator(sourceConfig, file)
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
		}*/
}
