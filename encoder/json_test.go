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

func TestNewJSONConcatenatingWriter(t *testing.T) {
	// Create temporary directory
	tempDir := os.TempDir()

	defer os.RemoveAll(tempDir)

	// Helper function to create a temp JSON file with given content
	createTempJSONFile := func(filename string, content string) (string, error) {
		filePath := tempDir + "/" + filename
		err := os.WriteFile(filePath, []byte(content), 0644)
		if err != nil {
			return "", err
		}
		return filePath, nil
	}

	// Write 3 JSON files
	file1, err := createTempJSONFile("file1.json", `[{"id":1,"name":"Object1"}]`)
	if err != nil {
		t.Fatalf("Failed to create temp file1: %v", err)
	}

	file2, err := createTempJSONFile("file2.json", `[{"id":2,"name":"Object2"}]`)
	if err != nil {
		t.Fatalf("Failed to create temp file2: %v", err)
	}

	file3, err := createTempJSONFile("file3.json", `[{"id":3,"name":"Object3"}]`)
	if err != nil {
		t.Fatalf("Failed to create temp file3: %v", err)
	}

	// Create output file
	outputFile, err := os.Create(tempDir + "/combined.json")
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	defer outputFile.Close()

	// Create JSONConcatenatingWriter
	jsonWriter := NewJSONConcatenatingWriter(outputFile)

	// List of files to concatenate
	files := []string{file1, file2, file3}

	// Concatenate files
	for _, file := range files {
		reader, err := os.Open(file)
		if err != nil {
			t.Fatalf("Failed to open file %s: %v", file, err)
		}
		if err := jsonWriter.Write(reader); err != nil {
			t.Fatalf("Write failed for file %s: %v", file, err)
		}
	}

	// Close the writer
	if err := jsonWriter.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify the combined output
	expectedOutput := `[{"id":1,"name":"Object1"},{"id":2,"name":"Object2"},{"id":3,"name":"Object3"}]`
	outputBytes, err := os.ReadFile(tempDir + "/combined.json")
	if err != nil {
		t.Fatalf("Failed to read combined output file: %v", err)
	}
	output := string(outputBytes)

	if output != expectedOutput {
		t.Errorf("Unexpected combined output:\nExpected:\n%s\nGot:\n%s", expectedOutput, output)
	}
}
