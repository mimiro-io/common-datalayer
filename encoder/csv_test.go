package encoder

import (
	"context"
	cdl "github.com/mimiro-io/common-datalayer"
	"io/ioutil"
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

func TestNewCSVConcatenatingWriter(t *testing.T) {
	// Create temporary directory
	tempDir := os.TempDir()
	defer os.RemoveAll(tempDir)

	// Helper function to create a temp CSV file with given content
	createTempCSVFile := func(filename string, content string) (string, error) {
		filePath := tempDir + "/" + filename
		err := os.WriteFile(filePath, []byte(content), 0644)
		if err != nil {
			return "", err
		}
		return filePath, nil
	}

	// Write 3 CSV files
	file1, err := createTempCSVFile("file1.csv", "header1,header2\nrow1col1,row1col2\n")
	if err != nil {
		t.Fatalf("Failed to create temp file1: %v", err)
	}

	file2, err := createTempCSVFile("file2.csv", "header1,header2\nrow2col1,row2col2\n")
	if err != nil {
		t.Fatalf("Failed to create temp file2: %v", err)
	}

	file3, err := createTempCSVFile("file3.csv", "header1,header2\nrow3col1,row3col2\n")
	if err != nil {
		t.Fatalf("Failed to create temp file3: %v", err)
	}

	// Create output file
	outputFile, err := os.Create(tempDir + "/combined.csv")
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	defer outputFile.Close()

	// Create CSVConcatenatingWriter
	csvWriter := NewCSVConcatenatingWriter(outputFile, true)

	// List of files to concatenate
	files := []string{file1, file2, file3}

	// Context
	ctx := context.Background()

	// Concatenate files
	for _, file := range files {
		reader, err := os.Open(file)
		if err != nil {
			t.Fatalf("Failed to open file %s: %v", file, err)
		}
		if err := csvWriter.WritePart(ctx, reader); err != nil {
			t.Fatalf("WritePart failed for file %s: %v", file, err)
		}
	}

	// Finalize the writer
	if err := csvWriter.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	// Verify the combined output
	expectedOutput := "header1,header2\nrow1col1,row1col2\nrow2col1,row2col2\nrow3col1,row3col2\n"
	outputBytes, err := ioutil.ReadFile(tempDir + "/combined.csv")
	if err != nil {
		t.Fatalf("Failed to read combined output file: %v", err)
	}
	output := string(outputBytes)

	if output != expectedOutput {
		t.Errorf("Unexpected combined output:\nExpected:\n%s\nGot:\n%s", expectedOutput, output)
	}
}
