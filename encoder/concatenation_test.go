package encoder

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
)

// TestGenericConcatenator tests the GenericConcatenator by writing 3 text files and concatenating them.
func TestGenericConcatenator(t *testing.T) {
	// Create temporary directory
	tempDir := os.TempDir()

	defer os.RemoveAll(tempDir)

	// Helper function to create a temp text file with given content
	createTempTextFile := func(filename string, content string) (string, error) {
		filePath := tempDir + "/" + filename
		err := os.WriteFile(filePath, []byte(content), 0644)
		if err != nil {
			return "", err
		}
		return filePath, nil
	}

	// Write 3 text files
	file1, err := createTempTextFile("file1.txt", "Line1\nLine2\n")
	if err != nil {
		t.Fatalf("Failed to create temp file1: %v", err)
	}

	file2, err := createTempTextFile("file2.txt", "Line3\nLine4\n")
	if err != nil {
		t.Fatalf("Failed to create temp file2: %v", err)
	}

	file3, err := createTempTextFile("file3.txt", "Line5\nLine6\n")
	if err != nil {
		t.Fatalf("Failed to create temp file3: %v", err)
	}

	// Create output file
	outputFile, err := os.Create(tempDir + "/combined.txt")
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	defer outputFile.Close()

	// Create GenericConcatenator
	genericConcatenator := NewGenericConcatenator(outputFile)

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
		if err := genericConcatenator.WritePart(ctx, reader); err != nil {
			t.Fatalf("WritePart failed for file %s: %v", file, err)
		}
	}

	// Finalize the writer
	if err := genericConcatenator.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	// Verify the combined output
	expectedOutput := "Line1\nLine2\nLine3\nLine4\nLine5\nLine6\n"
	outputBytes, err := ioutil.ReadFile(tempDir + "/combined.txt")
	if err != nil {
		t.Fatalf("Failed to read combined output file: %v", err)
	}
	output := string(outputBytes)

	if output != expectedOutput {
		t.Errorf("Unexpected combined output:\nExpected:\n%s\nGot:\n%s", expectedOutput, output)
	}
}
