package encoder

import (
	"bufio"
	"context"
	"io"
)

// ConcatenatingWriter is an interface that defines the functions for concatenating data from many files into one file
// It ensures that the header / metadata is only written once and then appends all content from the files
// in accordance with the specific format, e.g. CSV, JSON, Parquet etc.
// Users of this interface should call WritePart for each file to be concatenated and Finalize when all files have been
// submitted for concatenated.
type ConcatenatingWriter interface {
	WritePart(ctx context.Context, reader io.ReadCloser) error
	Finalize() error
}

// GenericConcatenator implements the Concatenator interface for simple concatenation.
// This can be used when all parts are headerless and can be concatenated without any special handling.
type GenericConcatenator struct {
	output         io.WriteCloser
	bufferedWriter *bufio.Writer
}

// NewGenericConcatenator creates a new GenericConcatenator.
func NewGenericConcatenator(output io.WriteCloser) *GenericConcatenator {
	return &GenericConcatenator{
		output:         output,
		bufferedWriter: bufio.NewWriter(output),
	}
}

// WritePart writes a part of a file to the target output by simple concatenation.
func (m *GenericConcatenator) WritePart(ctx context.Context, reader io.ReadCloser) (err error) {
	defer func() {
		closeErr := reader.Close()
		if err == nil {
			err = closeErr
		}
	}()

	_, err = io.Copy(m.bufferedWriter, reader)
	if err != nil {
		return err
	}

	return nil
}

// Finalize finalizes the writing process by flushing the buffer and closing the output.
func (m *GenericConcatenator) Finalize() error {
	if err := m.bufferedWriter.Flush(); err != nil {
		return err
	}
	return m.output.Close()
}
