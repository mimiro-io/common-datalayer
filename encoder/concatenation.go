package encoder

import (
	"bufio"
	"io"
)

// ConcatenatingWriter is an interface that defines the functions for concatenating data from many files into one file
// It ensures that the header / metadata is only written once and then appends all content from the files
// in accordance with the specific format, e.g. CSV, JSON, Parquet etc.
// Users of this interface should call Write for each file to be concatenated and Close when all files have been
// submitted for concatenated.
type ConcatenatingWriter interface {
	Write(reader io.ReadCloser) error
	Close() error
}

// GenericConcatenatingWriter implements the ConcatenatingWriter interface for simple concatenation.
// This can be used when all parts are headerless and can be concatenated without any special handling.
type GenericConcatenatingWriter struct {
	output         io.WriteCloser
	bufferedWriter *bufio.Writer
}

// NewGenericConcatenatingWriter creates a new GenericConcatenatingWriter.
func NewGenericConcatenatingWriter(output io.WriteCloser) *GenericConcatenatingWriter {
	return &GenericConcatenatingWriter{
		output:         output,
		bufferedWriter: bufio.NewWriter(output),
	}
}

// Write writes a part of a file to the target output by simple concatenation.
func (m *GenericConcatenatingWriter) Write(reader io.ReadCloser) (err error) {
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

// Close finalizes the writing process by flushing the buffer and closing the output.
func (m *GenericConcatenatingWriter) Close() error {
	if err := m.bufferedWriter.Flush(); err != nil {
		return err
	}
	return m.output.Close()
}
