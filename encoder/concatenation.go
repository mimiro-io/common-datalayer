package encoder

import (
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
