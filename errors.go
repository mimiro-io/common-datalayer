package common_datalayer

import (
	"fmt"

	"github.com/labstack/echo/v4"
)

type LayerErrorType int

const (
	LayerErrorBadParameter LayerErrorType = iota
	LayerErrorInternal
	LayerNotSupported
)

type LayerError interface {
	error
	toHTTPError() *echo.HTTPError
	Underlying() error
}

type layerError struct {
	err     error
	errType LayerErrorType
}

func (l layerError) Underlying() error {
	return l.err
}

func (l layerError) toHTTPError() *echo.HTTPError {
	// TODO: map LayerErrorType to HTTP status code and message
	return echo.NewHTTPError(500, l.err.Error())
}

func (l layerError) Error() string {
	return l.err.Error()
}

// TODO: add more error constructors without errType parameter. e.g. ErrBadParameter(err error)

func Err(err error, errType LayerErrorType) LayerError {
	if err == nil {
		return nil
	}
	return &layerError{err, errType}
}

func Errorf(errType LayerErrorType, format string, args ...any) LayerError {
	return &layerError{err: fmt.Errorf(format, args...), errType: errType}
}
