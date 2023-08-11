package core

import (
	"log/slog"
	"os"
)

type logger struct {
	log *slog.Logger
}

func (l *logger) With(name string, value string) Logger {
	return &logger{l.log.With(name, value)}
}

func (l *logger) Warn(message string, args ...any) {
	l.log.Warn(message, args...)
}

func (l *logger) Error(message string, args ...any) {
	l.log.Error(message, args...)
}

func (l *logger) Info(message string, args ...any) {
	l.log.Info(message, args...)
}

func (l *logger) Debug(message string, args ...any) {
	l.log.Debug(message, args...)
}

func NewLogger() Logger {
	outputHandler := slog.NewJSONHandler(os.Stdout, nil)
	log := slog.New(outputHandler)
	//log = slog.New(slog.NewTextHandler(os.Stdout, nil))
	return &logger{log}
}

type Logger interface {
	Error(message string, args ...any)
	Info(message string, args ...any)
	Debug(message string, args ...any)
	Warn(message string, args ...any)
	With(name string, value string) Logger
}