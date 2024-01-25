package common_datalayer

import (
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
)

/******************************************************************************/

type Metrics interface {
	Incr(s string, tags []string, i int) LayerError
	Timing(s string, timed time.Duration, tags []string, i int) LayerError
	Gauge(s string, f float64, tags []string, i int) LayerError
}

type Logger interface {
	Error(message string, args ...any)
	Info(message string, args ...any)
	Debug(message string, args ...any)
	Warn(message string, args ...any)
	With(name string, value string) Logger
}

/******************************************************************************/

type StatsdMetrics struct {
	client statsd.ClientInterface
}

func (sm StatsdMetrics) Incr(name string, tags []string, rate int) LayerError {
	return Err(sm.client.Incr(name, tags, float64(rate)), LayerErrorInternal)
}

func (sm StatsdMetrics) Timing(name string, value time.Duration, tags []string, rate int) LayerError {
	return Err(sm.client.Timing(name, value, tags, float64(rate)), LayerErrorInternal)
}

func (sm StatsdMetrics) Gauge(name string, value float64, tags []string, rate int) LayerError {
	return Err(sm.client.Gauge(name, value, tags, float64(rate)), LayerErrorInternal)
}

func newMetrics(conf *Config) (Metrics, error) {
	var client statsd.ClientInterface
	if conf.LayerServiceConfig.StatsdEnabled {
		c, err := statsd.New(conf.LayerServiceConfig.StatsdAgentAddress,
			statsd.WithNamespace(conf.LayerServiceConfig.ServiceName),
			statsd.WithTags([]string{"application:" + conf.LayerServiceConfig.ServiceName}))
		if err != nil {
			return nil, err
		}

		client = c
	} else {
		client = &statsd.NoOpClient{}
	}

	return &StatsdMetrics{client: client}, nil
}

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

func newLogger(serviceName string, format string) Logger {
	opts := &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
		// traverse call stack to find the first non-log/slog function and use that as the source
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == "source" {
				pcs := make([]uintptr, 10)
				runtime.Callers(2, pcs)
				fs := runtime.CallersFrames(pcs)

				f, more := fs.Next()
				for more {
					if strings.HasPrefix(f.Function, "log/slog") || strings.Contains(f.Function, "(*logger).") {
						f, more = fs.Next()
						continue
					}
					a.Value = slog.AnyValue(&slog.Source{
						Function: f.Function,
						File:     f.File,
						Line:     f.Line,
					})
					break
				}
			}
			return a
		},
	}
	var outputHandler slog.Handler = slog.NewJSONHandler(os.Stdout, opts)
	if format == "text" {
		outputHandler = slog.NewTextHandler(os.Stdout, opts)
	}
	log := slog.New(outputHandler).With(
		"go.version", runtime.Version(),
		"service", serviceName)
	// log = slog.New(slog.NewTextHandler(os.Stdout, nil))
	return &logger{log}
}
