package common_datalayer

import (
	"log/slog"
	"os"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
)

type CoreService struct {
	Config  *Config
	Logger  Logger
	Metrics Metrics
}

type Metrics interface {
	Incr(s string, tags []string, i int) error
	Timing(s string, timed time.Duration, tags []string, i int) error
	Gauge(s string, f float64, tags []string, i int) error
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

func (sm StatsdMetrics) Incr(name string, tags []string, rate int) error {
	return sm.client.Incr(name, tags, float64(rate))
}

func (sm StatsdMetrics) Timing(name string, value time.Duration, tags []string, rate int) error {
	return sm.client.Timing(name, value, tags, float64(rate))
}

func (sm StatsdMetrics) Gauge(name string, value float64, tags []string, rate int) error {
	return sm.client.Gauge(name, value, tags, float64(rate))
}

func newMetrics(conf *Config) (Metrics, error) {
	var clientInt statsd.ClientInterface
	if conf.SystemConfig.StatsdEnabled() {
		client, err := statsd.New(conf.SystemConfig.StatsdAgentAddress())
		if err != nil {
			return nil, err
		}
		client, err = statsd.CloneWithExtraOptions(client, statsd.WithTags([]string{"application:" + conf.SystemConfig.ServiceName()}))
		if err != nil {
			return nil, err
		}
		clientInt = client
	} else {
		clientInt = &statsd.NoOpClient{}
	}

	return &StatsdMetrics{client: clientInt}, nil
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

func newLogger() Logger {
	outputHandler := slog.NewJSONHandler(os.Stdout, nil)
	log := slog.New(outputHandler)
	//log = slog.New(slog.NewTextHandler(os.Stdout, nil))
	return &logger{log}
}