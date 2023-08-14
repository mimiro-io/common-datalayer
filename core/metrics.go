package core

import (
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
)

type Metrics interface {
	Incr(s string, tags []string, i int) error
	Timing(s string, timed time.Duration, tags []string, i int) error
	Gauge(s string, f float64, tags []string, i int) error
}

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

func NewMetrics(conf *Config) Metrics {
	// TODO: wire up proper client from config
	return &StatsdMetrics{&statsd.NoOpClient{}}
}