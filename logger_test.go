package common_datalayer

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestLoggerFormattedMethods(t *testing.T) {
	buf := &bytes.Buffer{}
	l := &logger{log: zerolog.New(buf)}

	l.Debugf("debug %s", "msg")
	if got := buf.String(); !strings.Contains(got, "\"level\":\"debug\"") || !strings.Contains(got, "debug msg") {
		t.Fatalf("unexpected debugf output: %s", got)
	}
	buf.Reset()

	l.Infof("info %s", "msg")
	if got := buf.String(); !strings.Contains(got, "\"level\":\"info\"") || !strings.Contains(got, "info msg") {
		t.Fatalf("unexpected infof output: %s", got)
	}
	buf.Reset()

	l.Warnf("warn %s", "msg")
	if got := buf.String(); !strings.Contains(got, "\"level\":\"warn\"") || !strings.Contains(got, "warn msg") {
		t.Fatalf("unexpected warnf output: %s", got)
	}
	buf.Reset()

	l.Errorf("error %s", "msg")
	if got := buf.String(); !strings.Contains(got, "\"level\":\"error\"") || !strings.Contains(got, "error msg") {
		t.Fatalf("unexpected errorf output: %s", got)
	}
}
