package common_datalayer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	egdm "github.com/mimiro-io/entity-graph-data-model"
)

// ctxService serves a single dataset that records the context its writers get.
type ctxService struct{ ds *ctxDataset }

func (s *ctxService) Stop(context.Context) error             { return nil }
func (s *ctxService) UpdateConfiguration(*Config) LayerError { return nil }
func (s *ctxService) Dataset(string) (Dataset, LayerError)   { return s.ds, nil }
func (s *ctxService) DatasetDescriptions() []*DatasetDescription {
	return nil
}

type ctxDataset struct{ ctxs chan context.Context }

func (d *ctxDataset) MetaData() map[string]any { return nil }
func (d *ctxDataset) Name() string             { return "test" }
func (d *ctxDataset) FullSync(ctx context.Context, _ BatchInfo) (DatasetWriter, LayerError) {
	d.ctxs <- ctx
	return nopWriter{}, nil
}
func (d *ctxDataset) Incremental(ctx context.Context) (DatasetWriter, LayerError) {
	d.ctxs <- ctx
	return nopWriter{}, nil
}
func (d *ctxDataset) Changes(string, int, bool) (EntityIterator, LayerError) { return nil, nil }
func (d *ctxDataset) Entities(string, int) (EntityIterator, LayerError)      { return nil, nil }

type nopWriter struct{}

func (nopWriter) Write(*egdm.Entity) LayerError { return nil }
func (nopWriter) Close() LayerError             { return nil }

// A writer must learn that its request ended even when the payload fails to
// parse, because the handler then returns without calling Close.
func TestPostEntitiesCancelsWriterContext(t *testing.T) {
	for name, header := range map[string]http.Header{
		"incremental": {},
		"full sync":   {"Universal-Data-Api-Full-Sync-Id": {"1"}, "Universal-Data-Api-Full-Sync-Start": {"true"}},
	} {
		t.Run(name, func(t *testing.T) {
			conf := &Config{LayerServiceConfig: &LayerServiceConfig{}}
			metrics, err := newMetrics(conf)
			if err != nil {
				t.Fatal(err)
			}
			ds := &ctxDataset{ctxs: make(chan context.Context, 1)}
			ws, err := newDataLayerWebService(conf, NewLogger("test", "json", "error"), metrics, &ctxService{ds})
			if err != nil {
				t.Fatal(err)
			}
			srv := httptest.NewServer(ws.e)
			defer srv.Close()

			req, err := http.NewRequest(http.MethodPost, srv.URL+"/datasets/test/entities", strings.NewReader("[{"))
			if err != nil {
				t.Fatal(err)
			}
			req.Header = header
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected status 400, got %d", resp.StatusCode)
			}

			ctx := <-ds.ctxs
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("writer context was not canceled after the request ended")
			}
		})
	}
}
