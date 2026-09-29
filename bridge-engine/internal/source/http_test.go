package source

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/producer"
	"github.com/raven-clown/ark/bridge-engine/internal/sink"
)

type memSink struct {
	mu   sync.Mutex
	got  []sink.Message
	fail bool
}

func (m *memSink) Write(_ context.Context, msg sink.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return errors.New("kafka is down")
	}
	m.got = append(m.got, msg)
	return nil
}

func (m *memSink) Close() error { return nil }

func testSource(t *testing.T) (*HTTP, *memSink) {
	t.Helper()
	t.Setenv("ARK_SOURCE_SHOP_TOKEN", "s3cret")
	out := &memSink{}
	h := NewHTTP(nil, 1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.newSink = func(string) (sink.Sink, *producer.Producer) { return out, nil }
	err := h.Set(context.Background(), []config.Source{{
		Name: "shop", Type: "http", Topic: "orders.raw", TokenEnv: "ARK_SOURCE_SHOP_TOKEN", Key: "order.id", MaxBodyBytes: 200,
		DataRules: &config.DataRules{Fields: []config.FieldRule{{Path: "order.id", Required: true, Type: "string"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return h, out
}

func post(h http.Handler, path, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("X-Correlation-ID", "abc")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHTTPSourceAcceptsAndKeysMessages(t *testing.T) {
	h, out := testSource(t)
	rec := post(h, "/ingest/shop", "s3cret", `{"order":{"id":"A1"},"amount":3}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if len(out.got) != 1 || string(out.got[0].Key) != "A1" || out.got[0].Headers[SourceHeader] != "shop" || out.got[0].Headers["X-Correlation-ID"] != "abc" {
		t.Fatalf("wrote %+v", out.got)
	}
}

func TestHTTPSourceRefusesWhatShouldNotReachKafka(t *testing.T) {
	h, out := testSource(t)
	cases := []struct {
		path, token, body string
		want              int
	}{
		{"/ingest/shop", "", `{"order":{"id":"A1"}}`, http.StatusUnauthorized},
		{"/ingest/shop", "wrong", `{"order":{"id":"A1"}}`, http.StatusUnauthorized},
		{"/ingest/nope", "s3cret", `{}`, http.StatusNotFound},
		{"/ingest/shop", "s3cret", `not json`, http.StatusBadRequest},
		{"/ingest/shop", "s3cret", `{"order":{"id":7}}`, http.StatusUnprocessableEntity},
		{"/ingest/shop", "s3cret", `{"pad":"` + strings.Repeat("x", 300) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, c := range cases {
		if rec := post(h, c.path, c.token, c.body); rec.Code != c.want {
			t.Errorf("%s token %q body %.30s: status %d, want %d (%s)", c.path, c.token, c.body, rec.Code, c.want, rec.Body)
		}
	}
	if len(out.got) != 0 {
		t.Fatalf("%d refused requests reached Kafka", len(out.got))
	}
	rec := post(h, "/ingest/shop", "s3cret", `{"order":{"id":7}}`)
	if !strings.Contains(rec.Body.String(), `"violations"`) {
		t.Fatalf("a data rule refusal should list the violations: %s", rec.Body)
	}
}

func TestHTTPSourceAsksToRetryWhenKafkaFails(t *testing.T) {
	h, out := testSource(t)
	out.fail = true
	rec := post(h, "/ingest/shop", "s3cret", `{"order":{"id":"A1"}}`)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("status %d, Retry-After %q", rec.Code, rec.Header().Get("Retry-After"))
	}
}

func TestHTTPSourceStopsServingRemovedSources(t *testing.T) {
	h, _ := testSource(t)
	if err := h.Set(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if rec := post(h, "/ingest/shop", "s3cret", `{"order":{"id":"A1"}}`); rec.Code != http.StatusNotFound {
		t.Fatalf("status %d after the source was removed", rec.Code)
	}
}
