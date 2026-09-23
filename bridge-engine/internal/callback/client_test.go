package callback

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPostSendsCorrelationIDAndBody(t *testing.T) {
	var gotHeader string
	var gotBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get(CorrelationIDHeader)
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	client := NewClient(5 * time.Second)
	resp, err := client.Post(context.Background(), server.URL, "corr-123", []byte(`{"amount":5}`))
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if gotHeader != "corr-123" {
		t.Errorf("expected correlation header 'corr-123', got %q", gotHeader)
	}
	if string(gotBody) != `{"amount":5}` {
		t.Errorf("expected body forwarded as-is, got %q", gotBody)
	}
	if !resp.Success() {
		t.Errorf("expected 200 to be a success, got %d", resp.StatusCode)
	}
	if string(resp.Body) != `{"ok":true}` {
		t.Errorf("expected response body captured, got %q", resp.Body)
	}
}

func TestPostReportsNon2xxWithoutError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	client := NewClient(5 * time.Second)
	resp, err := client.Post(context.Background(), server.URL, "corr", nil)
	if err != nil {
		t.Fatalf("expected no transport error for a 4xx response, got: %v", err)
	}
	if resp.Success() {
		t.Errorf("expected 400 to not be Success()")
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestPostUnreachableReturnsError(t *testing.T) {
	client := NewClient(500 * time.Millisecond)
	_, err := client.Post(context.Background(), "http://127.0.0.1:1", "corr", nil)
	if err == nil {
		t.Fatal("expected an error calling an unreachable address")
	}
}

func TestNewCorrelationIDIsUnique(t *testing.T) {
	a, err := NewCorrelationID()
	if err != nil {
		t.Fatalf("NewCorrelationID: %v", err)
	}
	b, err := NewCorrelationID()
	if err != nil {
		t.Fatalf("NewCorrelationID: %v", err)
	}
	if a == b {
		t.Fatalf("expected two correlation IDs to differ, both were %q", a)
	}
	if len(a) != 32 {
		t.Errorf("expected a 32-char hex id (16 bytes), got %d chars: %q", len(a), a)
	}
}

func TestProbeSuccessOnNon5xx(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewClient(5 * time.Second)
	if err := client.Probe(context.Background(), server.URL); err != nil {
		t.Errorf("expected Probe to treat a 404 as alive (app is answering), got error: %v", err)
	}
}

func TestProbeFailsOn5xx(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := NewClient(5 * time.Second)
	if err := client.Probe(context.Background(), server.URL); err == nil {
		t.Error("expected Probe to fail on a 503")
	}
}
