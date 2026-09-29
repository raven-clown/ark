package consumer

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

func batchCaller(t *testing.T, size, linger int, h http.HandlerFunc) *caller {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return newCaller("test", config.Target{Mode: config.TargetModeSingleURL, URL: srv.URL, TimeoutMs: 2000, BatchSize: size, BatchLingerMs: linger},
		config.Retry{MaxAttempts: 1}, config.CircuitBreaker{FailureThreshold: 5, CooldownSeconds: 1})
}

func sendAll(c *caller, ids ...string) map[string]batchResult {
	var mu sync.Mutex
	out := map[string]batchResult{}
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			r, _ := c.batch.submit(context.Background(), &batchItem{id: id, key: []byte("k-" + id), value: []byte(`{"n":"` + id + `"}`)})
			mu.Lock()
			out[id] = r
			mu.Unlock()
		}(id)
	}
	wg.Wait()
	return out
}

func TestBatchSendsOneRequestAndHandsEachMessageItsResult(t *testing.T) {
	var calls atomic.Int32
	var sizeHeader string
	c := batchCaller(t, 3, 1000, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		sizeHeader = r.Header.Get(BatchSizeHeader)
		var in batchRequest
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &in)
		var res []map[string]any
		for _, it := range in.Items {
			switch it.ID {
			case "bad":
				res = append(res, map[string]any{"id": it.ID, "status": 422, "body": map[string]string{"error": "no"}})
			case "lost":
			default:
				res = append(res, map[string]any{"id": it.ID, "body": map[string]any{"echo": it.Value, "key": it.Key}})
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": res})
	})
	got := sendAll(c, "ok", "bad", "lost")
	if calls.Load() != 1 || sizeHeader != "3" {
		t.Fatalf("%d requests with %s=%q, want one of 3", calls.Load(), BatchSizeHeader, sizeHeader)
	}
	if r := got["ok"]; r.err != nil || r.resp.StatusCode != 200 || string(r.resp.Body) != `{"echo":{"n":"ok"},"key":"k-ok"}` {
		t.Fatalf("ok = %+v %s", r, r.resp.Body)
	}
	if r := got["bad"]; r.err != nil || r.resp.StatusCode != 422 {
		t.Fatalf("bad = %+v", r)
	}
	if r := got["lost"]; r.err == nil {
		t.Fatalf("a message missing from the results must fail its attempt, got %+v", r.resp)
	}
}

func TestBatchLingerSendsAPartialBatch(t *testing.T) {
	var sizes []int
	var mu sync.Mutex
	c := batchCaller(t, 50, 20, func(w http.ResponseWriter, r *http.Request) {
		var in batchRequest
		_ = json.NewDecoder(r.Body).Decode(&in)
		mu.Lock()
		sizes = append(sizes, len(in.Items))
		mu.Unlock()
		res := make([]map[string]any, len(in.Items))
		for i, it := range in.Items {
			res[i] = map[string]any{"id": it.ID, "body": "done " + it.ID}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": res})
	})
	start := time.Now()
	got := sendAll(c, "a", "b")
	if time.Since(start) > time.Second {
		t.Fatal("a partial batch waited far longer than batch_linger_ms")
	}
	if len(sizes) != 1 || sizes[0] != 2 || string(got["a"].resp.Body) != "done a" {
		t.Fatalf("sizes %v, a = %+v", sizes, got["a"])
	}
}

func TestBatchWholeRequestStatusAppliesToEveryMessage(t *testing.T) {
	c := batchCaller(t, 2, 0, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	for id, r := range sendAll(c, "x", "y") {
		if r.err != nil || r.resp.StatusCode != 429 || !r.resp.RetryLater() || r.resp.RetryAfter != 3*time.Second {
			t.Fatalf("%s = %+v", id, r)
		}
	}
	bad := batchCaller(t, 2, 0, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	for id, r := range sendAll(bad, "x", "y") {
		if r.err == nil {
			t.Fatalf("%s: a body without results must fail the attempt", id)
		}
	}
}

func TestBatchKeepsNonJSONValuesAsStrings(t *testing.T) {
	var seen any
	c := batchCaller(t, 2, 0, func(w http.ResponseWriter, r *http.Request) {
		var in batchRequest
		_ = json.NewDecoder(r.Body).Decode(&in)
		seen = in.Items[0].Value
		res := []map[string]any{}
		for _, it := range in.Items {
			res = append(res, map[string]any{"id": it.ID})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": res})
	})
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.batch.submit(context.Background(), &batchItem{id: strconv.Itoa(i), value: []byte("plain text")})
		}()
	}
	wg.Wait()
	if seen != "plain text" {
		t.Fatalf("value = %#v", seen)
	}
}
