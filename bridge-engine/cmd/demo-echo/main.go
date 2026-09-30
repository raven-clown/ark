package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

type item struct {
	ID    string          `json:"id"`
	Key   string          `json:"key,omitempty"`
	Value json.RawMessage `json:"value"`
}

type result struct {
	ID     string          `json:"id"`
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}

// answer is what the app does with one message.
func answer(body []byte, correlationID string) (int, []byte) {
	var msg map[string]any
	if err := json.Unmarshal(body, &msg); err != nil {
		return http.StatusBadRequest, []byte(`{"error":"body is not a JSON object"}`)
	}
	switch {
	case msg["invalid"] == true:
		return http.StatusBadRequest, []byte(`{"error":"invalid order"}`)
	case msg["fail"] == true:
		return http.StatusInternalServerError, []byte(`{"error":"simulated failure"}`)
	}
	out, _ := json.Marshal(map[string]any{
		"received":       json.RawMessage(body),
		"correlation_id": correlationID,
		"processed_by":   "demo-echo",
		"processed_at":   time.Now().UTC().Format(time.RFC3339),
	})
	return http.StatusOK, out
}

func main() {
	addr := os.Getenv("DEMO_ECHO_ADDR")
	if addr == "" {
		addr = ":8081"
	}
	delay, _ := strconv.Atoi(os.Getenv("DEMO_ECHO_DELAY_MS"))
	var slots chan struct{}
	if n, _ := strconv.Atoi(os.Getenv("DEMO_ECHO_MAX_CONCURRENT")); n > 0 {
		slots = make(chan struct{}, n)
	}
	work := func() func() {
		if slots != nil {
			slots <- struct{}{}
		}
		time.Sleep(time.Duration(delay) * time.Millisecond)
		return func() {
			if slots != nil {
				<-slots
			}
		}
	}
	write := func(w http.ResponseWriter, status int, body []byte) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(status)
		_, _ = w.Write(body) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter.no-direct-write-to-responsewriter -- JSON from json.Marshal, served as application/json
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /batch", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Items []item `json:"items"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&in); err != nil {
			write(w, http.StatusBadRequest, []byte(`{"error":"body is not {\"items\": [...]}"}`))
			return
		}
		done := work()
		defer done()
		results := make([]result, len(in.Items))
		for i, it := range in.Items {
			status, body := answer(it.Value, it.ID)
			results[i] = result{ID: it.ID, Status: status, Body: body}
		}
		out, _ := json.Marshal(map[string]any{"results": results})
		write(w, http.StatusOK, out)
	})
	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		done := work()
		defer done()
		status, out := answer(body, r.Header.Get("X-Correlation-ID"))
		write(w, status, out)
	})
	log.Printf("demo-echo listening on %s", addr) // #nosec G706 -- addr is the operator-set DEMO_ECHO_ADDR
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
