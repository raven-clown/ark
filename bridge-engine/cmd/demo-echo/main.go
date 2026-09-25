// demo-echo is a tiny stand-in for "your app", used by docker compose so
// ARK can be tried without writing anything: it answers every POST with
// the message it received plus a processed_at timestamp. A message with
// "fail": true gets a 500, and one with "invalid": true a 400, to show
// retries, dead-lettering and rejects.
package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	addr := os.Getenv("DEMO_ECHO_ADDR")
	if addr == "" {
		addr = ":8081"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var msg map[string]any
		if err := json.Unmarshal(body, &msg); err != nil {
			http.Error(w, `{"error":"body is not a JSON object"}`, http.StatusBadRequest)
			return
		}
		switch {
		case msg["invalid"] == true:
			http.Error(w, `{"error":"invalid order"}`, http.StatusBadRequest)
			return
		case msg["fail"] == true:
			http.Error(w, `{"error":"simulated failure"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		out, _ := json.Marshal(map[string]any{
			"received":       json.RawMessage(body),
			"correlation_id": r.Header.Get("X-Correlation-ID"),
			"processed_by":   "demo-echo",
			"processed_at":   time.Now().UTC().Format(time.RFC3339),
		})
		_, _ = w.Write(out)
	})
	log.Printf("demo-echo listening on %s", addr) // #nosec G706 -- addr is the operator-set DEMO_ECHO_ADDR
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
