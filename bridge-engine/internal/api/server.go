package api

import (
	"encoding/json"
	"net/http"

	"github.com/raven-clown/ark/bridge-engine/internal/consumer"
)

type Registry interface {
	Runners() []*consumer.Runner
}

type staticRegistry struct {
	runners []*consumer.Runner
}

func NewRegistry(runners []*consumer.Runner) Registry {
	return &staticRegistry{runners: runners}
}

func (s *staticRegistry) Runners() []*consumer.Runner {
	return s.runners
}

func NewServer(reg Registry) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("GET /api/v1/pipelines", func(w http.ResponseWriter, r *http.Request) {
		statuses := make([]consumer.Status, 0, len(reg.Runners()))
		for _, run := range reg.Runners() {
			statuses = append(statuses, run.Status())
		}
		writeJSON(w, http.StatusOK, statuses)
	})

	mux.HandleFunc("GET /api/v1/pipelines/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		for _, run := range reg.Runners() {
			if run.Name() == name {
				writeJSON(w, http.StatusOK, run.Status())
				return
			}
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "pipeline not found: " + name})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
