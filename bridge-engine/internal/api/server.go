package api

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/raven-clown/ark/bridge-engine/internal/consumer"
)

type Registry interface {
	Runners() []*consumer.Runner
	PipelineRunners(name string) []*consumer.Runner
}

type staticRegistry struct {
	runners []*consumer.Runner
	byName  map[string][]*consumer.Runner
}

func NewRegistry(runners []*consumer.Runner) Registry {
	byName := make(map[string][]*consumer.Runner)
	for _, r := range runners {
		byName[r.Name()] = append(byName[r.Name()], r)
	}
	return &staticRegistry{runners: runners, byName: byName}
}

func (s *staticRegistry) Runners() []*consumer.Runner {
	return s.runners
}

func (s *staticRegistry) PipelineRunners(name string) []*consumer.Runner {
	return s.byName[name]
}

func NewServer(reg Registry) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("GET /api/v1/pipelines", func(w http.ResponseWriter, r *http.Request) {
		statuses := make([]consumer.Status, 0, len(reg.Runners()))
		for _, run := range reg.Runners() {
			statuses = append(statuses, run.Status())
		}
		writeJSON(w, http.StatusOK, statuses)
	})

	mux.HandleFunc("GET /api/v1/pipelines/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		runners := reg.PipelineRunners(name)
		if len(runners) == 0 {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "pipeline not found: " + name})
			return
		}
		statuses := make([]consumer.Status, 0, len(runners))
		for _, run := range runners {
			statuses = append(statuses, run.Status())
		}
		writeJSON(w, http.StatusOK, statuses)
	})

	mux.HandleFunc("POST /api/v1/pipelines/{name}/pause", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		runners := reg.PipelineRunners(name)
		if len(runners) == 0 {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "pipeline not found: " + name})
			return
		}
		consumer.Pause(runners)
		writeJSON(w, http.StatusOK, map[string]string{"pipeline": name, "state": "paused"})
	})

	mux.HandleFunc("POST /api/v1/pipelines/{name}/resume", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		runners := reg.PipelineRunners(name)
		if len(runners) == 0 {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "pipeline not found: " + name})
			return
		}
		consumer.Resume(runners)
		writeJSON(w, http.StatusOK, map[string]string{"pipeline": name, "state": "running"})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
