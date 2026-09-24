package api

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/raven-clown/ark/bridge-engine/internal/cluster"
	"github.com/raven-clown/ark/bridge-engine/internal/consumer"
	"github.com/raven-clown/ark/bridge-engine/internal/dlq"
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

// Reloader re-reads the config file from disk and reconciles running
// pipelines to match it: starting new ones, stopping removed ones, and
// restarting ones whose config changed. It returns an error if the file
// fails to parse or validate, in which case the running pipelines are left
// untouched. A pipeline (re)started by Reload keeps running past the
// lifetime of whatever request triggered it, exactly like one started at
// boot; implementations must not tie its lifetime to the caller's context.
type Reloader interface {
	Reload() error
}

// NewServer builds the REST API. clusterNode is nil when cluster mode is
// off; GET /api/v1/cluster then reports that explicitly instead of a
// snapshot.
func NewServer(reg Registry, reload Reloader, clusterNode *cluster.Node) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("GET /api/v1/cluster", func(w http.ResponseWriter, r *http.Request) {
		if clusterNode == nil {
			writeJSON(w, http.StatusOK, map[string]any{"enabled": false})
			return
		}
		status := clusterNode.StatusSnapshot()
		writeJSON(w, http.StatusOK, map[string]any{
			"enabled":    true,
			"node_id":    status.NodeID,
			"leader":     status.Leader,
			"live_nodes": status.LiveNodes,
		})
	})

	mux.HandleFunc("POST /api/v1/config/reload", func(w http.ResponseWriter, r *http.Request) {
		if reload == nil {
			writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "hot-reload not configured"})
			return
		}
		if err := reload.Reload(); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"state": "reloaded"})
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

	registerDLQRoutes(mux, reg, "dlq", func(run *consumer.Runner) *dlq.Browser { return run.DLQBrowser() })
	registerDLQRoutes(mux, reg, "reject", func(run *consumer.Runner) *dlq.Browser { return run.RejectBrowser() })

	return mux
}

func registerDLQRoutes(mux *http.ServeMux, reg Registry, kind string, pick func(*consumer.Runner) *dlq.Browser) {
	findBrowser := func(pipelineName string) (*dlq.Browser, bool) {
		for _, run := range reg.PipelineRunners(pipelineName) {
			if b := pick(run); b != nil {
				return b, true
			}
		}
		return nil, false
	}

	mux.HandleFunc("GET /api/v1/pipelines/{name}/"+kind, func(w http.ResponseWriter, r *http.Request) {
		browser, ok := findBrowser(r.PathValue("name"))
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": kind + " not configured for pipeline: " + r.PathValue("name")})
			return
		}
		writeJSON(w, http.StatusOK, browser.List())
	})

	mux.HandleFunc("GET /api/v1/pipelines/{name}/"+kind+"/{id}", func(w http.ResponseWriter, r *http.Request) {
		browser, ok := findBrowser(r.PathValue("name"))
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": kind + " not configured for pipeline: " + r.PathValue("name")})
			return
		}
		entry, ok := browser.Get(r.PathValue("id"))
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": kind + " entry not found: " + r.PathValue("id")})
			return
		}
		writeJSON(w, http.StatusOK, entry)
	})

	mux.HandleFunc("POST /api/v1/pipelines/{name}/"+kind+"/{id}/retry", func(w http.ResponseWriter, r *http.Request) {
		browser, ok := findBrowser(r.PathValue("name"))
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": kind + " not configured for pipeline: " + r.PathValue("name")})
			return
		}
		if err := browser.Retry(r.Context(), r.PathValue("id")); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"id": r.PathValue("id"), "state": "retried"})
	})

	mux.HandleFunc("POST /api/v1/pipelines/{name}/"+kind+"/{id}/discard", func(w http.ResponseWriter, r *http.Request) {
		browser, ok := findBrowser(r.PathValue("name"))
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": kind + " not configured for pipeline: " + r.PathValue("name")})
			return
		}
		if !browser.Discard(r.PathValue("id")) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": kind + " entry not found: " + r.PathValue("id")})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"id": r.PathValue("id"), "state": "discarded"})
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
