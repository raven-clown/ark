package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/raven-clown/ark/bridge-engine/internal/authz"
	"github.com/raven-clown/ark/bridge-engine/internal/cluster"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/consumer"
	"github.com/raven-clown/ark/bridge-engine/internal/dlq"
)

type Registry interface {
	Runners() []*consumer.Runner
	PipelineRunners(name string) []*consumer.Runner
	SetPaused(name string, paused bool) bool
	// DLQBrowser is a fallback for pipelines not running on this node.
	DLQBrowser(name, kind string) (*dlq.Browser, config.MCPAccess, bool)
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

func (s *staticRegistry) DLQBrowser(string, string) (*dlq.Browser, config.MCPAccess, bool) {
	return nil, "", false
}

func (s *staticRegistry) SetPaused(name string, paused bool) bool {
	runners, ok := s.byName[name]
	if !ok {
		return false
	}
	if paused {
		consumer.Pause(runners)
	} else {
		consumer.Resume(runners)
	}
	return true
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
// snapshot. Every route except /healthz and /metrics requires a token from
// tokens (see guard); with no tokens configured, only requests from this
// host are served.
func NewServer(reg Registry, reload Reloader, clusterNode *cluster.Node, tokens *authz.TokenStore) http.Handler {
	if tokens == nil {
		tokens = &authz.TokenStore{}
	}
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
			"enabled":              true,
			"cluster":              status.Cluster,
			"node_id":              status.NodeID,
			"leader":               status.Leader,
			"live_nodes":           status.LiveNodes,
			"config_version":       status.ConfigVersion,
			"node_config_versions": status.NodeVersions,
			"leader_node":          status.LeaderNode,
			"node_labels":          status.NodeLabels,
		})
	})

	mux.HandleFunc("GET /api/v1/cluster/pipelines", func(w http.ResponseWriter, r *http.Request) {
		if clusterNode == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "cluster mode is not enabled"})
			return
		}
		writeJSON(w, http.StatusOK, clusterNode.ClusterPipelines())
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
			if clusterNode != nil && clusterNode.AssignedElsewhere(name) {
				writeJSON(w, http.StatusOK, map[string]any{"pipeline": name, "local_workers": 0, "note": "running on other cluster nodes"})
				return
			}
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
		if !reg.SetPaused(name, true) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "pipeline not found: " + name})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"pipeline": name, "state": "paused"})
	})

	mux.HandleFunc("POST /api/v1/pipelines/{name}/resume", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if !reg.SetPaused(name, false) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "pipeline not found: " + name})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"pipeline": name, "state": "running"})
	})

	registerDLQRoutes(mux, reg, "dlq", func(run *consumer.Runner) *dlq.Browser { return run.DLQBrowser() })
	registerDLQRoutes(mux, reg, "reject", func(run *consumer.Runner) *dlq.Browser { return run.RejectBrowser() })

	return guard(tokens, mux)
}

// requiredScope maps a request to the scope it needs. It is deliberately
// path-agnostic apart from the public probes and the reload endpoint, so a
// newly added route is protected by default rather than open by default.
func requiredScope(r *http.Request) (scope authz.Scope, public bool) {
	switch {
	case r.Method == http.MethodGet && (r.URL.Path == "/healthz" || r.URL.Path == "/metrics"):
		return "", true
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		return authz.ScopeViewer, false
	case r.Method == http.MethodPost && (r.URL.Path == "/api/v1/config/validate" || strings.HasSuffix(r.URL.Path, "/test-message") || strings.HasSuffix(r.URL.Path, "/rules/test") || r.URL.Path == "/api/v1/assistant/chat"):
		return authz.ScopeViewer, false
	case strings.HasPrefix(r.URL.Path, "/api/v1/config/") || strings.HasSuffix(r.URL.Path, "/scale") || strings.HasSuffix(r.URL.Path, "/rules/preview"):
		return authz.ScopeAdmin, false
	default:
		return authz.ScopeOperator, false
	}
}

// Guard protects next with the same token rules as the rest of the API and
// puts the caller on the request context (authz.CallerFrom).
func Guard(tokens *authz.TokenStore, next http.Handler) http.Handler {
	if tokens == nil {
		tokens = &authz.TokenStore{}
	}
	return guard(tokens, next)
}

func guard(tokens *authz.TokenStore, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		min, public := requiredScope(r)
		if public {
			next.ServeHTTP(w, r)
			return
		}

		if !tokens.Enabled() {
			if authz.IsLoopback(r) {
				next.ServeHTTP(w, r.WithContext(authz.WithCaller(r.Context(), authz.Caller{ID: "localhost", Scope: authz.ScopeAdmin})))
				return
			}
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "REST API tokens are not configured (ARK_API_VIEWER_TOKENS / ARK_API_OPERATOR_TOKENS / ARK_API_ADMIN_TOKENS), so only requests from localhost are accepted"})
			return
		}

		token, ok := authz.BearerToken(r)
		scope, known := tokens.Lookup(token)
		if !ok || !known {
			w.Header().Set("WWW-Authenticate", `Bearer realm="ark-api"`)
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid or missing bearer token"})
			return
		}
		if !scope.AtLeast(min) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "this action needs the " + string(min) + " scope, token has " + string(scope)})
			return
		}
		next.ServeHTTP(w, r.WithContext(authz.WithCaller(r.Context(), authz.Caller{ID: authz.UserID(token), Scope: scope})))
	})
}

func registerDLQRoutes(mux *http.ServeMux, reg Registry, kind string, pick func(*consumer.Runner) *dlq.Browser) {
	findBrowser := func(pipelineName string) (*dlq.Browser, bool) {
		for _, run := range reg.PipelineRunners(pipelineName) {
			if b := pick(run); b != nil {
				return b, true
			}
		}
		if b, _, ok := reg.DLQBrowser(pipelineName, kind); ok {
			return b, true
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
		found, err := browser.Discard(r.Context(), r.PathValue("id"))
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if !found {
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
