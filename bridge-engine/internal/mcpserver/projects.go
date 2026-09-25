package mcpserver

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/raven-clown/ark/bridge-engine/internal/authz"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

// allTools is every tool buildServer can register, so an endpoint's tool
// list can be turned into the set to remove.
var allTools = []string{
	"interpret_request", "get_help", "get_overview", "diagnose_pipeline", "get_recent_events", "explain_error",
	"recommend_tuning", "check_data", "test_message", "get_pipeline_schema", "get_pipeline_config", "list_topics",
	"validate_pipeline_config", "list_pipelines", "get_pipeline_status", "list_dlq_messages", "get_dlq_message",
	"pause_pipeline", "resume_pipeline", "retry_dlq_message", "discard_dlq_message", "create_pipeline", "apply_pipeline_config",
}

// ScopeFor maps a project access level to the MCP scope that grants it.
func ScopeFor(a config.AIAccess) (Scope, bool) {
	switch a {
	case config.AIAccessReadOnly:
		return ScopeViewer, true
	case config.AIAccessOperate:
		return ScopeOperator, true
	case config.AIAccessConfigure:
		return ScopeAdmin, true
	}
	return "", false
}

// NewProjectHandler serves every project's MCP endpoints under
// /mcp/<project>/<endpoint>. Each endpoint only sees its project's
// pipelines, accepts only the tokens in its own environment variable, and
// is limited to the lower of its access and the project's ai_access, and
// to its tool list. Config changes take effect on the next request.
func NewProjectHandler(d Deps) http.Handler {
	var mu sync.Mutex
	cache := map[string]http.Handler{}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/mcp/"), "/"), "/")
		if len(parts) < 2 || d.Config == nil {
			http.NotFound(w, r)
			return
		}
		var project *config.Project
		var endpoint *config.MCPEndpoint
		for _, p := range d.Config.Projects() {
			if p.Name != parts[0] {
				continue
			}
			pc := p
			project = &pc
			for _, e := range p.MCPEndpoints {
				if e.Name == parts[1] {
					ec := e
					endpoint = &ec
				}
			}
		}
		if project == nil || endpoint == nil {
			http.NotFound(w, r)
			return
		}
		scope, ok := ScopeFor(endpoint.Access.Min(project.AIAccess))
		if !ok {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "project " + project.Name + " has ai_access: none"})
			return
		}
		tokens := os.Getenv(endpoint.TokensEnv)
		if strings.TrimSpace(tokens) == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no tokens set for this endpoint; set " + endpoint.TokensEnv + " on the ARK process"})
			return
		}

		spec, _ := json.Marshal(struct {
			P *config.Project
			E *config.MCPEndpoint
			T string
		}{project, endpoint, tokens})
		sum := sha256.Sum256(spec)
		key := hex.EncodeToString(sum[:])

		mu.Lock()
		h, ok := cache[key]
		if !ok {
			pd := d
			pd.Project = project.Name
			h = newEndpointHandler(pd, authz.NewTokenStore(tokens, scope), endpoint.Tools)
			if len(cache) >= 256 {
				clear(cache)
			}
			cache[key] = h
		}
		mu.Unlock()
		h.ServeHTTP(w, r)
	})
}

// removedTools is every tool not in keep; an empty keep keeps everything.
func removedTools(keep []string) []string {
	if len(keep) == 0 {
		return nil
	}
	want := map[string]bool{}
	for _, k := range keep {
		want[k] = true
	}
	var out []string
	for _, t := range allTools {
		if !want[t] {
			out = append(out, t)
		}
	}
	return out
}

type projectEndpointOut struct {
	config.MCPEndpoint
	Path             string          `json:"path"`
	EffectiveAccess  config.AIAccess `json:"effective_access"`
	TokensConfigured bool            `json:"tokens_configured"`
}

type projectOut struct {
	config.Project
	Pipelines          []string               `json:"pipelines"`
	Endpoints          []projectEndpointOut   `json:"endpoints"`
	AssistantKeyReady  bool                   `json:"assistant_key_configured"`
	EffectiveAssistant *config.AssistantModel `json:"effective_assistant,omitempty"`
}

func keySet(m *config.AssistantModel) bool {
	if m == nil {
		return false
	}
	if m.APIKeyEnv == "" {
		return m.Provider == "openai_compatible"
	}
	return os.Getenv(m.APIKeyEnv) != ""
}

func (c *Console) projectsRoutes(mux *http.ServeMux) {
	d := c.d
	mux.HandleFunc("GET /api/v1/projects", func(w http.ResponseWriter, r *http.Request) {
		byProject := map[string][]string{}
		for _, p := range d.Config.Pipelines() {
			byProject[p.Project] = append(byProject[p.Project], p.Name)
		}
		out := []projectOut{}
		for _, p := range d.Config.Projects() {
			po := projectOut{Project: p, Pipelines: byProject[p.Name], Endpoints: []projectEndpointOut{}}
			for _, e := range p.MCPEndpoints {
				po.Endpoints = append(po.Endpoints, projectEndpointOut{MCPEndpoint: e, Path: "/mcp/" + p.Name + "/" + e.Name,
					EffectiveAccess: e.Access.Min(p.AIAccess), TokensConfigured: strings.TrimSpace(os.Getenv(e.TokensEnv)) != ""})
			}
			po.EffectiveAssistant = p.Assistant
			if po.EffectiveAssistant == nil {
				po.EffectiveAssistant = d.Config.DefaultModel()
			}
			po.AssistantKeyReady = keySet(po.EffectiveAssistant)
			out = append(out, po)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"projects":              out,
			"unassigned":            byProject[""],
			"default_assistant":     d.Config.DefaultModel(),
			"default_assistant_key": keySet(d.Config.DefaultModel()),
			"tools":                 allTools,
			"applies_to":            d.Config.Mode(),
		})
	})

	mux.HandleFunc("PUT /api/v1/config/projects/{name}", func(w http.ResponseWriter, r *http.Request) {
		var p config.Project
		if !readJSON(w, r, &p) {
			return
		}
		p.Name = r.PathValue("name")
		config.ApplyProjectDefaults(&p)
		projects := d.Config.Projects()
		replaced := false
		for i := range projects {
			if projects[i].Name == p.Name {
				projects[i], replaced = p, true
			}
		}
		if !replaced {
			projects = append(projects, p)
		}
		if err := config.ValidateProjects(projects, d.Config.Pipelines()); err != nil {
			writeJSON(w, http.StatusBadRequest, errBody(err))
			return
		}
		if err := d.Config.ApplyProjects(r.Context(), projects); err != nil {
			writeJSON(w, http.StatusInternalServerError, errBody(err))
			return
		}
		c.audit(r, "project "+map[bool]string{true: "updated", false: "created"}[replaced], p.Name)
		writeJSON(w, http.StatusOK, map[string]string{"project": p.Name, "state": "applied", "applies_to": d.Config.Mode()})
	})

	mux.HandleFunc("DELETE /api/v1/config/projects/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		for _, p := range d.Config.Pipelines() {
			if p.Project == name {
				writeJSON(w, http.StatusConflict, map[string]string{"error": "pipeline " + p.Name + " still belongs to project " + name + "; move or delete it first"})
				return
			}
		}
		var kept []config.Project
		for _, p := range d.Config.Projects() {
			if p.Name != name {
				kept = append(kept, p)
			}
		}
		if err := d.Config.ApplyProjects(r.Context(), kept); err != nil {
			writeJSON(w, http.StatusInternalServerError, errBody(err))
			return
		}
		c.audit(r, "project deleted", name)
		writeJSON(w, http.StatusOK, map[string]string{"project": name, "state": "deleted"})
	})
}
