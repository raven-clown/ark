package mcpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
	"github.com/raven-clown/ark/bridge-engine/internal/authz"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/tap"
)

type memSource struct {
	mu       sync.Mutex
	p        []config.Pipeline
	projects []config.Project
}

func (s *memSource) Pipelines() []config.Pipeline {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]config.Pipeline(nil), s.p...)
}

func (s *memSource) Apply(_ context.Context, p []config.Pipeline) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.p = p
	return nil
}

func (s *memSource) Mode() string { return "file" }

func (s *memSource) Projects() []config.Project {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]config.Project(nil), s.projects...)
}

func (s *memSource) ApplyProjects(_ context.Context, p []config.Project) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.projects = p
	return nil
}

func (s *memSource) DefaultModel() *config.AssistantModel { return nil }

func pipelineYAML(name, source, dest string) string {
	return `name: ` + name + `
source_topic: ` + source + `
destination_topic: ` + dest + `
dead_letter_topic: ` + name + `.dlq
consumer_group: ` + name + `-group
target:
  url: http://app:8080/process
`
}

func mustPipeline(t *testing.T, src string) config.Pipeline {
	t.Helper()
	p, err := parsePipelineYAML(src)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// consoleServer serves the console with the caller taken from X-Caller.
func consoleServer(t *testing.T, src *memSource, hub *tap.Hub) *httptest.Server {
	t.Helper()
	c := NewConsole(Deps{Registry: api.NewRegistry(nil), Config: src})
	c.Tap = hub
	routes := c.Handler()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := authz.WithCaller(r.Context(), authz.Caller{ID: r.Header.Get("X-Caller"), Scope: authz.ScopeAdmin})
		routes.ServeHTTP(w, r.WithContext(ctx))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func call(t *testing.T, srv *httptest.Server, method, path, caller string, body any) (int, map[string]any) {
	t.Helper()
	var rd *strings.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = strings.NewReader(string(b))
	} else {
		rd = strings.NewReader("")
	}
	req, _ := http.NewRequest(method, srv.URL+path, rd)
	req.Header.Set("X-Caller", caller)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestConsolePreviewConfirmIsBoundToCaller(t *testing.T) {
	src := &memSource{}
	srv := consoleServer(t, src, tap.NewHub())

	code, out := call(t, srv, "POST", "/api/v1/config/preview", "alice", map[string]string{"yaml": pipelineYAML("orders", "orders.raw", "orders.done")})
	if code != 200 || out["state"] != "awaiting_confirmation" {
		t.Fatalf("preview: %d %v", code, out)
	}
	token := out["confirm_token"].(string)

	if code, out := call(t, srv, "POST", "/api/v1/config/confirm", "mallory", map[string]string{"confirm_token": token}); code != http.StatusConflict {
		t.Fatalf("another caller confirmed: %d %v", code, out)
	}
	if len(src.Pipelines()) != 0 {
		t.Fatal("applied without a valid confirm")
	}

	// The failed attempt used the token up, so preview again.
	_, out = call(t, srv, "POST", "/api/v1/config/preview", "alice", map[string]string{"yaml": pipelineYAML("orders", "orders.raw", "orders.done")})
	code, out = call(t, srv, "POST", "/api/v1/config/confirm", "alice", map[string]string{"confirm_token": out["confirm_token"].(string)})
	if code != 200 || out["state"] != "applied" || len(src.Pipelines()) != 1 {
		t.Fatalf("confirm: %d %v, pipelines %d", code, out, len(src.Pipelines()))
	}

	code, out = call(t, srv, "POST", "/api/v1/config/preview", "alice", map[string]string{"delete": "orders"})
	if code != 200 {
		t.Fatalf("delete preview: %d %v", code, out)
	}
	call(t, srv, "POST", "/api/v1/config/confirm", "alice", map[string]string{"confirm_token": out["confirm_token"].(string)})
	if len(src.Pipelines()) != 0 {
		t.Fatal("delete was not applied")
	}
}

func TestConsoleSeesPipelinesHiddenFromMCP(t *testing.T) {
	p := mustPipeline(t, pipelineYAML("audit", "audit.raw", "audit.done"))
	p.MCPAccess = config.MCPAccessNone
	srv := consoleServer(t, &memSource{p: []config.Pipeline{p}}, tap.NewHub())
	if code, out := call(t, srv, "GET", "/api/v1/config/pipelines/audit", "alice", nil); code != 200 {
		t.Fatalf("console should see mcp_access: none pipelines: %d %v", code, out)
	}
}

func TestConsoleScale(t *testing.T) {
	src := &memSource{p: []config.Pipeline{mustPipeline(t, pipelineYAML("orders", "orders.raw", "orders.done"))}}
	srv := consoleServer(t, src, tap.NewHub())
	if code, _ := call(t, srv, "POST", "/api/v1/pipelines/orders/scale", "alice", map[string]int{"workers": 0}); code != 400 {
		t.Fatalf("workers 0 accepted: %d", code)
	}
	if code, out := call(t, srv, "POST", "/api/v1/pipelines/orders/scale", "alice", map[string]int{"workers": 4}); code != 200 {
		t.Fatalf("scale: %d %v", code, out)
	}
	if w := src.Pipelines()[0].Workers; w != 4 {
		t.Fatalf("workers = %d", w)
	}
}

func TestTopologyChainsPipelinesThroughTopics(t *testing.T) {
	src := &memSource{p: []config.Pipeline{
		mustPipeline(t, pipelineYAML("ingest", "orders.raw", "orders.clean")),
		mustPipeline(t, pipelineYAML("fraud", "orders.clean", "orders.checked")),
	}}
	topo := topology(Deps{Registry: api.NewRegistry(nil), Config: src, AllPipelines: true})
	var into, outOf bool
	for _, e := range topo.Edges {
		if e.From == "pipeline:ingest" && e.To == "topic:orders.clean" && e.Role == "destination" {
			into = true
		}
		if e.From == "topic:orders.clean" && e.To == "pipeline:fraud" && e.Role == "consume" {
			outOf = true
		}
	}
	count := 0
	for _, n := range topo.Nodes {
		if n.ID == "topic:orders.clean" {
			count++
		}
	}
	if !into || !outOf || count != 1 {
		t.Fatalf("pipelines not chained through one topic node: %+v", topo)
	}
}

func TestTailStreamsFilteredRecords(t *testing.T) {
	hub := tap.NewHub()
	src := &memSource{p: []config.Pipeline{mustPipeline(t, pipelineYAML("orders", "orders.raw", "orders.done"))}}
	srv := consoleServer(t, src, hub)

	req, _ := http.NewRequest("GET", srv.URL+"/api/v1/pipelines/orders/tail?stage=out", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !hub.Watching("orders") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	hub.Publish(tap.Record{Pipeline: "orders", Stage: tap.StageIn, CorrelationID: "skip-me"})
	hub.Publish(tap.Record{Pipeline: "orders", Stage: tap.StageOut, To: tap.ToDLQ, CorrelationID: "abc"})

	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") || !strings.Contains(line, "correlation_id") {
			continue
		}
		var rec tap.Record
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &rec); err != nil {
			t.Fatal(err)
		}
		if rec.CorrelationID != "abc" || rec.To != tap.ToDLQ {
			t.Fatalf("got %+v, the stage filter should have skipped the in record", rec)
		}
		return
	}
	t.Fatal("stream ended without a record")
}

func TestConsoleProjectsCRUD(t *testing.T) {
	src := &memSource{p: []config.Pipeline{mustPipeline(t, pipelineYAML("orders", "orders.raw", "orders.done"))}}
	srv := consoleServer(t, src, tap.NewHub())
	body := map[string]any{"ai_access": "operate", "mcp_endpoints": []map[string]any{{"name": "ops", "access": "operate", "tokens_env": "ARK_MCP_COMMERCE_OPS"}}}
	if code, out := call(t, srv, "PUT", "/api/v1/config/projects/commerce", "alice", body); code != 200 {
		t.Fatalf("create: %d %v", code, out)
	}
	if len(src.Projects()) != 1 || src.Projects()[0].AIAccess != config.AIAccessOperate {
		t.Fatalf("not stored: %+v", src.Projects())
	}
	if code, _ := call(t, srv, "PUT", "/api/v1/config/projects/Bad", "alice", body); code != 400 {
		t.Fatalf("invalid name accepted: %d", code)
	}
	code, out := call(t, srv, "GET", "/api/v1/projects", "alice", nil)
	if code != 200 || len(out["projects"].([]any)) != 1 {
		t.Fatalf("list: %d %v", code, out)
	}
	p := src.p[0]
	p.Project = "commerce"
	src.p = []config.Pipeline{p}
	if code, _ := call(t, srv, "DELETE", "/api/v1/config/projects/commerce", "alice", nil); code != 409 {
		t.Fatalf("deleted a project that still has pipelines: %d", code)
	}
	src.p = nil
	if code, _ := call(t, srv, "DELETE", "/api/v1/config/projects/commerce", "alice", nil); code != 200 || len(src.Projects()) != 0 {
		t.Fatalf("delete: %d", code)
	}
}
