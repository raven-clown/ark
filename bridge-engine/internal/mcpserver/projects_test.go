package mcpserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

type bearer struct {
	token string
	next  http.RoundTripper
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.next.RoundTrip(r)
}

func connect(t *testing.T, url, token string) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	tr := &mcp.StreamableClientTransport{Endpoint: url, HTTPClient: &http.Client{Transport: bearer{token, http.DefaultTransport}}}
	cs, err := client.Connect(context.Background(), tr, nil)
	if err != nil {
		t.Fatalf("connect %s: %v", url, err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func toolNames(t *testing.T, cs *mcp.ClientSession) []string {
	t.Helper()
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, tool := range res.Tools {
		out = append(out, tool.Name)
	}
	sort.Strings(out)
	return out
}

func projectFixture(t *testing.T) (*memSource, *httptest.Server) {
	t.Helper()
	orders := mustPipeline(t, pipelineYAML("orders", "orders.raw", "orders.done"))
	orders.Project, orders.MCPAccess = "commerce", config.MCPAccessReadWrite
	audit := mustPipeline(t, pipelineYAML("audit", "audit.raw", "audit.done"))
	audit.Project, audit.MCPAccess = "compliance", config.MCPAccessReadWrite
	loose := mustPipeline(t, pipelineYAML("loose", "loose.raw", "loose.done"))
	loose.MCPAccess = config.MCPAccessReadWrite
	src := &memSource{p: []config.Pipeline{orders, audit, loose}, projects: []config.Project{
		{Name: "commerce", AIAccess: config.AIAccessOperate, MCPEndpoints: []config.MCPEndpoint{
			{Name: "ops", Access: config.AIAccessConfigure, TokensEnv: "ARK_TEST_OPS"},
			{Name: "support", Access: config.AIAccessReadOnly, TokensEnv: "ARK_TEST_SUPPORT", Tools: []string{"get_overview", "diagnose_pipeline"}},
		}},
		{Name: "compliance", AIAccess: config.AIAccessNone, MCPEndpoints: []config.MCPEndpoint{
			{Name: "bot", Access: config.AIAccessReadOnly, TokensEnv: "ARK_TEST_BOT"},
		}},
	}}
	t.Setenv("ARK_TEST_OPS", "ops-token")
	t.Setenv("ARK_TEST_SUPPORT", "support-token")
	t.Setenv("ARK_TEST_BOT", "bot-token")
	d := Deps{Registry: api.NewRegistry(nil), Config: src}
	mux := http.NewServeMux()
	mux.Handle("/mcp/", NewProjectHandler(d))
	mux.Handle("/mcp", NewHTTPHandler(d, LoadTokenStoreFromEnv()))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return src, srv
}

func TestProjectEndpointSeesOnlyItsProject(t *testing.T) {
	_, srv := projectFixture(t)
	cs := connect(t, srv.URL+"/mcp/commerce/ops", "ops-token")
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "list_pipelines", Arguments: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	text := res.Content[0].(*mcp.TextContent).Text
	if !strings.Contains(text, `"orders"`) || strings.Contains(text, "audit") || strings.Contains(text, "loose") {
		t.Fatalf("endpoint leaked other projects' pipelines: %s", text)
	}
}

func TestProjectAccessIsACeiling(t *testing.T) {
	_, srv := projectFixture(t)
	// ops asks for configure, but commerce only allows operate.
	tools := strings.Join(toolNames(t, connect(t, srv.URL+"/mcp/commerce/ops", "ops-token")), ",")
	if !strings.Contains(tools, "pause_pipeline") || strings.Contains(tools, "create_pipeline") {
		t.Fatalf("expected operate tools without config tools, got %s", tools)
	}
}

func TestEndpointToolList(t *testing.T) {
	_, srv := projectFixture(t)
	got := toolNames(t, connect(t, srv.URL+"/mcp/commerce/support", "support-token"))
	if strings.Join(got, ",") != "diagnose_pipeline,get_overview" {
		t.Fatalf("tools = %v", got)
	}
}

func TestEndpointTokensAndStatus(t *testing.T) {
	_, srv := projectFixture(t)
	post := func(path, token string) int {
		req, _ := http.NewRequest("POST", srv.URL+path, strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if c := post("/mcp/commerce/ops", "support-token"); c != http.StatusUnauthorized {
		t.Errorf("another endpoint's token: %d, want 401", c)
	}
	if c := post("/mcp/commerce/nope", "ops-token"); c != http.StatusNotFound {
		t.Errorf("unknown endpoint: %d, want 404", c)
	}
	if c := post("/mcp/compliance/bot", "bot-token"); c != http.StatusForbidden {
		t.Errorf("ai_access none: %d, want 403", c)
	}
}

func TestGlobalMCPRespectsProjectAccess(t *testing.T) {
	src, _ := projectFixture(t)
	d := Deps{Registry: api.NewRegistry(nil), Config: src}
	names := map[string]config.MCPAccess{}
	for _, p := range d.visiblePipelines() {
		names[p.Name] = p.MCPAccess
	}
	if _, ok := names["audit"]; ok {
		t.Error("a pipeline in an ai_access: none project must be hidden")
	}
	if names["orders"] != config.MCPAccessReadWrite || names["loose"] != config.MCPAccessReadWrite {
		t.Errorf("unexpected access: %v", names)
	}
	src.projects[0].AIAccess = config.AIAccessReadOnly
	for _, p := range d.visiblePipelines() {
		if p.Name == "orders" && p.MCPAccess != config.MCPAccessReadOnly {
			t.Error("a read_only project must lower its pipelines to read_only")
		}
	}
}

func TestAllToolsListMatchesRegistration(t *testing.T) {
	s := buildServer(ScopeAdmin, Deps{Registry: api.NewRegistry(nil), Config: &memSource{}}, newConfirmations())
	ct, st := mcp.NewInMemoryTransports()
	if _, err := s.Connect(context.Background(), st, nil); err != nil {
		t.Fatal(err)
	}
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "t", Version: "1"}, nil).Connect(context.Background(), ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	got := toolNames(t, cs)
	want := append([]string(nil), allTools...)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("allTools is out of date:\n got  %v\n want %v", got, want)
	}
}
