package mcpserver

import (
	"strings"
	"testing"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

// Found live: a project's MCP endpoint could pause another project's
// pipeline, and the global endpoint ignored a project's ai_access, because
// the write tools checked only the pipeline's own mcp_access.
func TestWriteToolsStayInsideTheProjectAndItsAIAccess(t *testing.T) {
	pipe := func(name, project string) config.Pipeline {
		return config.Pipeline{Name: name, Project: project, MCPAccess: config.MCPAccessReadWrite}
	}
	src := &memSource{
		p:        []config.Pipeline{pipe("shop-flow", "shop"), pipe("parts", ""), pipe("ledger", "vault")},
		projects: []config.Project{{Name: "shop", AIAccess: config.AIAccessOperate}, {Name: "vault", AIAccess: config.AIAccessReadOnly}},
	}
	d := Deps{Registry: api.NewRegistry(nil), Config: src}
	hidden := func(err error) bool { return err != nil && strings.Contains(err.Error(), "not visible") }
	running := func(err error) bool { return err != nil && strings.Contains(err.Error(), "isn't running") }

	shop := d
	shop.Project = "shop"
	if err := writablePipeline(shop, "parts"); !hidden(err) {
		t.Errorf("shop's endpoint reached parts: %v", err)
	}
	if err := writablePipeline(shop, "shop-flow"); !running(err) {
		t.Errorf("shop's endpoint should pass the access checks for its own pipeline, got %v", err)
	}
	if err := writablePipeline(d, "ledger"); err == nil || !strings.Contains(err.Error(), "mcp_access: read_only") {
		t.Errorf("a project with ai_access: read_only must cap writes on the global endpoint, got %v", err)
	}
	if _, _, err := findDLQBrowser(shop, "parts", "dlq"); !hidden(err) {
		t.Errorf("shop's endpoint reached parts' DLQ: %v", err)
	}
}

func TestVisibleToMCP(t *testing.T) {
	cases := map[string]bool{
		"read_only":  true,
		"read_write": true,
		"none":       false,
		// An empty string isn't a real config value in practice --
		// config.applyDefaults always fills it in to "read_only" before
		// this ever runs -- but matches that default's spirit: only an
		// explicit "none" hides a pipeline.
		"": true,
	}
	for access, want := range cases {
		if got := visibleToMCP(access); got != want {
			t.Errorf("visibleToMCP(%q) = %v, want %v", access, got, want)
		}
	}
}

func TestWritableByMCP(t *testing.T) {
	cases := map[string]bool{
		"read_write": true,
		"read_only":  false,
		"none":       false,
		"":           false,
	}
	for access, want := range cases {
		if got := writableByMCP(access); got != want {
			t.Errorf("writableByMCP(%q) = %v, want %v", access, got, want)
		}
	}
}

func TestRequireWritableIsAHardCeiling(t *testing.T) {
	if err := requireWritable("read_only", "billing"); err == nil {
		t.Fatal("expected a read_only pipeline to reject a write regardless of token scope")
	}
	if err := requireWritable("none", "hidden"); err == nil {
		t.Fatal("expected a none pipeline to reject a write")
	}
	if err := requireWritable("read_write", "order-processor"); err != nil {
		t.Fatalf("expected a read_write pipeline to allow a write, got: %v", err)
	}
}

func TestTokenStoreScopesAreIndependent(t *testing.T) {
	t.Setenv("ARK_MCP_VIEWER_TOKENS", "v1, v2")
	t.Setenv("ARK_MCP_OPERATOR_TOKENS", "o1")
	t.Setenv("ARK_MCP_ADMIN_TOKENS", "")

	store := LoadTokenStoreFromEnv()

	if scope, ok := store.Lookup("v1"); !ok || scope != ScopeViewer {
		t.Errorf("expected v1 to be viewer, got scope=%v ok=%v", scope, ok)
	}
	if scope, ok := store.Lookup("v2"); !ok || scope != ScopeViewer {
		t.Errorf("expected v2 to be viewer, got scope=%v ok=%v", scope, ok)
	}
	if scope, ok := store.Lookup("o1"); !ok || scope != ScopeOperator {
		t.Errorf("expected o1 to be operator, got scope=%v ok=%v", scope, ok)
	}
	if _, ok := store.Lookup("nonexistent"); ok {
		t.Error("expected an unknown token to not resolve to any scope")
	}
	if !store.Enabled() {
		t.Error("expected the store to be enabled since viewer/operator tokens are set")
	}
}

func TestTokenStoreDisabledWhenEmpty(t *testing.T) {
	t.Setenv("ARK_MCP_VIEWER_TOKENS", "")
	t.Setenv("ARK_MCP_OPERATOR_TOKENS", "")
	t.Setenv("ARK_MCP_ADMIN_TOKENS", "")

	store := LoadTokenStoreFromEnv()
	if store.Enabled() {
		t.Error("expected the store to be disabled when no tokens are configured at all")
	}
}
