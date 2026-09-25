package config

import (
	"os"
	"testing"
)

func TestProjectsValidate(t *testing.T) {
	ok := []Project{{Name: "commerce", AIAccess: AIAccessOperate, MCPEndpoints: []MCPEndpoint{{Name: "ops", Access: AIAccessOperate, TokensEnv: "ARK_MCP_COMMERCE_OPS"}}}}
	if err := ValidateProjects(ok, []Pipeline{{Name: "orders", Project: "commerce"}}); err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		projects  []Project
		pipelines []Pipeline
	}{
		"unknown project":   {ok, []Pipeline{{Name: "orders", Project: "nope"}}},
		"bad name":          {[]Project{{Name: "Commerce", AIAccess: AIAccessReadOnly}}, nil},
		"duplicate":         {[]Project{{Name: "a", AIAccess: AIAccessReadOnly}, {Name: "a", AIAccess: AIAccessReadOnly}}, nil},
		"endpoint none":     {[]Project{{Name: "a", AIAccess: AIAccessReadOnly, MCPEndpoints: []MCPEndpoint{{Name: "e", Access: AIAccessNone, TokensEnv: "X"}}}}, nil},
		"token in config":   {[]Project{{Name: "a", AIAccess: AIAccessReadOnly, MCPEndpoints: []MCPEndpoint{{Name: "e", Access: AIAccessReadOnly, TokensEnv: "secret-token-123"}}}}, nil},
		"compatible no url": {[]Project{{Name: "a", AIAccess: AIAccessReadOnly, Assistant: &AssistantModel{Provider: "openai_compatible", Model: "qwen"}}}, nil},
		"unknown provider":  {[]Project{{Name: "a", AIAccess: AIAccessReadOnly, Assistant: &AssistantModel{Provider: "skynet", Model: "x"}}}, nil},
	}
	for name, c := range cases {
		if err := ValidateProjects(c.projects, c.pipelines); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestAIAccessMin(t *testing.T) {
	if AIAccessConfigure.Min(AIAccessReadOnly) != AIAccessReadOnly || AIAccessOperate.Min(AIAccessConfigure) != AIAccessOperate {
		t.Fatal("Min should return the lower level")
	}
}

func TestProjectsLoadFromYAML(t *testing.T) {
	path := t.TempDir() + "/c.yaml"
	if err := os.WriteFile(path, []byte(`
brokers: [k:9092]
projects:
  - name: commerce
    mcp_endpoints:
      - {name: support, tokens_env: ARK_MCP_SUPPORT}
    assistant: {provider: anthropic, model: claude-sonnet-5, api_key_env: ANTHROPIC_API_KEY}
pipelines:
  - name: orders
    project: commerce
    source_topic: a
    destination_topic: b
    dead_letter_topic: c
    consumer_group: g
    target: {url: "http://x"}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.Projects[0]
	if p.AIAccess != AIAccessReadOnly || p.MCPEndpoints[0].Access != AIAccessReadOnly || p.Assistant.MaxSteps != 8 {
		t.Fatalf("defaults not applied: %+v", p)
	}
}
