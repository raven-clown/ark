package llm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

var testTools = []Tool{{Name: "diagnose_pipeline", Description: "diagnose", Schema: map[string]any{
	"type": "object", "additionalProperties": false, "$schema": "x",
	"properties": map[string]any{"name": map[string]any{"type": "string"}},
	"required":   []any{"name"},
}}}

// capture serves reply to every request and records the request bodies.
func capture(t *testing.T, reply string) (*httptest.Server, *[]map[string]any, *[]http.Header) {
	t.Helper()
	var bodies []map[string]any
	var headers []http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		bodies = append(bodies, m)
		headers = append(headers, r.Header.Clone())
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(reply))
	}))
	t.Cleanup(srv.Close)
	return srv, &bodies, &headers
}

func TestOpenAICompatibleToolCall(t *testing.T) {
	srv, bodies, headers := capture(t, `{"choices":[{"finish_reason":"tool_calls","message":{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"diagnose_pipeline","arguments":"{\"name\":\"orders\"}"}}]}}]}`)
	t.Setenv("LOCAL_KEY", "")
	p, err := New(&config.AssistantModel{Provider: "openai_compatible", Model: "qwen3", BaseURL: srv.URL + "/v1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := p.Chat(context.Background(), "sys", []Message{{Role: "user", Text: "why is orders slow"}}, testTools)
	if err != nil {
		t.Fatal(err)
	}
	if r.Done || len(r.Message.ToolCalls) != 1 || string(r.Message.ToolCalls[0].Args) != `{"name":"orders"}` {
		t.Fatalf("reply = %+v", r)
	}
	b := (*bodies)[0]
	if b["model"] != "qwen3" || (*headers)[0].Get("Authorization") != "" {
		t.Fatalf("request = %v, auth %q (a keyless local server should get no header)", b, (*headers)[0].Get("Authorization"))
	}
	msgs := b["messages"].([]any)
	if msgs[0].(map[string]any)["role"] != "system" {
		t.Fatal("system prompt should be the first message")
	}

	// Tool results go back as role: tool with the call id.
	history := []Message{{Role: "user", Text: "q"}, r.Message, {Role: "tool", ToolResults: []ToolResult{{ID: "c1", Name: "diagnose_pipeline", Content: "ok"}}}}
	_, _ = p.Chat(context.Background(), "sys", history, testTools)
	last := (*bodies)[1]["messages"].([]any)
	tool := last[len(last)-1].(map[string]any)
	if tool["role"] != "tool" || tool["tool_call_id"] != "c1" {
		t.Fatalf("tool result sent as %v", tool)
	}
}

func TestGeminiCleansSchemaAndMapsCalls(t *testing.T) {
	srv, bodies, headers := capture(t, `{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"diagnose_pipeline","args":{"name":"orders"}}}]}}]}`)
	t.Setenv("GEMINI_API_KEY", "g-key")
	p, err := New(&config.AssistantModel{Provider: "gemini", Model: "gemini-pro", BaseURL: srv.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := p.Chat(context.Background(), "sys", []Message{{Role: "user", Text: "hi"}}, testTools)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Message.ToolCalls) != 1 || r.Message.ToolCalls[0].Name != "diagnose_pipeline" {
		t.Fatalf("reply = %+v", r)
	}
	if (*headers)[0].Get("x-goog-api-key") != "g-key" {
		t.Fatal("key header missing")
	}
	decl := (*bodies)[0]["tools"].([]any)[0].(map[string]any)["functionDeclarations"].([]any)[0].(map[string]any)
	params := decl["parameters"].(map[string]any)
	if _, bad := params["additionalProperties"]; bad {
		t.Fatal("additionalProperties must be stripped for Gemini")
	}
	if _, bad := params["$schema"]; bad {
		t.Fatal("$schema must be stripped for Gemini")
	}
}

func TestAnthropicToolLoop(t *testing.T) {
	srv, bodies, headers := capture(t, `{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5","stop_reason":"tool_use","content":[{"type":"text","text":"Checking."},{"type":"tool_use","id":"tu_1","name":"diagnose_pipeline","input":{"name":"orders"}}],"usage":{"input_tokens":1,"output_tokens":1}}`)
	t.Setenv("ANTHROPIC_API_KEY", "a-key")
	p, err := New(&config.AssistantModel{Provider: "anthropic", Model: "claude-opus-5", BaseURL: srv.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := p.Chat(context.Background(), "sys", []Message{{Role: "user", Text: "why"}}, testTools)
	if err != nil {
		t.Fatal(err)
	}
	if r.Done || r.Message.Text != "Checking." || len(r.Message.ToolCalls) != 1 || r.Message.ToolCalls[0].ID != "tu_1" {
		t.Fatalf("reply = %+v", r)
	}
	if (*headers)[0].Get("X-Api-Key") != "a-key" {
		t.Fatal("api key header missing")
	}
	history := []Message{{Role: "user", Text: "why"}, r.Message, {Role: "tool", ToolResults: []ToolResult{{ID: "tu_1", Content: "fine"}}}}
	_, _ = p.Chat(context.Background(), "sys", history, testTools)
	sent, _ := json.Marshal((*bodies)[1]["messages"])
	if !strings.Contains(string(sent), `"tool_use_id":"tu_1"`) || !strings.Contains(string(sent), `"type":"tool_use"`) {
		t.Fatalf("the assistant turn and tool result must be replayed: %s", sent)
	}
}

func TestMissingKeyIsExplained(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	_, err := New(&config.AssistantModel{Provider: "openai", Model: "gpt"}, nil)
	if err == nil || !strings.Contains(err.Error(), "OPENAI_API_KEY") {
		t.Fatalf("err = %v", err)
	}
}
