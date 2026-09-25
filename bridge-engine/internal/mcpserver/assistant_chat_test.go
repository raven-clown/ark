package mcpserver

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
	"github.com/raven-clown/ark/bridge-engine/internal/authz"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/llm"
)

// scripted plays back replies in order and records what it was sent.
type scripted struct {
	replies []llm.Reply
	systems []string
	tools   [][]string
	history [][]llm.Message
}

func (s *scripted) Chat(_ context.Context, system string, history []llm.Message, tools []llm.Tool) (llm.Reply, error) {
	s.systems = append(s.systems, system)
	var names []string
	for _, t := range tools {
		names = append(names, t.Name)
	}
	s.tools = append(s.tools, names)
	s.history = append(s.history, append([]llm.Message(nil), history...))
	r := s.replies[0]
	s.replies = s.replies[1:]
	return r, nil
}

func text(t string) llm.Reply {
	return llm.Reply{Done: true, Message: llm.Message{Role: "assistant", Text: t}}
}

func chatFixture(t *testing.T, fake *scripted) *assistant {
	t.Helper()
	orders := mustPipeline(t, pipelineYAML("orders", "orders.raw", "orders.done"))
	orders.MCPAccess, orders.Project = config.MCPAccessReadWrite, "commerce"
	src := &memSource{p: []config.Pipeline{orders}, projects: []config.Project{{Name: "commerce", AIAccess: config.AIAccessReadOnly,
		Assistant: &config.AssistantModel{Provider: "openai_compatible", Model: "m", BaseURL: "http://x"}}}}
	a := newAssistant(Deps{Registry: api.NewRegistry(nil), Config: src})
	a.newProvider = func(*config.AssistantModel) (llm.Provider, error) { return fake, nil }
	return a
}

func TestAssistantUnderstandsThenUsesTools(t *testing.T) {
	fake := &scripted{replies: []llm.Reply{
		text(`{"request": "Diagnose the orders pipeline", "ask": ""}`),
		{Message: llm.Message{Role: "assistant", ToolCalls: []llm.ToolCall{{ID: "1", Name: "diagnose_pipeline", Args: json.RawMessage(`{"name":"orders"}`)}}}},
		text("orders has no running workers on this node."),
	}}
	a := chatFixture(t, fake)
	out, err := a.Chat(context.Background(), authz.Caller{ID: "alice", Scope: authz.ScopeAdmin}, "", "commerce", "ทำไม orders ช้า")
	if err != nil {
		t.Fatal(err)
	}
	if out.Answer != "orders has no running workers on this node." || out.Understood != "Diagnose the orders pipeline" || out.AskedBack {
		t.Fatalf("out = %+v", out)
	}
	if len(out.Steps) != 1 || out.Steps[0].Tool != "diagnose_pipeline" || out.Steps[0].Error != "" {
		t.Fatalf("steps = %+v", out.Steps)
	}
	// Pass 1 runs without tools and sees the parser's hint.
	if len(fake.tools[0]) != 0 || !strings.Contains(fake.history[0][0].Text, "parser hint") {
		t.Fatal("the understanding pass should have no tools and include the hint")
	}
	// The tool result reached the model on the next step.
	last := fake.history[2]
	if last[len(last)-1].Role != "tool" || !strings.Contains(last[len(last)-1].ToolResults[0].Content, "orders") {
		t.Fatalf("tool result not passed back: %+v", last[len(last)-1])
	}
	if !strings.Contains(fake.systems[1], "Diagnose the orders pipeline") {
		t.Fatal("the restated request should guide the second pass")
	}
}

func TestAssistantAsksBack(t *testing.T) {
	fake := &scripted{replies: []llm.Reply{text(`{"request": "", "ask": "Which URL should it call?"}`)}}
	a := chatFixture(t, fake)
	out, err := a.Chat(context.Background(), authz.Caller{ID: "alice", Scope: authz.ScopeAdmin}, "", "commerce", "create a pipeline for signups")
	if err != nil {
		t.Fatal(err)
	}
	if !out.AskedBack || out.Answer != "Which URL should it call?" || len(fake.systems) != 1 {
		t.Fatalf("out = %+v, calls %d", out, len(fake.systems))
	}
}

func TestAssistantScopeIsTheLowerOfCallerAndProject(t *testing.T) {
	fake := &scripted{replies: []llm.Reply{text(`{"request":"x","ask":""}`), text("ok")}}
	a := chatFixture(t, fake)
	out, err := a.Chat(context.Background(), authz.Caller{ID: "alice", Scope: authz.ScopeAdmin}, "", "commerce", "pause orders")
	if err != nil {
		t.Fatal(err)
	}
	if out.Scope != string(ScopeViewer) {
		t.Fatalf("an admin caller in a read_only project should get viewer, got %s", out.Scope)
	}
	for _, name := range fake.tools[1] {
		if name == "pause_pipeline" || name == "create_pipeline" {
			t.Fatalf("write tool %s offered in a read_only project", name)
		}
	}
}

func TestAssistantConversationIsBoundToCaller(t *testing.T) {
	fake := &scripted{replies: []llm.Reply{text(`{"request":"x","ask":""}`), text("ok")}}
	a := chatFixture(t, fake)
	out, err := a.Chat(context.Background(), authz.Caller{ID: "alice", Scope: authz.ScopeViewer}, "", "commerce", "hi")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Chat(context.Background(), authz.Caller{ID: "mallory", Scope: authz.ScopeViewer}, out.ConversationID, "commerce", "hi"); err == nil {
		t.Fatal("another caller continued alice's conversation")
	}
}
