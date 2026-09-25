package mcpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/raven-clown/ark/bridge-engine/internal/authz"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/llm"
	"github.com/raven-clown/ark/bridge-engine/internal/tuning"
)

const (
	maxChatSession = 200
)

// understandPrompt is the first pass: work out what the person means, or
// ask them, before anything is looked up or changed.
const understandPrompt = `You are the first step of ARK's assistant. ARK is a Kafka callback bridge: pipelines consume a Kafka topic, call an HTTP endpoint per message, and produce the answer to another topic, with retries, dead-letter and reject topics, data rules and routing rules.

Read the conversation and the person's latest message. Work out precisely what they want, in plain words, naming pipelines, topics, numbers and time ranges they mean. A hint from ARK's own parser is included; use it, but trust the person's words over it.

If something essential is missing and can't be looked up by ARK's tools (for example the URL of a new endpoint, or which of several pipelines they mean when nothing narrows it down), ask one short question instead. Do not ask about things the tools can find out.

Answer with only a JSON object: {"request": "<what they want, restated>", "ask": "<one question, or empty>"}. Write both in the person's language.`

// ChatStep is one tool call the assistant made while answering.
type ChatStep struct {
	Tool  string          `json:"tool"`
	Args  json.RawMessage `json:"args,omitempty"`
	Error string          `json:"error,omitempty"`
}

type ChatOut struct {
	ConversationID string     `json:"conversation_id"`
	Answer         string     `json:"answer"`
	Understood     string     `json:"understood,omitempty"`
	AskedBack      bool       `json:"asked_back"`
	Steps          []ChatStep `json:"steps"`
	Model          string     `json:"model"`
	Scope          string     `json:"scope"`
	Project        string     `json:"project,omitempty"`
}

type chatSession struct {
	mu      sync.Mutex
	caller  string
	project string
	scope   Scope
	model   config.AssistantModel
	history []llm.Message
	cs      *mcp.ClientSession
	tools   []llm.Tool
	used    time.Time
}

type assistant struct {
	d           Deps
	mu          sync.Mutex
	sessions    map[string]*chatSession
	newProvider func(*config.AssistantModel) (llm.Provider, error)
}

func newAssistant(d Deps) *assistant {
	d.AllPipelines = false
	return &assistant{d: d, sessions: map[string]*chatSession{}, newProvider: func(m *config.AssistantModel) (llm.Provider, error) { return llm.New(m, nil) }}
}

func scopeOfCaller(s authz.Scope) Scope {
	switch s {
	case authz.ScopeAdmin:
		return ScopeAdmin
	case authz.ScopeOperator:
		return ScopeOperator
	}
	return ScopeViewer
}

func minScope(a, b Scope) Scope {
	rank := map[Scope]int{ScopeViewer: 1, ScopeOperator: 2, ScopeAdmin: 3}
	if rank[a] <= rank[b] {
		return a
	}
	return b
}

// modelFor is the project's model, or assistant.model.
func (a *assistant) modelFor(project string) (*config.AssistantModel, config.AIAccess, error) {
	access := config.AIAccessConfigure
	var m *config.AssistantModel
	if project != "" {
		found := false
		for _, p := range a.d.Config.Projects() {
			if p.Name == project {
				found, access, m = true, p.AIAccess, p.Assistant
			}
		}
		if !found {
			return nil, "", fmt.Errorf("project not found: %s", project)
		}
	}
	if m == nil {
		m = a.d.Config.DefaultModel()
	}
	if m == nil {
		return nil, access, fmt.Errorf("no AI model is configured: set assistant.model in the config, or an assistant on the project")
	}
	return m, access, nil
}

func (a *assistant) session(ctx context.Context, id string, caller authz.Caller, project string) (*chatSession, string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	for k, s := range a.sessions {
		if now.Sub(s.used) > tuning.AssistantSession() {
			_ = s.cs.Close()
			delete(a.sessions, k)
		}
	}
	if s, ok := a.sessions[id]; ok && id != "" {
		if s.caller != caller.ID || s.project != project {
			return nil, "", fmt.Errorf("that conversation belongs to someone else or another project; start a new one")
		}
		s.used = now
		return s, id, nil
	}

	m, access, err := a.modelFor(project)
	if err != nil {
		return nil, "", err
	}
	projectScope, ok := ScopeFor(access)
	if !ok {
		return nil, "", fmt.Errorf("project %s has ai_access: none", project)
	}
	scope := minScope(scopeOfCaller(caller.Scope), projectScope)

	d := a.d
	d.Project = project
	server := buildServer(scope, d, newConfirmations())
	ct, st := mcp.NewInMemoryTransports()
	if _, err := server.Connect(ctx, st, nil); err != nil {
		return nil, "", err
	}
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "ark-console", Version: a.d.Version}, nil).Connect(ctx, ct, nil)
	if err != nil {
		return nil, "", err
	}
	listed, err := cs.ListTools(ctx, nil)
	if err != nil {
		_ = cs.Close()
		return nil, "", err
	}
	s := &chatSession{caller: caller.ID, project: project, scope: scope, model: *m, cs: cs, used: now}
	for _, t := range listed.Tools {
		schema := map[string]any{}
		if b, err := json.Marshal(t.InputSchema); err == nil {
			_ = json.Unmarshal(b, &schema)
		}
		if len(schema) == 0 {
			schema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		s.tools = append(s.tools, llm.Tool{Name: t.Name, Description: t.Description, Schema: schema})
	}

	if len(a.sessions) >= maxChatSession {
		var oldest string
		for k, v := range a.sessions {
			if oldest == "" || v.used.Before(a.sessions[oldest].used) {
				oldest = k
			}
		}
		_ = a.sessions[oldest].cs.Close()
		delete(a.sessions, oldest)
	}
	buf := make([]byte, 12)
	_, _ = rand.Read(buf)
	id = hex.EncodeToString(buf)
	a.sessions[id] = s
	return s, id, nil
}

// callTool runs one MCP tool and returns its text for the model.
func (s *chatSession) callTool(ctx context.Context, name string, args json.RawMessage) (string, bool) {
	var in map[string]any
	if len(args) > 0 {
		_ = json.Unmarshal(args, &in)
	}
	if in == nil {
		in = map[string]any{}
	}
	res, err := s.cs.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: in})
	if err != nil {
		return err.Error(), true
	}
	var parts []string
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			parts = append(parts, tc.Text)
		}
	}
	text := strings.Join(parts, "\n")
	if len(text) > 60000 {
		text = text[:60000] + "\n(truncated)"
	}
	return text, res.IsError
}

func extractJSON(s string) map[string]string {
	start, end := strings.Index(s, "{"), strings.LastIndex(s, "}")
	out := map[string]string{}
	if start >= 0 && end > start {
		_ = json.Unmarshal([]byte(s[start:end+1]), &out)
	}
	return out
}

// Chat answers one message: first it works out what the person means
// (asking back when something essential is missing), then it looks things
// up and acts through the same MCP tools any agent uses, within the
// caller's scope and the project's ai_access.
func (a *assistant) Chat(ctx context.Context, caller authz.Caller, conversationID, project, message string) (ChatOut, error) {
	s, id, err := a.session(ctx, conversationID, caller, project)
	if err != nil {
		return ChatOut{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := ChatOut{ConversationID: id, Model: s.model.Provider + "/" + s.model.Model, Scope: string(s.scope), Project: project, Steps: []ChatStep{}}
	provider, err := a.newProvider(&s.model)
	if err != nil {
		return out, err
	}

	// Pass 1: understand.
	hint, _ := s.callTool(ctx, "interpret_request", json.RawMessage(mustJSON(map[string]string{"message": message})))
	var convo []llm.Message
	for _, m := range s.history {
		if (m.Role == "user" || m.Role == "assistant") && m.Text != "" && len(m.ToolCalls) == 0 {
			convo = append(convo, llm.Message{Role: m.Role, Text: m.Text})
		}
	}
	convo = append(convo, llm.Message{Role: "user", Text: message + "\n\n[ARK's parser hint]\n" + hint})
	first, err := provider.Chat(ctx, understandPrompt, convo, nil)
	if err != nil {
		return out, err
	}
	u := extractJSON(first.Message.Text)
	out.Understood = u["request"]
	s.history = append(s.history, llm.Message{Role: "user", Text: message})
	if q := strings.TrimSpace(u["ask"]); q != "" {
		out.Answer, out.AskedBack = q, true
		s.history = append(s.history, llm.Message{Role: "assistant", Text: q})
		return out, nil
	}

	// Pass 2: look up, act, answer.
	system := instructions + fmt.Sprintf("\n\nYou are answering inside the ARK console (scope: %s", s.scope)
	if project != "" {
		system += ", project: " + project
	}
	system += "). interpret_request has already run for this message."
	if out.Understood != "" {
		system += "\nWhat the person wants, restated: " + out.Understood
	}
	system += "\nThe person sees your answer as Markdown. They can't see tool results, so say what you found."

	steps := s.model.MaxSteps
	if steps <= 0 {
		steps = 8
	}
	for i := 0; i <= steps; i++ {
		tools := s.tools
		if i == steps {
			tools = nil // last round: answer with what was found
		}
		reply, err := provider.Chat(ctx, system, s.history, tools)
		if err != nil {
			return out, err
		}
		s.history = append(s.history, reply.Message)
		if reply.Done || len(reply.Message.ToolCalls) == 0 {
			out.Answer = strings.TrimSpace(reply.Message.Text)
			return out, nil
		}
		results := llm.Message{Role: "tool"}
		for _, call := range reply.Message.ToolCalls {
			text, isErr := s.callTool(ctx, call.Name, call.Args)
			step := ChatStep{Tool: call.Name, Args: call.Args}
			if isErr {
				step.Error = firstLine(text)
			}
			out.Steps = append(out.Steps, step)
			results.ToolResults = append(results.ToolResults, llm.ToolResult{ID: call.ID, Name: call.Name, Content: text, IsError: isErr})
		}
		s.history = append(s.history, results)
	}
	out.Answer = "I couldn't finish within the step limit. Ask again with a narrower question."
	return out, nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func (c *Console) assistantRoutes(mux *http.ServeMux) {
	a := newAssistant(c.d)
	c.chat = a
	mux.HandleFunc("POST /api/v1/assistant/chat", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			ConversationID string `json:"conversation_id"`
			Project        string `json:"project"`
			Message        string `json:"message"`
		}
		if !readJSON(w, r, &in) {
			return
		}
		if strings.TrimSpace(in.Message) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "message is required"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
		defer cancel()
		out, err := a.Chat(ctx, callerOf(r), in.ConversationID, in.Project, in.Message)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error(), "conversation_id": out.ConversationID})
			return
		}
		writeJSON(w, http.StatusOK, out)
	})
	mux.HandleFunc("GET /api/v1/assistant/status", func(w http.ResponseWriter, r *http.Request) {
		project := r.URL.Query().Get("project")
		m, access, err := a.modelFor(project)
		out := map[string]any{"project": project, "ready": err == nil && keySet(m), "ai_access": access}
		if m != nil {
			out["model"] = m.Provider + "/" + m.Model
			if !keySet(m) {
				env := m.APIKeyEnv
				if env == "" {
					env = map[string]string{"anthropic": "ANTHROPIC_API_KEY", "openai": "OPENAI_API_KEY", "gemini": "GEMINI_API_KEY"}[m.Provider]
				}
				out["hint"] = "set " + env + " on the ARK process"
			}
		}
		if err != nil {
			out["hint"] = err.Error()
		}
		writeJSON(w, http.StatusOK, out)
	})
}
