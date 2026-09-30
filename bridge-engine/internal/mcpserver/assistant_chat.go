package mcpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

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
	// pending is a previewed config change waiting for the person's yes.
	pending *awaitingYes
}

type awaitingYes struct {
	tool, token string
}

// Short replies that mean yes or no to a previewed change, in the
// languages the assistant answers in.
var (
	yesWords = []string{"yes", "yep", "confirm", "apply", "go ahead", "do it", "ok", "okay", "sure", "ยืนยัน", "ใช่", "ตกลง", "โอเค", "ใช้ได้", "ทำเลย", "จัดไป", "เอาเลย", "确认", "確認", "是的", "好的", "可以", "应用", "套用"}
	noWords  = []string{"no", "cancel", "stop", "don't", "wait", "ไม่", "ยกเลิก", "อย่า", "รอก่อน", "不", "取消", "别", "別"}
)

// answersPreview reports whether message is a clear yes (true, true) or no
// (false, true) to a pending change; anything longer or unclear is neither.
func answersPreview(message string) (yes, clear bool) {
	m := strings.ToLower(strings.TrimSpace(message))
	if m == "" || len([]rune(m)) > 40 {
		return false, false
	}
	latin := " " + strings.Join(strings.FieldsFunc(m, func(r rune) bool { return !unicode.IsLetter(r) && r != '\'' }), " ") + " "
	has := func(w string) bool {
		if w[0] < utf8.RuneSelf {
			return strings.Contains(latin, " "+w+" ")
		}
		return strings.Contains(m, w)
	}
	for _, w := range noWords {
		if has(w) {
			return false, true
		}
	}
	for _, w := range yesWords {
		if has(w) {
			return true, true
		}
	}
	return false, false
}

// notePreview remembers or clears a pending change from a config tool's
// result.
func (s *chatSession) notePreview(tool, result string, isErr bool) {
	if tool != "create_pipeline" && tool != "apply_pipeline_config" || isErr {
		return
	}
	var r struct {
		State        string `json:"state"`
		ConfirmToken string `json:"confirm_token"`
	}
	if json.Unmarshal([]byte(result), &r) != nil {
		return
	}
	if r.State == "awaiting_confirmation" && r.ConfirmToken != "" {
		s.pending = &awaitingYes{tool: tool, token: r.ConfirmToken}
	} else {
		s.pending = nil
	}
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
func (a *assistant) Chat(ctx context.Context, caller authz.Caller, conversationID, project, message, reply string) (ChatOut, error) {
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
	lang := replyLanguage(reply, message)

	// A yes to a previewed change applies it here rather than trusting the
	// model to pass the confirm token back; a no drops it.
	if p := s.pending; p != nil {
		if yes, clear := answersPreview(message); clear {
			s.pending = nil
			if yes {
				return a.applyPending(ctx, provider, s, out, p, message, lang)
			}
		}
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
	understand := understandPrompt
	if lang != "" {
		understand += " Write both fields in " + languageNames[lang] + "."
	}
	first, err := provider.Chat(ctx, understand, convo, nil)
	if err != nil {
		return out, err
	}
	u := extractJSON(first.Message.Text)
	out.Understood = u["request"]
	s.history = append(s.history, llm.Message{Role: "user", Text: message})
	if q := strings.TrimSpace(u["ask"]); q != "" && parserWouldAsk(hint) {
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
	system += "\nThe person sees your answer as Markdown. They can't see tool results, so say what you found. They can't call tools either, so don't name them; say what you checked or did in plain words."
	system += "\nEvery time from the tools is ISO 8601 in " + a.d.loc().String() + ", the engine's timezone. Quote times exactly as given, in that zone and format; never convert them to another zone."
	if lang != "" {
		system += "\nWrite your whole answer in " + languageNames[lang] + ", even though the tool results are in English."
	}

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
		if reply.Done || len(reply.Message.ToolCalls) == 0 {
			out.Answer = polish(ctx, provider, lang, strings.TrimSpace(reply.Message.Text), s.tools)
			reply.Message.Text = out.Answer
			s.history = append(s.history, reply.Message)
			return out, nil
		}
		s.history = append(s.history, reply.Message)
		results := llm.Message{Role: "tool"}
		for _, call := range reply.Message.ToolCalls {
			text, isErr := s.callTool(ctx, call.Name, call.Args)
			s.notePreview(call.Name, text, isErr)
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

// applyPending confirms p and has the model tell the person how it went.
func (a *assistant) applyPending(ctx context.Context, provider llm.Provider, s *chatSession, out ChatOut, p *awaitingYes, message, lang string) (ChatOut, error) {
	args := json.RawMessage(mustJSON(map[string]string{"confirm_token": p.token}))
	text, isErr := s.callTool(ctx, p.tool, args)
	step := ChatStep{Tool: p.tool, Args: json.RawMessage(`{"confirm_token":"(from the preview)"}`)}
	if isErr {
		step.Error = firstLine(text)
	}
	out.Steps = append(out.Steps, step)
	prompt := "The person confirmed a previewed ARK config change and ARK sent it. In two sentences at most, tell them whether it was applied, using only this result, and if it failed, why and what to do."
	if lang != "" {
		prompt += " Write in " + languageNames[lang] + "."
	}
	res, err := provider.Chat(ctx, prompt, []llm.Message{{Role: "user", Text: text}}, nil)
	if err != nil {
		return out, err
	}
	out.Answer = polish(ctx, provider, lang, strings.TrimSpace(res.Message.Text), s.tools)
	s.history = append(s.history, llm.Message{Role: "user", Text: message}, llm.Message{Role: "assistant", Text: out.Answer})
	return out, nil
}

// parserWouldAsk reports whether interpret_request also found something the
// tools can't answer. Small models ask back about things a tool would find,
// so a question goes to the person only when the parser agrees one is needed.
func parserWouldAsk(hint string) bool {
	var h struct {
		AskTheUser []string `json:"ask_the_user"`
	}
	if json.Unmarshal([]byte(hint), &h) != nil {
		return true
	}
	return len(h.AskTheUser) > 0
}

// languageNames are the languages an answer can be asked for, by code: the
// console's own languages, plus the scripts scriptOf recognizes.
var languageNames = map[string]string{
	"en":      "English",
	"th":      "Thai",
	"zh-Hans": "Simplified Chinese",
	"zh-Hant": "Traditional Chinese",
	"zh":      "Chinese, in the same script (simplified or traditional) they used",
	"ja":      "Japanese",
	"ko":      "Korean",
}

// replyLanguage picks the language to answer in: the one the person chose in
// the console, or else the one their message is written in. It returns ""
// when neither says anything beyond plain English.
func replyLanguage(chosen, message string) string {
	if _, ok := languageNames[chosen]; ok {
		return chosen
	}
	return scriptOf(message)
}

// written reports whether text is in language code's script.
// written reports whether text reads as the language code: no letters from
// another language's script, and for languages not written in Latin letters,
// no English sentence among them. Names, code spans and URLs don't count.
func written(text, code string) bool {
	prose := codeOrURL.ReplaceAllString(text, " ")
	want := code
	switch code {
	case "en":
		want = ""
	case "zh-Hans", "zh-Hant":
		want = "zh"
	}
	counts := scriptCounts(prose)
	for script, n := range counts {
		if n > 0 && script != want {
			return false
		}
	}
	if want != "" && counts[want] == 0 {
		return false
	}
	return want == "" || longestLatinRun(prose) < 7
}

var codeOrURL = regexp.MustCompile("(?s)```.*?```|`[^`]*`|https?://\\S+")

func scriptCounts(s string) map[string]int {
	out := map[string]int{}
	for _, r := range s {
		switch {
		case unicode.Is(unicode.Thai, r):
			out["th"]++
		case unicode.In(r, unicode.Hiragana, unicode.Katakana):
			out["ja"]++
		case unicode.Is(unicode.Hangul, r):
			out["ko"]++
		case unicode.Is(unicode.Han, r):
			out["zh"]++
		}
	}
	return out
}

// longestLatinRun is the most English words in a row, a sign of a sentence
// left untranslated rather than a product name or a field.
func longestLatinRun(s string) int {
	best, run := 0, 0
	for _, w := range strings.Fields(s) {
		w = strings.Trim(w, ".,:;!?()[]*\"'“”-—")
		latin := w != ""
		for _, r := range w {
			if !(r < utf8.RuneSelf && (unicode.IsLetter(r) || r == '\'' || r == '-')) {
				latin = false
				break
			}
		}
		if latin {
			run++
			best = max(best, run)
		} else if w != "" {
			run = 0
		}
	}
	return best
}

// mentionsTools lists the internal tool names an answer mentions; the
// person can't call them, so they only confuse.
func mentionsTools(answer string, tools []llm.Tool) []string {
	var out []string
	for _, t := range tools {
		if strings.Contains(answer, t.Name) {
			out = append(out, t.Name)
		}
	}
	return out
}

// polish asks the model once to fix an answer that drifted into another
// language or names internal tools, keeping the original if that fails.
func polish(ctx context.Context, provider llm.Provider, code, answer string, tools []llm.Tool) string {
	named := mentionsTools(answer, tools)
	if (code == "" || written(answer, code)) && len(named) == 0 {
		return answer
	}
	prompt := "Rewrite the user's text"
	if code != "" {
		prompt += " entirely in " + languageNames[code] + ", leaving no sentence in another language"
	}
	prompt += ". Keep every name, number, time, unit, topic, URL, Markdown mark and code span exactly as it is."
	if len(named) > 0 {
		prompt += " These are internal tool names the reader can't use: " + strings.Join(named, ", ") + ". Replace each mention with what it does in plain words, or drop it."
	}
	prompt += " Reply with only the rewritten text."
	r, err := provider.Chat(ctx, prompt, []llm.Message{{Role: "user", Text: answer}}, nil)
	if err != nil {
		return answer
	}
	t := strings.TrimSpace(r.Message.Text)
	if t == "" || (code != "" && !written(t, code) && written(answer, code)) {
		return answer
	}
	return t
}

// rewriteIn is polish for language only.
func rewriteIn(ctx context.Context, provider llm.Provider, code, answer string) string {
	return polish(ctx, provider, code, answer, nil)
}

// scriptOf names the language of a message when its script makes it plain:
// th, ja, ko or zh, and "" for Latin script. Small models tend to answer in
// English after reading English tool results unless they're told.
func scriptOf(s string) string {
	var thai, kana, hangul, han int
	for _, r := range s {
		switch {
		case unicode.Is(unicode.Thai, r):
			thai++
		case unicode.In(r, unicode.Hiragana, unicode.Katakana):
			kana++
		case unicode.Is(unicode.Hangul, r):
			hangul++
		case unicode.Is(unicode.Han, r):
			han++
		}
	}
	switch {
	case thai > 0:
		return "th"
	case kana > 0:
		return "ja"
	case hangul > 0:
		return "ko"
	case han > 0:
		return "zh"
	}
	return ""
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
			// Reply is the language to answer in (en, th, zh-Hans, zh-Hant);
			// empty follows the language of the message.
			Reply string `json:"reply_language"`
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
		answer, err := a.Chat(ctx, callerOf(r), in.ConversationID, in.Project, in.Message, in.Reply)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errBody(err))
			return
		}
		writeJSON(w, http.StatusOK, answer)
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
