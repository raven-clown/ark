package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

// openAIProvider speaks the OpenAI chat completions format, which OpenAI
// and most other providers and local servers accept.
type openAIProvider struct {
	url, key, model string
	hc              *http.Client
}

func newOpenAI(m *config.AssistantModel, key string, hc *http.Client) *openAIProvider {
	base := strings.TrimRight(m.BaseURL, "/")
	if base == "" {
		base = "https://api.openai.com/v1"
	}
	return &openAIProvider{url: base + "/chat/completions", key: key, model: m.Model, hc: hc}
}

type oaMessage struct {
	Role       string       `json:"role"`
	Content    *string      `json:"content"`
	ToolCalls  []oaToolCall `json:"tool_calls,omitempty"`
	ToolCallID string       `json:"tool_call_id,omitempty"`
}

type oaToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

func str(s string) *string { return &s }

func (p *openAIProvider) Chat(ctx context.Context, system string, history []Message, tools []Tool) (Reply, error) {
	msgs := []oaMessage{{Role: "system", Content: str(system)}}
	for _, m := range history {
		switch m.Role {
		case "assistant":
			om := oaMessage{Role: "assistant"}
			if m.Text != "" || len(m.ToolCalls) == 0 {
				om.Content = str(m.Text)
			}
			for _, c := range m.ToolCalls {
				tc := oaToolCall{ID: c.ID, Type: "function"}
				tc.Function.Name, tc.Function.Arguments = c.Name, string(c.Args)
				om.ToolCalls = append(om.ToolCalls, tc)
			}
			msgs = append(msgs, om)
		case "tool":
			for _, r := range m.ToolResults {
				content := r.Content
				if r.IsError {
					content = "ERROR: " + content
				}
				msgs = append(msgs, oaMessage{Role: "tool", Content: str(content), ToolCallID: r.ID})
			}
		default:
			msgs = append(msgs, oaMessage{Role: "user", Content: str(m.Text)})
		}
	}
	body := map[string]any{"model": p.model, "messages": msgs}
	if len(tools) > 0 {
		var ts []map[string]any
		for _, t := range tools {
			ts = append(ts, map[string]any{"type": "function", "function": map[string]any{"name": t.Name, "description": t.Description, "parameters": t.Schema}})
		}
		body["tools"] = ts
	}

	var resp struct {
		Choices []struct {
			Message      oaMessage `json:"message"`
			FinishReason string    `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := postJSON(ctx, p.hc, p.url, map[string]string{"Authorization": "Bearer " + p.key}, body, &resp); err != nil {
		return Reply{}, err
	}
	if len(resp.Choices) == 0 {
		return Reply{}, fmt.Errorf("the model returned no answer")
	}
	m := resp.Choices[0].Message
	out := Message{Role: "assistant"}
	if m.Content != nil {
		out.Text = *m.Content
	}
	for _, c := range m.ToolCalls {
		args := json.RawMessage(c.Function.Arguments)
		if !json.Valid(args) {
			args = json.RawMessage("{}")
		}
		out.ToolCalls = append(out.ToolCalls, ToolCall{ID: c.ID, Name: c.Function.Name, Args: args})
	}
	return Reply{Message: out, Done: len(out.ToolCalls) == 0}, nil
}

// postJSON sends body and decodes the answer into out, turning an error
// status into an error that carries the provider's own message.
func postJSON(ctx context.Context, hc *http.Client, url string, headers map[string]string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		if v != "" && !strings.HasSuffix(v, " ") {
			req.Header.Set(k, v)
		}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		msg := strings.TrimSpace(string(raw))
		if len(msg) > 400 {
			msg = msg[:400]
		}
		return fmt.Errorf("model API answered %d: %s", resp.StatusCode, msg)
	}
	return json.Unmarshal(raw, out)
}
