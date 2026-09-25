package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

type geminiProvider struct {
	url, key string
	hc       *http.Client
}

func newGemini(m *config.AssistantModel, key string, hc *http.Client) *geminiProvider {
	base := strings.TrimRight(m.BaseURL, "/")
	if base == "" {
		base = "https://generativelanguage.googleapis.com/v1beta"
	}
	return &geminiProvider{url: base + "/models/" + url.PathEscape(m.Model) + ":generateContent", key: key, hc: hc}
}

type gmPart struct {
	Text             string          `json:"text,omitempty"`
	FunctionCall     *gmFunctionCall `json:"functionCall,omitempty"`
	FunctionResponse *gmFunctionResp `json:"functionResponse,omitempty"`
}

type gmFunctionCall struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args,omitempty"`
}

type gmFunctionResp struct {
	Name     string         `json:"name"`
	Response map[string]any `json:"response"`
}

type gmContent struct {
	Role  string   `json:"role,omitempty"`
	Parts []gmPart `json:"parts"`
}

// geminiSchema keeps the part of JSON Schema Gemini accepts; it rejects
// keys such as additionalProperties and $schema that MCP schemas carry.
func geminiSchema(v any) any {
	switch s := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, val := range s {
			switch k {
			case "type", "description", "enum", "required", "nullable", "format":
				out[k] = val
			case "items":
				out[k] = geminiSchema(val)
			case "properties":
				props := map[string]any{}
				if m, ok := val.(map[string]any); ok {
					for name, p := range m {
						props[name] = geminiSchema(p)
					}
				}
				out[k] = props
			}
		}
		if t, ok := out["type"].([]any); ok && len(t) > 0 {
			out["type"] = t[0]
		}
		return out
	default:
		return v
	}
}

func (p *geminiProvider) Chat(ctx context.Context, system string, history []Message, tools []Tool) (Reply, error) {
	var contents []gmContent
	for _, m := range history {
		switch m.Role {
		case "assistant":
			c := gmContent{Role: "model"}
			if m.Text != "" {
				c.Parts = append(c.Parts, gmPart{Text: m.Text})
			}
			for _, call := range m.ToolCalls {
				c.Parts = append(c.Parts, gmPart{FunctionCall: &gmFunctionCall{Name: call.Name, Args: call.Args}})
			}
			contents = append(contents, c)
		case "tool":
			c := gmContent{Role: "user"}
			for _, r := range m.ToolResults {
				resp := map[string]any{"content": r.Content}
				if r.IsError {
					resp = map[string]any{"error": r.Content}
				}
				c.Parts = append(c.Parts, gmPart{FunctionResponse: &gmFunctionResp{Name: r.Name, Response: resp}})
			}
			contents = append(contents, c)
		default:
			contents = append(contents, gmContent{Role: "user", Parts: []gmPart{{Text: m.Text}}})
		}
	}
	body := map[string]any{
		"systemInstruction": gmContent{Parts: []gmPart{{Text: system}}},
		"contents":          contents,
	}
	if len(tools) > 0 {
		var decls []map[string]any
		for _, t := range tools {
			decls = append(decls, map[string]any{"name": t.Name, "description": t.Description, "parameters": geminiSchema(t.Schema)})
		}
		body["tools"] = []map[string]any{{"functionDeclarations": decls}}
	}

	var resp struct {
		Candidates []struct {
			Content      gmContent `json:"content"`
			FinishReason string    `json:"finishReason"`
		} `json:"candidates"`
	}
	if err := postJSON(ctx, p.hc, p.url, map[string]string{"x-goog-api-key": p.key}, body, &resp); err != nil {
		return Reply{}, err
	}
	if len(resp.Candidates) == 0 {
		return Reply{}, fmt.Errorf("the model returned no answer")
	}
	out := Message{Role: "assistant"}
	for i, part := range resp.Candidates[0].Content.Parts {
		if part.Text != "" {
			out.Text += part.Text
		}
		if part.FunctionCall != nil {
			args := part.FunctionCall.Args
			if len(args) == 0 {
				args = json.RawMessage("{}")
			}
			out.ToolCalls = append(out.ToolCalls, ToolCall{ID: fmt.Sprintf("call_%d_%s", i, part.FunctionCall.Name), Name: part.FunctionCall.Name, Args: args})
		}
	}
	return Reply{Message: out, Done: len(out.ToolCalls) == 0}, nil
}
