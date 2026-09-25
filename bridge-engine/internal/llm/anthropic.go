package llm

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

type anthropicProvider struct {
	client anthropic.Client
	model  string
}

func newAnthropic(m *config.AssistantModel, key string, hc *http.Client) *anthropicProvider {
	opts := []option.RequestOption{option.WithAPIKey(key), option.WithHTTPClient(hc)}
	if m.BaseURL != "" {
		opts = append(opts, option.WithBaseURL(m.BaseURL))
	}
	return &anthropicProvider{client: anthropic.NewClient(opts...), model: m.Model}
}

func (p *anthropicProvider) Chat(ctx context.Context, system string, history []Message, tools []Tool) (Reply, error) {
	params := anthropic.MessageNewParams{
		Model:     anthropic.Model(p.model),
		MaxTokens: 16000,
		System:    []anthropic.TextBlockParam{{Text: system}},
	}
	for _, t := range tools {
		props, _ := t.Schema["properties"].(map[string]any)
		schema := anthropic.ToolInputSchemaParam{Properties: props}
		if req, ok := t.Schema["required"].([]any); ok {
			for _, r := range req {
				if s, ok := r.(string); ok {
					schema.Required = append(schema.Required, s)
				}
			}
		}
		tool := anthropic.ToolParam{Name: t.Name, Description: anthropic.String(t.Description), InputSchema: schema}
		params.Tools = append(params.Tools, anthropic.ToolUnionParam{OfTool: &tool})
	}
	for _, m := range history {
		switch {
		case m.Role == "assistant" && m.raw != nil:
			if mp, ok := m.raw.(anthropic.MessageParam); ok {
				params.Messages = append(params.Messages, mp)
				continue
			}
			fallthrough
		case m.Role == "assistant":
			params.Messages = append(params.Messages, anthropic.NewAssistantMessage(anthropic.NewTextBlock(m.Text)))
		case m.Role == "tool":
			var blocks []anthropic.ContentBlockParamUnion
			for _, r := range m.ToolResults {
				blocks = append(blocks, anthropic.NewToolResultBlock(r.ID, r.Content, r.IsError))
			}
			params.Messages = append(params.Messages, anthropic.NewUserMessage(blocks...))
		default:
			params.Messages = append(params.Messages, anthropic.NewUserMessage(anthropic.NewTextBlock(m.Text)))
		}
	}

	resp, err := p.client.Messages.New(ctx, params)
	if err != nil {
		return Reply{}, err
	}
	out := Message{Role: "assistant", raw: resp.ToParam()}
	for _, block := range resp.Content {
		switch v := block.AsAny().(type) {
		case anthropic.TextBlock:
			out.Text += v.Text
		case anthropic.ToolUseBlock:
			out.ToolCalls = append(out.ToolCalls, ToolCall{ID: v.ID, Name: v.Name, Args: json.RawMessage(v.JSON.Input.Raw())})
		}
	}
	if resp.StopReason == anthropic.StopReasonRefusal && out.Text == "" {
		out.Text = "The model declined to answer this request."
	}
	return Reply{Message: out, Done: resp.StopReason != anthropic.StopReasonToolUse}, nil
}
