// Package llm talks to AI models from several providers through one small
// interface, so the console's assistant works with whichever model a
// project chooses: Anthropic, OpenAI, Google Gemini, or any API that
// speaks the OpenAI chat format.
package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

// Message is one turn of a conversation in a provider-neutral shape.
type Message struct {
	Role        string // user, assistant or tool
	Text        string
	ToolCalls   []ToolCall
	ToolResults []ToolResult
	// raw is the provider's own form of an assistant turn, replayed as-is
	// on the next request so provider-specific parts (such as thinking
	// blocks) survive the tool loop. Only the provider that made it reads it.
	raw any
}

type ToolCall struct {
	ID   string
	Name string
	Args json.RawMessage
}

type ToolResult struct {
	ID      string
	Name    string
	Content string
	IsError bool
}

type Tool struct {
	Name        string
	Description string
	Schema      map[string]any
}

// Reply is what a model said in one step.
type Reply struct {
	Message Message
	// Done is true when the model answered instead of asking for tools.
	Done bool
}

type Provider interface {
	Chat(ctx context.Context, system string, history []Message, tools []Tool) (Reply, error)
}

// New builds the provider m names. The API key comes from the environment
// variable m.APIKeyEnv, or the provider's usual variable when unset.
func New(m *config.AssistantModel, client *http.Client) (Provider, error) {
	if m == nil {
		return nil, fmt.Errorf("no assistant model is configured (set assistant.model, or assistant on the project)")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	}
	keyEnv := m.APIKeyEnv
	if keyEnv == "" {
		keyEnv = map[string]string{"anthropic": "ANTHROPIC_API_KEY", "openai": "OPENAI_API_KEY", "gemini": "GEMINI_API_KEY"}[m.Provider]
	}
	key := ""
	if keyEnv != "" {
		key = os.Getenv(keyEnv)
		if key == "" && m.Provider != "openai_compatible" {
			return nil, fmt.Errorf("the API key for %s is not set: set %s on the ARK process", m.Provider, keyEnv)
		}
	}
	switch m.Provider {
	case "anthropic":
		return newAnthropic(m, key, client), nil
	case "openai", "openai_compatible":
		return newOpenAI(m, key, client), nil
	case "gemini":
		return newGemini(m, key, client), nil
	}
	return nil, fmt.Errorf("unknown provider %q", m.Provider)
}
