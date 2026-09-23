package mcpserver

import (
	"os"
	"strings"
)

type Scope string

const (
	ScopeViewer   Scope = "viewer"
	ScopeOperator Scope = "operator"
	ScopeAdmin    Scope = "admin"
)

type TokenStore struct {
	tokens map[string]Scope
}

func LoadTokenStoreFromEnv() *TokenStore {
	store := &TokenStore{tokens: make(map[string]Scope)}
	store.load(os.Getenv("ARK_MCP_VIEWER_TOKENS"), ScopeViewer)
	store.load(os.Getenv("ARK_MCP_OPERATOR_TOKENS"), ScopeOperator)
	store.load(os.Getenv("ARK_MCP_ADMIN_TOKENS"), ScopeAdmin)
	return store
}

func (t *TokenStore) load(csv string, scope Scope) {
	for _, tok := range strings.Split(csv, ",") {
		tok = strings.TrimSpace(tok)
		if tok != "" {
			t.tokens[tok] = scope
		}
	}
}

func (t *TokenStore) Lookup(token string) (Scope, bool) {
	scope, ok := t.tokens[token]
	return scope, ok
}

func (t *TokenStore) Enabled() bool {
	return len(t.tokens) > 0
}
