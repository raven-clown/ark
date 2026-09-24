package mcpserver

import "github.com/raven-clown/ark/bridge-engine/internal/authz"

type Scope = authz.Scope

const (
	ScopeViewer   = authz.ScopeViewer
	ScopeOperator = authz.ScopeOperator
	ScopeAdmin    = authz.ScopeAdmin
)

type TokenStore = authz.TokenStore

func LoadTokenStoreFromEnv() *TokenStore {
	return authz.LoadFromEnv("ARK_MCP")
}
