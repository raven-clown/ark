package mcpserver

import (
	"context"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/raven-clown/ark/bridge-engine/internal/authz"
)

// NewHTTPHandler serves MCP behind bearer-token auth. Each request's
// TokenInfo carries a UserID derived from its token, which the SDK checks
// on every request that reuses a session: a session opened with an
// operator token can't be driven by a request holding a different token,
// even if the session ID leaks.
func NewHTTPHandler(d Deps, tokens *TokenStore) http.Handler {
	return newEndpointHandler(d, tokens, nil)
}

// newEndpointHandler serves MCP for d with tokens, offering only tools
// (all tools when empty).
func newEndpointHandler(d Deps, tokens *TokenStore, tools []string) http.Handler {
	cf := newConfirmations()
	servers := map[Scope]*mcp.Server{
		ScopeViewer:   buildServer(ScopeViewer, d, cf),
		ScopeOperator: buildServer(ScopeOperator, d, cf),
		ScopeAdmin:    buildServer(ScopeAdmin, d, cf),
	}
	if drop := removedTools(tools); len(drop) > 0 {
		for _, s := range servers {
			s.RemoveTools(drop...)
		}
	}

	mcpHandler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		info := auth.TokenInfoFromContext(r.Context())
		if info == nil || len(info.Scopes) != 1 {
			return nil
		}
		return servers[Scope(info.Scopes[0])]
	}, nil)

	verify := func(_ context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		scope, ok := tokens.Lookup(token)
		if !ok {
			return nil, auth.ErrInvalidToken
		}
		return &auth.TokenInfo{Scopes: []string{string(scope)}, UserID: authz.UserID(token)}, nil
	}

	return auth.RequireBearerToken(verify, &auth.RequireBearerTokenOptions{AllowMissingExpiration: true})(mcpHandler)
}
