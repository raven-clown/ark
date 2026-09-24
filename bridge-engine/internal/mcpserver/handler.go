package mcpserver

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
	"github.com/raven-clown/ark/bridge-engine/internal/authz"
)

// NewHTTPHandler serves MCP behind bearer-token auth. Each request's
// TokenInfo carries a UserID derived from its token, which the SDK checks
// on every request that reuses a session: a session opened with an
// operator token can't be driven by a request holding a different token,
// even if the session ID leaks.
func NewHTTPHandler(reg api.Registry, tokens *TokenStore, auditLog *slog.Logger) http.Handler {
	servers := map[Scope]*mcp.Server{
		ScopeViewer:   buildServer(ScopeViewer, reg, auditLog),
		ScopeOperator: buildServer(ScopeOperator, reg, auditLog),
		ScopeAdmin:    buildServer(ScopeAdmin, reg, auditLog),
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
