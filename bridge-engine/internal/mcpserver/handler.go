package mcpserver

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/raven-clown/ark/bridge-engine/internal/api"
)

func NewHTTPHandler(reg api.Registry, tokens *TokenStore, auditLog *slog.Logger) http.Handler {
	servers := map[Scope]*mcp.Server{
		ScopeViewer:   buildServer(ScopeViewer, reg, auditLog),
		ScopeOperator: buildServer(ScopeOperator, reg, auditLog),
		ScopeAdmin:    buildServer(ScopeAdmin, reg, auditLog),
	}

	mcpHandler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		scope, ok := authenticate(r, tokens)
		if !ok {
			return nil
		}
		return servers[scope]
	}, nil)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := authenticate(r, tokens); !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="ark-mcp"`)
			http.Error(w, "invalid or missing bearer token", http.StatusUnauthorized)
			return
		}
		mcpHandler.ServeHTTP(w, r)
	})
}

func authenticate(r *http.Request, tokens *TokenStore) (Scope, bool) {
	header := r.Header.Get("Authorization")
	token, found := strings.CutPrefix(header, "Bearer ")
	if !found || token == "" {
		return "", false
	}
	return tokens.Lookup(token)
}
