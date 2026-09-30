package mcpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/raven-clown/ark/bridge-engine/internal/authz"
)

func NewHTTPHandler(d Deps, authn *authz.Authenticator) http.Handler {
	return newEndpointHandler(d, authn, nil)
}

func newEndpointHandler(d Deps, authn *authz.Authenticator, tools []string) http.Handler {
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

	anon := make([]byte, 16)
	_, _ = rand.Read(anon)
	anonToken := "anonymous-" + hex.EncodeToString(anon)
	verify := func(ctx context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		if token == anonToken {
			return &auth.TokenInfo{Scopes: []string{string(authn.Anonymous)}, UserID: "anonymous"}, nil
		}
		c, err := authn.AuthenticateToken(ctx, token)
		if err != nil {
			return nil, auth.ErrInvalidToken
		}
		return &auth.TokenInfo{Scopes: []string{string(c.Scope)}, UserID: c.ID}, nil
	}
	guarded := auth.RequireBearerToken(verify, &auth.RequireBearerTokenOptions{AllowMissingExpiration: true})(mcpHandler)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, has := authz.BearerToken(r); !has && authn.Anonymous != "" {
			r.Header.Set("Authorization", "Bearer "+anonToken)
		}
		guarded.ServeHTTP(w, r)
	})
}
