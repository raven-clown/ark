// Package authz holds the bearer-token scopes shared by the REST API and
// the MCP server. The two surfaces load separate token sets, so a token
// issued to an AI agent for MCP can't be replayed against REST to get
// around per-pipeline mcp_access.
package authz

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net"
	"net/http"
	"os"
	"strings"
)

type Scope string

const (
	ScopeViewer   Scope = "viewer"
	ScopeOperator Scope = "operator"
	ScopeAdmin    Scope = "admin"
)

var rank = map[Scope]int{ScopeViewer: 1, ScopeOperator: 2, ScopeAdmin: 3}

// AtLeast reports whether s grants everything min grants.
func (s Scope) AtLeast(min Scope) bool {
	return rank[s] >= rank[min] && rank[s] > 0
}

type entry struct {
	token []byte
	scope Scope
}

type TokenStore struct {
	entries []entry
}

// LoadFromEnv reads <prefix>_VIEWER_TOKENS, <prefix>_OPERATOR_TOKENS and
// <prefix>_ADMIN_TOKENS, each a comma-separated list.
func LoadFromEnv(prefix string) *TokenStore {
	store := &TokenStore{}
	store.load(os.Getenv(prefix+"_VIEWER_TOKENS"), ScopeViewer)
	store.load(os.Getenv(prefix+"_OPERATOR_TOKENS"), ScopeOperator)
	store.load(os.Getenv(prefix+"_ADMIN_TOKENS"), ScopeAdmin)
	return store
}

func (t *TokenStore) load(csv string, scope Scope) {
	for _, tok := range strings.Split(csv, ",") {
		tok = strings.TrimSpace(tok)
		if tok != "" {
			t.entries = append(t.entries, entry{token: []byte(tok), scope: scope})
		}
	}
}

// Lookup compares against every configured token in constant time, so
// response timing doesn't reveal how much of a guess matched.
func (t *TokenStore) Lookup(token string) (Scope, bool) {
	var found Scope
	candidate := []byte(token)
	for _, e := range t.entries {
		if subtle.ConstantTimeCompare(e.token, candidate) == 1 {
			found = e.scope
		}
	}
	return found, found != ""
}

func (t *TokenStore) Enabled() bool {
	return len(t.entries) > 0
}

// BearerToken extracts the token from an Authorization: Bearer header.
func BearerToken(r *http.Request) (string, bool) {
	token, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found || token == "" {
		return "", false
	}
	return token, true
}

// UserID is a stable, non-reversible identifier for a token, used to bind
// an MCP session to the token that created it.
func UserID(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:16])
}

// IsLoopback reports whether the request came from this host.
func IsLoopback(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Caller is who made a request, as established by the API guard.
type Caller struct {
	ID    string
	Scope Scope
}

type callerKey struct{}

func WithCaller(ctx context.Context, c Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, c)
}

// CallerFrom returns the caller the guard stored on ctx.
func CallerFrom(ctx context.Context) (Caller, bool) {
	c, ok := ctx.Value(callerKey{}).(Caller)
	return c, ok
}
