package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/raven-clown/ark/bridge-engine/internal/authz"
)

func do(t *testing.T, h http.Handler, method, path, remote, token string) int {
	t.Helper()
	r := httptest.NewRequest(method, path, nil)
	r.RemoteAddr = remote
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Code
}

func TestNoTokensOnlyLoopbackServed(t *testing.T) {
	h := NewServer(NewRegistry(nil), nil, nil, nil)

	if got := do(t, h, "GET", "/api/v1/pipelines", "10.0.0.5:1234", ""); got != http.StatusForbidden {
		t.Errorf("remote request without tokens configured: got %d, want 403", got)
	}
	if got := do(t, h, "GET", "/api/v1/pipelines", "127.0.0.1:1234", ""); got != http.StatusOK {
		t.Errorf("loopback request: got %d, want 200", got)
	}
	if got := do(t, h, "GET", "/healthz", "10.0.0.5:1234", ""); got != http.StatusOK {
		t.Errorf("healthz must stay public: got %d", got)
	}
}

func TestTokenScopesEnforced(t *testing.T) {
	t.Setenv("TA_VIEWER_TOKENS", "view")
	t.Setenv("TA_OPERATOR_TOKENS", "op")
	h := NewServer(NewRegistry(nil), nil, nil, authz.LoadFromEnv("TA"))
	remote := "10.0.0.5:1234"

	cases := []struct {
		method, path, token string
		want                int
	}{
		{"GET", "/api/v1/pipelines", "", http.StatusUnauthorized},
		{"GET", "/api/v1/pipelines", "wrong", http.StatusUnauthorized},
		{"GET", "/api/v1/pipelines", "view", http.StatusOK},
		{"POST", "/api/v1/pipelines/x/pause", "view", http.StatusForbidden},
		{"POST", "/api/v1/pipelines/x/pause", "op", http.StatusNotFound},
		{"POST", "/api/v1/config/reload", "op", http.StatusForbidden},
		{"GET", "/api/v1/pipelines/x/dlq", "", http.StatusUnauthorized},
	}
	for _, c := range cases {
		if got := do(t, h, c.method, c.path, remote, c.token); got != c.want {
			t.Errorf("%s %s token=%q: got %d, want %d", c.method, c.path, c.token, got, c.want)
		}
	}
}

func TestConsoleRouteScopes(t *testing.T) {
	cases := []struct {
		method, path string
		want         authz.Scope
	}{
		{"GET", "/api/v1/config/pipelines", authz.ScopeViewer},
		{"POST", "/api/v1/config/validate", authz.ScopeViewer},
		{"POST", "/api/v1/pipelines/orders/test-message", authz.ScopeViewer},
		{"POST", "/api/v1/config/preview", authz.ScopeAdmin},
		{"POST", "/api/v1/config/confirm", authz.ScopeAdmin},
		{"POST", "/api/v1/config/reload", authz.ScopeAdmin},
		{"POST", "/api/v1/pipelines/orders/scale", authz.ScopeAdmin},
		{"POST", "/api/v1/pipelines/orders/restart", authz.ScopeOperator},
		{"DELETE", "/api/v1/anything-new", authz.ScopeOperator},
	}
	for _, c := range cases {
		r := httptest.NewRequest(c.method, c.path, nil)
		if got, _ := requiredScope(r); got != c.want {
			t.Errorf("%s %s needs %s, want %s", c.method, c.path, got, c.want)
		}
	}
}
