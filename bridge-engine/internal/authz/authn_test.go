package authz

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"golang.org/x/crypto/bcrypt"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

func req(method, remote string, set func(*http.Request)) *http.Request {
	r := httptest.NewRequest(method, "/api/v1/pipelines", nil)
	r.RemoteAddr = remote
	if set != nil {
		set(r)
	}
	return r
}

func mustAuth(t *testing.T, a config.Auth, api *TokenStore) *Authenticator {
	t.Helper()
	if api == nil {
		api = &TokenStore{}
	}
	out, _, err := FromConfig(a, api, &TokenStore{})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func sign(t *testing.T, key any, alg jose.SignatureAlgorithm, kid string, claims map[string]any) string {
	t.Helper()
	opts := (&jose.SignerOptions{}).WithType("JWT")
	if kid != "" {
		opts = opts.WithHeader("kid", kid)
	}
	s, err := jose.NewSigner(jose.SigningKey{Algorithm: alg, Key: key}, opts)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := jwt.Signed(s).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestNothingConfiguredAllowsOnlyLocalhost(t *testing.T) {
	a := mustAuth(t, config.Auth{}, nil)
	if c, err := a.Authenticate(req("GET", "127.0.0.1:5", nil)); err != nil || c.Scope != ScopeAdmin {
		t.Fatalf("localhost: %+v %v", c, err)
	}
	if _, err := a.Authenticate(req("GET", "10.0.0.9:5", nil)); err != ErrNoCredentials {
		t.Fatalf("remote: %v", err)
	}
}

func TestAnonymousAndTokensTogether(t *testing.T) {
	a := mustAuth(t, config.Auth{Anonymous: "viewer"}, NewTokenStore("secret-admin", ScopeAdmin))
	c, err := a.Authenticate(req("GET", "10.0.0.9:5", nil))
	if err != nil || c.Scope != ScopeViewer || !c.Anonymous {
		t.Fatalf("anonymous: %+v %v", c, err)
	}
	c, err = a.Authenticate(req("GET", "10.0.0.9:5", func(r *http.Request) { r.Header.Set("Authorization", "Bearer secret-admin") }))
	if err != nil || c.Scope != ScopeAdmin {
		t.Fatalf("token: %+v %v", c, err)
	}
	if _, err := a.Authenticate(req("GET", "10.0.0.9:5", func(r *http.Request) { r.Header.Set("Authorization", "Bearer wrong") })); err == nil {
		t.Fatal("a wrong token must not fall back to anonymous")
	}
}

func TestAccountsSessionsAndBasic(t *testing.T) {
	hash, _ := bcrypt.GenerateFromPassword([]byte("correct horse"), bcrypt.MinCost)
	a := mustAuth(t, config.Auth{Basic: true, Users: []config.User{{Username: "alice", PasswordHash: string(hash), Scope: "operator"}}}, nil)
	if _, err := a.checkPassword(context.Background(), "alice", "nope"); err != ErrBadPassword {
		t.Fatalf("wrong password: %v", err)
	}
	if _, err := a.checkPassword(context.Background(), "mallory", "x"); err != ErrBadPassword {
		t.Fatalf("unknown user: %v", err)
	}
	scope, err := a.checkPassword(context.Background(), "alice", "correct horse")
	if err != nil || scope != ScopeOperator {
		t.Fatalf("login: %v %v", scope, err)
	}
	tok, _, err := a.issueSession("alice", scope)
	if err != nil {
		t.Fatal(err)
	}
	c, err := a.AuthenticateToken(context.Background(), tok)
	if err != nil || c.ID != "user:alice" || c.Scope != ScopeOperator {
		t.Fatalf("session: %+v %v", c, err)
	}
	cookie := func(method string, csrf bool) (Caller, error) {
		return a.Authenticate(req(method, "10.0.0.9:5", func(r *http.Request) {
			r.AddCookie(&http.Cookie{Name: SessionCookie, Value: tok})
			if csrf {
				r.Header.Set(CSRFHeader, "1")
			}
		}))
	}
	if _, err := cookie("GET", false); err != nil {
		t.Fatalf("cookie read: %v", err)
	}
	if _, err := cookie("POST", false); err != ErrCSRF {
		t.Fatalf("a cookie write without the CSRF header must fail, got %v", err)
	}
	if _, err := cookie("POST", true); err != nil {
		t.Fatalf("cookie write: %v", err)
	}
	c, err = a.Authenticate(req("GET", "10.0.0.9:5", func(r *http.Request) { r.SetBasicAuth("alice", "correct horse") }))
	if err != nil || c.Scope != ScopeOperator {
		t.Fatalf("basic: %+v %v", c, err)
	}
	a.SetUsers([]config.User{{Username: "alice", PasswordHash: string(hash), Scope: "viewer"}})
	if _, err := a.AuthenticateToken(context.Background(), tok); err == nil {
		t.Fatal("a session must end when the account's access changes")
	}
	a.SetUsers(nil)
	if _, err := a.AuthenticateToken(context.Background(), tok); err == nil {
		t.Fatal("a session must end when the account is deleted")
	}
}

func TestJWTWithASharedSecret(t *testing.T) {
	secret := strings.Repeat("s", 40)
	t.Setenv("TEST_JWT_SECRET", secret)
	a := mustAuth(t, config.Auth{JWT: &config.JWT{Issuer: "https://issuer.example", Audiences: []string{"ark"}, SecretEnv: "TEST_JWT_SECRET",
		RoleMap: config.RoleMap{RolesClaim: "roles", Admin: []string{"ops-admin"}}}}, nil)
	now := time.Now()
	good := sign(t, []byte(secret), jose.HS256, "", map[string]any{"iss": "https://issuer.example", "aud": "ark", "sub": "u1", "email": "u1@example.com", "roles": []string{"ops-admin"}, "exp": now.Add(time.Hour).Unix()})
	c, err := a.AuthenticateToken(context.Background(), good)
	if err != nil || c.Scope != ScopeAdmin || c.ID != "user:u1@example.com" {
		t.Fatalf("good: %+v %v", c, err)
	}
	for name, claims := range map[string]map[string]any{
		"expired":        {"iss": "https://issuer.example", "aud": "ark", "sub": "u1", "exp": now.Add(-time.Hour).Unix()},
		"other audience": {"iss": "https://issuer.example", "aud": "else", "sub": "u1", "exp": now.Add(time.Hour).Unix()},
		"other issuer":   {"iss": "https://evil.example", "aud": "ark", "sub": "u1", "exp": now.Add(time.Hour).Unix()},
	} {
		if _, err := a.AuthenticateToken(context.Background(), sign(t, []byte(secret), jose.HS256, "", claims)); err == nil {
			t.Errorf("%s token was accepted", name)
		}
	}
	forged := sign(t, []byte(strings.Repeat("x", 40)), jose.HS256, "", map[string]any{"iss": "https://issuer.example", "aud": "ark", "sub": "u1", "exp": now.Add(time.Hour).Unix()})
	if _, err := a.AuthenticateToken(context.Background(), forged); err == nil {
		t.Error("a token signed with another secret was accepted")
	}
}

// idp is a tiny OpenID provider: discovery and a JWKS with one RSA key.
func idp(t *testing.T) (*httptest.Server, *rsa.PrivateKey) {
	t.Helper()
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]any{"issuer": srv.URL, "jwks_uri": srv.URL + "/jwks", "authorization_endpoint": srv.URL + "/auth", "token_endpoint": srv.URL + "/token", "id_token_signing_alg_values_supported": []string{"RS256"}})
		case "/jwks":
			_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig"}}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, key
}

func TestOIDCAndJWKS(t *testing.T) {
	srv, key := idp(t)
	a := mustAuth(t, config.Auth{OIDC: &config.OIDC{Issuer: srv.URL, ClientID: "ark", RoleMap: config.RoleMap{Admin: []string{"ark-admins"}, Viewer: []string{"staff"}}}}, nil)
	claims := func(groups ...string) map[string]any {
		return map[string]any{"iss": srv.URL, "aud": "ark", "sub": "s1", "preferred_username": "alice", "groups": groups, "exp": time.Now().Add(time.Hour).Unix()}
	}
	c, err := a.AuthenticateToken(context.Background(), sign(t, key, jose.RS256, "k1", claims("ark-admins")))
	if err != nil || c.Scope != ScopeAdmin || c.ID != "user:alice" {
		t.Fatalf("admin: %+v %v", c, err)
	}
	if _, err := a.AuthenticateToken(context.Background(), sign(t, key, jose.RS256, "k1", claims("contractors"))); err != ErrNotAllowed {
		t.Fatalf("a person in no accepted group must be refused, got %v", err)
	}

	j := mustAuth(t, config.Auth{JWT: &config.JWT{JWKSURL: srv.URL + "/jwks", Issuer: srv.URL}}, nil)
	if c, err := j.AuthenticateToken(context.Background(), sign(t, key, jose.RS256, "k1", claims())); err != nil || c.Scope != ScopeViewer {
		t.Fatalf("jwks: %+v %v", c, err)
	}
}

func TestTrustedHeadersOnlyFromTheProxy(t *testing.T) {
	a := mustAuth(t, config.Auth{Header: &config.HeaderAuth{Preset: "oauth2-proxy", TrustedProxies: []string{"10.1.0.0/16"}, RoleMap: config.RoleMap{Operator: []string{"sre"}}}}, nil)
	set := func(r *http.Request) {
		r.Header.Set("X-Forwarded-Preferred-Username", "carol")
		r.Header.Set("X-Forwarded-Groups", "sre,staff")
	}
	c, err := a.Authenticate(req("GET", "10.1.2.3:5", set))
	if err != nil || c.Scope != ScopeOperator || c.ID != "user:carol" {
		t.Fatalf("from proxy: %+v %v", c, err)
	}
	if _, err := a.Authenticate(req("GET", "192.168.9.9:5", set)); err == nil {
		t.Fatal("headers from outside the trusted proxies must be ignored")
	}
}

func TestAzureEasyAuthRoles(t *testing.T) {
	principal := base64.StdEncoding.EncodeToString([]byte(`{"auth_typ":"aad","role_typ":"http://schemas.microsoft.com/ws/2008/06/identity/claims/role","claims":[{"typ":"http://schemas.microsoft.com/ws/2008/06/identity/claims/role","val":"ARK.Admin"},{"typ":"name","val":"Dan"}]}`))
	a := mustAuth(t, config.Auth{Header: &config.HeaderAuth{Preset: "azure-easy-auth", TrustedProxies: []string{"127.0.0.1"}, RoleMap: config.RoleMap{Admin: []string{"ARK.Admin"}}}}, nil)
	c, err := a.Authenticate(req("GET", "127.0.0.1:5", func(r *http.Request) {
		r.Header.Set("X-Ms-Client-Principal-Name", "dan@contoso.com")
		r.Header.Set("X-Ms-Client-Principal", principal)
	}))
	if err != nil || c.Scope != ScopeAdmin || c.ID != "user:dan@contoso.com" {
		t.Fatalf("%+v %v", c, err)
	}
}
