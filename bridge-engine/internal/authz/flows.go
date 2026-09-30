package authz

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

const flowCookie = "ark_flow"

var oidcPresets = map[string]config.OIDC{
	"azure":    {RoleMap: config.RoleMap{RolesClaim: "roles"}},
	"adfs":     {Scopes: []string{"allatclaims"}, RoleMap: config.RoleMap{RolesClaim: "group", UsernameClaim: "upn"}},
	"google":   {RoleMap: config.RoleMap{RolesClaim: "hd", UsernameClaim: "email"}},
	"keycloak": {RoleMap: config.RoleMap{RolesClaim: "groups"}},
	"okta":     {Scopes: []string{"groups"}, RoleMap: config.RoleMap{RolesClaim: "groups"}},
	"auth0":    {RoleMap: config.RoleMap{UsernameClaim: "email"}},
	"cognito":  {RoleMap: config.RoleMap{RolesClaim: "cognito:groups", UsernameClaim: "cognito:username"}},
}

func withPreset(c config.OIDC) config.OIDC {
	p := oidcPresets[c.Preset]
	if c.RolesClaim == "" {
		c.RolesClaim = p.RolesClaim
	}
	if c.UsernameClaim == "" {
		c.UsernameClaim = p.UsernameClaim
	}
	if len(c.Scopes) == 0 {
		c.Scopes = p.Scopes
	}
	return c
}

var oauth2Presets = map[string]config.OAuth2{
	"github": {AuthURL: "https://github.com/login/oauth/authorize", TokenURL: "https://github.com/login/oauth/access_token", // #nosec G101 -- public endpoint URL
		UserInfoURL: "https://api.github.com/user", GroupsURL: "https://api.github.com/user/teams", Scopes: []string{"read:user", "read:org"}},
	"gitlab": {AuthURL: "https://gitlab.com/oauth/authorize", TokenURL: "https://gitlab.com/oauth/token", // #nosec G101 -- public endpoint URL
		UserInfoURL: "https://gitlab.com/api/v4/user", GroupsURL: "https://gitlab.com/api/v4/groups?min_access_level=10", Scopes: []string{"read_user", "read_api"}},
	"bitbucket": {AuthURL: "https://bitbucket.org/site/oauth2/authorize", TokenURL: "https://bitbucket.org/site/oauth2/access_token", // #nosec G101 -- public endpoint URL
		UserInfoURL: "https://api.bitbucket.org/2.0/user", GroupsURL: "https://api.bitbucket.org/2.0/workspaces?role=member", Scopes: []string{"account"}},
}

type flowState struct {
	Provider string `json:"p"`
	State    string `json:"s"`
	Verifier string `json:"v,omitempty"`
	Nonce    string `json:"n,omitempty"`
	Request  string `json:"r,omitempty"`
	Expires  int64  `json:"e"`
}

func randomString() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func (a *Authenticator) sign(v []byte) string {
	m := hmac.New(sha256.New, a.sessionKey)
	m.Write(v)
	return hex.EncodeToString(m.Sum(nil))
}

func (a *Authenticator) saveFlow(w http.ResponseWriter, r *http.Request, s flowState) {
	s.Expires = time.Now().Add(10 * time.Minute).Unix()
	raw, _ := json.Marshal(s)
	v := base64.RawURLEncoding.EncodeToString(raw)
	setCookie(w, r, flowCookie, v+"."+a.sign([]byte(v)), "/auth/", 600, time.Time{})
}

func (a *Authenticator) loadFlow(w http.ResponseWriter, r *http.Request, provider string) (flowState, error) {
	var s flowState
	ck, err := r.Cookie(flowCookie)
	setCookie(w, r, flowCookie, "", "/auth/", -1, time.Time{})
	if err != nil {
		return s, errors.New("the sign-in expired or started in another browser; try again")
	}
	v, sig, ok := strings.Cut(ck.Value, ".")
	if !ok || !hmac.Equal([]byte(sig), []byte(a.sign([]byte(v)))) {
		return s, errors.New("the sign-in state was tampered with; try again")
	}
	raw, _ := base64.RawURLEncoding.DecodeString(v)
	if json.Unmarshal(raw, &s) != nil || s.Provider != provider || time.Now().Unix() > s.Expires {
		return s, errors.New("the sign-in expired; try again")
	}
	return s, nil
}

func setCookie(w http.ResponseWriter, r *http.Request, name, value, path string, maxAge int, expires time.Time) {
	c := &http.Cookie{Name: name, Value: value, Path: path, MaxAge: maxAge, Expires: expires, HttpOnly: true, SameSite: http.SameSiteLaxMode} // #nosec G124 nosemgrep: go.lang.security.audit.net.cookie-missing-secure.cookie-missing-secure -- Secure is set next from the request scheme, so plain-HTTP installs still sign in
	c.Secure = secure(r)
	http.SetCookie(w, c)
}

// redirect goes to a local path or the configured identity provider.
func redirect(w http.ResponseWriter, r *http.Request, to string) {
	http.Redirect(w, r, to, http.StatusFound) // #nosec G710 -- targets are "/" paths or URLs built from the provider in the config
}

func secure(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func PublicURL(r *http.Request) string {
	if u := strings.TrimRight(os.Getenv("ARK_PUBLIC_URL"), "/"); u != "" {
		return u
	}
	scheme := "http"
	if secure(r) {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

func (a *Authenticator) finish(w http.ResponseWriter, r *http.Request, username string, scope Scope) {
	token, exp, err := a.issueSession(username, scope)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	setCookie(w, r, SessionCookie, token, "/", 0, exp)
	redirect(w, r, "/")
}

func failed(w http.ResponseWriter, r *http.Request, err error) {
	redirect(w, r, "/?signin_error="+urlQueryEscape(err.Error()))
}

// Routes serves the browser sign-in flows under /auth/.
func (a *Authenticator) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /auth/logout", func(w http.ResponseWriter, r *http.Request) {
		setCookie(w, r, SessionCookie, "", "/", -1, time.Time{})
		redirect(w, r, "/")
	})
	if a.oidcCfg != nil {
		mux.HandleFunc("GET /auth/oidc/login", a.oidcLogin)
		mux.HandleFunc("GET /auth/oidc/callback", a.oidcCallback)
	}
	for _, o := range a.oauth2 {
		mux.HandleFunc("GET /auth/oauth2/"+o.Name+"/login", func(w http.ResponseWriter, r *http.Request) { a.oauth2Login(w, r, o) })
		mux.HandleFunc("GET /auth/oauth2/"+o.Name+"/callback", func(w http.ResponseWriter, r *http.Request) { a.oauth2Callback(w, r, o) })
	}
	if a.saml != nil {
		mux.HandleFunc("GET /auth/saml/metadata", a.samlMetadata)
		mux.HandleFunc("GET /auth/saml/login", a.samlLogin)
		mux.HandleFunc("POST /auth/saml/acs", a.samlACS)
	}
}

func (a *Authenticator) oidcConfig(ctx context.Context, r *http.Request) (*oauth2.Config, *oidc.Provider, error) {
	c := withPreset(*a.oidcCfg)
	p, err := oidc.NewProvider(ctx, c.Issuer)
	if err != nil {
		return nil, nil, fmt.Errorf("reaching the identity provider: %w", err)
	}
	return &oauth2.Config{
		ClientID: c.ClientID, ClientSecret: os.Getenv(c.ClientSecretEnv), Endpoint: p.Endpoint(),
		RedirectURL: PublicURL(r) + "/auth/oidc/callback", Scopes: append([]string{oidc.ScopeOpenID, "profile", "email"}, c.Scopes...),
	}, p, nil
}

func (a *Authenticator) oidcLogin(w http.ResponseWriter, r *http.Request) {
	conf, _, err := a.oidcConfig(r.Context(), r)
	if err != nil {
		failed(w, r, err)
		return
	}
	s := flowState{Provider: "oidc", State: randomString(), Verifier: oauth2.GenerateVerifier(), Nonce: randomString()}
	a.saveFlow(w, r, s)
	redirect(w, r, conf.AuthCodeURL(s.State, oauth2.S256ChallengeOption(s.Verifier), oidc.Nonce(s.Nonce)))
}

func (a *Authenticator) oidcCallback(w http.ResponseWriter, r *http.Request) {
	s, err := a.loadFlow(w, r, "oidc")
	if err != nil {
		failed(w, r, err)
		return
	}
	if e := r.URL.Query().Get("error"); e != "" {
		failed(w, r, fmt.Errorf("the identity provider said: %s %s", e, r.URL.Query().Get("error_description")))
		return
	}
	if r.URL.Query().Get("state") != s.State {
		failed(w, r, errors.New("the sign-in state didn't match; try again"))
		return
	}
	conf, p, err := a.oidcConfig(r.Context(), r)
	if err != nil {
		failed(w, r, err)
		return
	}
	tok, err := conf.Exchange(r.Context(), r.URL.Query().Get("code"), oauth2.VerifierOption(s.Verifier))
	if err != nil {
		failed(w, r, fmt.Errorf("exchanging the sign-in code: %w", err))
		return
	}
	raw, _ := tok.Extra("id_token").(string)
	idt, err := p.Verifier(&oidc.Config{ClientID: conf.ClientID}).Verify(r.Context(), raw)
	if err != nil || idt.Nonce != s.Nonce {
		failed(w, r, fmt.Errorf("the identity provider's token didn't check out: %v", err))
		return
	}
	var claims map[string]any
	_ = idt.Claims(&claims)
	c := withPreset(*a.oidcCfg)
	if info, err := p.UserInfo(r.Context(), oauth2.StaticTokenSource(tok)); err == nil {
		var extra map[string]any
		if info.Claims(&extra) == nil {
			for k, v := range extra {
				if _, has := claims[k]; !has {
					claims[k] = v
				}
			}
		}
	}
	scope, ok := scopeOf(c.RoleMap, claims)
	if !ok {
		failed(w, r, ErrNotAllowed)
		return
	}
	a.finish(w, r, usernameOf(c.RoleMap, claims, idt.Subject), scope)
}

func oauth2WithPreset(o config.OAuth2) config.OAuth2 {
	p := oauth2Presets[o.Preset]
	for _, f := range []struct {
		dst *string
		src string
	}{{&o.AuthURL, p.AuthURL}, {&o.TokenURL, p.TokenURL}, {&o.UserInfoURL, p.UserInfoURL}, {&o.GroupsURL, p.GroupsURL}} {
		if *f.dst == "" {
			*f.dst = f.src
		}
	}
	if len(o.Scopes) == 0 {
		o.Scopes = p.Scopes
	}
	return o
}

func (a *Authenticator) oauth2Config(r *http.Request, o config.OAuth2) *oauth2.Config {
	return &oauth2.Config{ClientID: o.ClientID, ClientSecret: os.Getenv(o.ClientSecretEnv), Scopes: o.Scopes,
		Endpoint:    oauth2.Endpoint{AuthURL: o.AuthURL, TokenURL: o.TokenURL},
		RedirectURL: PublicURL(r) + "/auth/oauth2/" + o.Name + "/callback"}
}

func (a *Authenticator) oauth2Login(w http.ResponseWriter, r *http.Request, o config.OAuth2) {
	s := flowState{Provider: "oauth2:" + o.Name, State: randomString(), Verifier: oauth2.GenerateVerifier()}
	a.saveFlow(w, r, s)
	redirect(w, r, a.oauth2Config(r, o).AuthCodeURL(s.State, oauth2.S256ChallengeOption(s.Verifier)))
}

func (a *Authenticator) oauth2Callback(w http.ResponseWriter, r *http.Request, o config.OAuth2) {
	s, err := a.loadFlow(w, r, "oauth2:"+o.Name)
	if err != nil || r.URL.Query().Get("state") != s.State {
		failed(w, r, errors.Join(err, errors.New("the sign-in state didn't match; try again")))
		return
	}
	conf := a.oauth2Config(r, o)
	tok, err := conf.Exchange(r.Context(), r.URL.Query().Get("code"), oauth2.VerifierOption(s.Verifier))
	if err != nil {
		failed(w, r, fmt.Errorf("exchanging the sign-in code: %w", err))
		return
	}
	client := conf.Client(r.Context(), tok)
	var user map[string]any
	if err := getJSON(client, o.UserInfoURL, &user); err != nil {
		failed(w, r, fmt.Errorf("reading who signed in: %w", err))
		return
	}
	name := ""
	for _, k := range []string{o.UsernameClaim, "login", "username", "preferred_username", "email", "name"} {
		if v, ok := user[k].(string); k != "" && ok && v != "" {
			name = v
			break
		}
	}
	var groups []string
	if o.GroupsURL != "" {
		groups = oauth2Groups(client, o.GroupsURL)
	}
	if o.RolesClaim != "" {
		groups = append(groups, claimValues(user[o.RolesClaim])...)
	}
	scope, ok := pickScope(groups, o.Admin, o.Operator, o.Viewer)
	if !ok || name == "" {
		failed(w, r, ErrNotAllowed)
		return
	}
	a.finish(w, r, name, scope)
}

func oauth2Groups(client *http.Client, url string) []string {
	var raw any
	if getJSON(client, url, &raw) != nil {
		return nil
	}
	if m, ok := raw.(map[string]any); ok {
		raw = m["values"]
	}
	list, _ := raw.([]any)
	var out []string
	for _, item := range list {
		switch v := item.(type) {
		case string:
			out = append(out, v)
		case map[string]any:
			if org, ok := v["organization"].(map[string]any); ok {
				out = append(out, fmt.Sprint(org["login"]), fmt.Sprint(org["login"])+"/"+fmt.Sprint(v["slug"]))
			}
			for _, k := range []string{"full_path", "slug", "name", "login"} {
				if s, ok := v[k].(string); ok {
					out = append(out, s)
				}
			}
		}
	}
	return out
}

func getJSON(client *http.Client, url string, v any) error {
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s answered %d", url, resp.StatusCode)
	}
	return json.Unmarshal(body, v)
}

func urlQueryEscape(s string) string {
	return strings.NewReplacer("%", "%25", "&", "%26", "#", "%23", "+", "%2B", " ", "+", "?", "%3F", "=", "%3D").Replace(s)
}
