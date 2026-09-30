package authz

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"golang.org/x/crypto/bcrypt"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

var (
	ErrNoCredentials = errors.New("no credentials; send a bearer token or sign in")
	ErrBadToken      = errors.New("invalid or expired bearer token")
	ErrNotAllowed    = errors.New("signed in, but not in any group or role ARK accepts")
	ErrBadPassword   = errors.New("wrong username or password")
)

const sessionIssuer = "ark"

var dummyHash = sync.OnceValue(func() []byte {
	h, _ := bcrypt.GenerateFromPassword([]byte(randomString()), 12)
	return h
})

type Authenticator struct {
	Tokens    *TokenStore
	Anonymous Scope

	oidc       *jwtVerifier
	jwt        *jwtVerifier
	session    *jwtVerifier
	sessionKey []byte
	sessionTTL time.Duration
	users      *userList
	ldap       *config.LDAP
	oidcCfg    *config.OIDC
	basic      bool
	header     *headerAuth
	cert       *config.RoleMap
	oauth2     []config.OAuth2
	saml       *samlAuth
	accounts   bool
}

type userList struct {
	mu   sync.RWMutex
	list []config.User
}

func (u *userList) get() []config.User {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.list
}

func (a *Authenticator) SetUsers(users []config.User) {
	a.users.mu.Lock()
	a.users.list = users
	a.users.mu.Unlock()
}

func FromConfig(a config.Auth, apiTokens, mcpTokens *TokenStore) (api, mcp *Authenticator, err error) {
	shared := Authenticator{users: &userList{list: a.Users}, ldap: a.LDAP, oidcCfg: a.OIDC, sessionTTL: 12 * time.Hour}
	if a.SessionHours > 0 {
		shared.sessionTTL = time.Duration(a.SessionHours) * time.Hour
	}
	if a.OIDC != nil {
		shared.oidc = newOIDCVerifier(*a.OIDC)
	}
	if a.JWT != nil {
		if shared.jwt, err = newJWTVerifier(*a.JWT); err != nil {
			return nil, nil, err
		}
	}
	if a.Header != nil {
		if shared.header, err = newHeaderAuth(*a.Header); err != nil {
			return nil, nil, err
		}
	}
	if a.ClientCert != nil {
		shared.cert = &a.ClientCert.RoleMap
	}
	shared.basic, shared.accounts = a.Basic, a.Accounts
	for _, o := range a.OAuth2 {
		o = oauth2WithPreset(o)
		if o.Name == "" || o.AuthURL == "" || o.TokenURL == "" || o.UserInfoURL == "" {
			return nil, nil, fmt.Errorf("auth.oauth2 %q needs a name and a preset (github, gitlab, bitbucket) or auth_url, token_url and userinfo_url", o.Name)
		}
		shared.oauth2 = append(shared.oauth2, o)
	}
	if a.SAML != nil {
		if shared.saml, err = newSAML(*a.SAML); err != nil {
			return nil, nil, err
		}
	}
	browser := a.OIDC != nil || a.SAML != nil || len(a.OAuth2) > 0
	if a.Accounts || len(a.Users) > 0 || a.LDAP != nil || browser {
		shared.sessionKey = []byte(os.Getenv("ARK_SESSION_SECRET"))
		if len(shared.sessionKey) < 32 {
			shared.sessionKey = make([]byte, 32)
			_, _ = rand.Read(shared.sessionKey)
		}
		shared.session = &jwtVerifier{issuer: sessionIssuer, audiences: []string{sessionIssuer}, hmac: shared.sessionKey,
			roles: config.RoleMap{RolesClaim: "ark_scope", Admin: []string{"admin"}, Operator: []string{"operator"}, Viewer: []string{"viewer"}}}
	}
	api, mcp = new(Authenticator), new(Authenticator)
	*api, *mcp = shared, shared
	api.Tokens, api.Anonymous = apiTokens, Scope(a.Anonymous)
	mcp.Tokens, mcp.Anonymous = mcpTokens, Scope(a.MCPAnonymous)
	return api, mcp, nil
}

func (a *Authenticator) tokens() *TokenStore {
	if a.Tokens == nil {
		return &TokenStore{}
	}
	return a.Tokens
}

func (a *Authenticator) Configured() bool {
	return a.tokens().Enabled() || a.oidc != nil || a.jwt != nil || a.session != nil || a.header != nil || a.cert != nil || a.Anonymous != ""
}

const (
	SessionCookie = "ark_session"
	CSRFHeader    = "X-Ark-Csrf"
)

var ErrCSRF = errors.New("a change made with the session cookie needs the " + CSRFHeader + " header")

func (a *Authenticator) Authenticate(r *http.Request) (Caller, error) {
	if token, ok := BearerToken(r); ok {
		return a.AuthenticateToken(r.Context(), token)
	}
	if user, pass, ok := r.BasicAuth(); ok && a.basic {
		scope, err := a.checkPassword(r.Context(), user, pass)
		if err != nil {
			return Caller{}, err
		}
		return Caller{ID: "user:" + user, Scope: scope}, nil
	}
	if c, ok, err := a.fromCert(r); ok || err != nil {
		return c, err
	}
	if c, ok, err := a.fromHeader(r); ok || err != nil {
		return c, err
	}
	if ck, err := r.Cookie(SessionCookie); err == nil && a.session != nil {
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Header.Get(CSRFHeader) == "" {
			return Caller{}, ErrCSRF
		}
		return a.AuthenticateToken(r.Context(), ck.Value)
	}
	switch {
	case a.Anonymous != "":
		return Caller{ID: "anonymous", Scope: a.Anonymous, Anonymous: true}, nil
	case !a.Configured() && IsLoopback(r):
		return Caller{ID: "localhost", Scope: ScopeAdmin}, nil
	}
	return Caller{}, ErrNoCredentials
}

func (a *Authenticator) AuthenticateToken(ctx context.Context, token string) (Caller, error) {
	if scope, known := a.tokens().Lookup(token); known {
		return Caller{ID: UserID(token), Scope: scope}, nil
	}
	err := ErrBadToken
	for _, v := range []*jwtVerifier{a.session, a.oidc, a.jwt} {
		if v == nil {
			continue
		}
		c, verr := v.verify(ctx, token)
		if verr == nil && v == a.session {
			return a.stillValid(c)
		}
		if verr == nil || errors.Is(verr, ErrNotAllowed) {
			return c, verr
		}
		err = verr
	}
	return Caller{}, err
}

func (a *Authenticator) stillValid(c Caller) (Caller, error) {
	name := strings.TrimPrefix(c.ID, "user:")
	for _, u := range a.users.get() {
		if u.Username == name {
			if Scope(u.Scope) != c.Scope {
				return Caller{}, fmt.Errorf("%w: this account's access changed; sign in again", ErrBadToken)
			}
			return c, nil
		}
	}
	if a.ldap != nil {
		return c, nil
	}
	return Caller{}, fmt.Errorf("%w: this account no longer exists", ErrBadToken)
}

type authInfo struct {
	Tokens    bool           `json:"tokens"`
	Password  bool           `json:"password"`
	Basic     bool           `json:"basic"`
	JWT       bool           `json:"jwt"`
	Header    bool           `json:"header"`
	Cert      bool           `json:"client_cert"`
	Anonymous string         `json:"anonymous,omitempty"`
	Browser   []browserLogin `json:"sign_in_with"`
}

type browserLogin struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
	URL  string `json:"url"`
}

// InfoHandler tells the sign-in page which methods are on; it's public.
func (a *Authenticator) InfoHandler(w http.ResponseWriter, _ *http.Request) {
	out := authInfo{Tokens: a.tokens().Enabled(), Password: a.ldap != nil || len(a.users.get()) > 0 || a.accounts, Basic: a.basic,
		JWT: a.jwt != nil, Header: a.header != nil, Cert: a.cert != nil, Anonymous: string(a.Anonymous), Browser: []browserLogin{}}
	if o := a.oidcCfg; o != nil {
		out.Browser = append(out.Browser, browserLogin{"oidc", cmpOr(o.Preset, "sso"), "/auth/oidc/login"})
	}
	if a.saml != nil {
		out.Browser = append(out.Browser, browserLogin{"saml", "saml", "/auth/saml/login"})
	}
	for _, o := range a.oauth2 {
		out.Browser = append(out.Browser, browserLogin{"oauth2", cmpOr(o.Preset, o.Name), "/auth/oauth2/" + o.Name + "/login"})
	}
	writeJSON(w, http.StatusOK, out)
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (a *Authenticator) LoginHandler(w http.ResponseWriter, r *http.Request) {
	if a.session == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "password sign-in isn't configured (auth.users or auth.ldap)"})
		return
	}
	var in struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&in); err != nil || in.Username == "" || in.Password == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "send {\"username\": ..., \"password\": ...}"})
		return
	}
	scope, err := a.checkPassword(r.Context(), in.Username, in.Password)
	if err != nil {
		time.Sleep(700 * time.Millisecond)
		status := http.StatusUnauthorized
		if errors.Is(err, ErrNotAllowed) {
			status = http.StatusForbidden
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	token, exp, err := a.issueSession(in.Username, scope)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	setCookie(w, r, SessionCookie, token, "/", 0, exp)
	writeJSON(w, http.StatusOK, map[string]any{"token": token, "expires_at": exp.UTC().Format(time.RFC3339), "user": in.Username, "scope": scope})
}

func (a *Authenticator) checkPassword(ctx context.Context, username, password string) (Scope, error) {
	for _, u := range a.users.get() {
		if u.Username == username {
			if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)) != nil {
				return "", ErrBadPassword
			}
			return Scope(u.Scope), nil
		}
	}
	if a.ldap != nil {
		return ldapLogin(ctx, *a.ldap, username, password)
	}
	// Spend the same time as a real check so timing doesn't reveal users.
	_ = bcrypt.CompareHashAndPassword(dummyHash(), []byte(password))
	return "", ErrBadPassword
}

func (a *Authenticator) issueSession(username string, scope Scope) (string, time.Time, error) {
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.HS256, Key: a.sessionKey}, (&jose.SignerOptions{}).WithType("JWT"))
	if err != nil {
		return "", time.Time{}, err
	}
	now := time.Now()
	exp := now.Add(a.sessionTTL)
	claims := struct {
		jwt.Claims
		Scope string `json:"ark_scope"`
		Name  string `json:"preferred_username"`
	}{jwt.Claims{Issuer: sessionIssuer, Audience: jwt.Audience{sessionIssuer}, Subject: username, IssuedAt: jwt.NewNumericDate(now), Expiry: jwt.NewNumericDate(exp)}, string(scope), username}
	token, err := jwt.Signed(signer).Claims(claims).Serialize()
	return token, exp, err
}

// jwtVerifier checks signed tokens from one issuer and maps their claims.
type jwtVerifier struct {
	roles     config.RoleMap
	audiences []string
	issuer    string
	hmac      []byte
	mu        sync.Mutex
	verifier  *oidc.IDTokenVerifier
	lastTry   time.Time
	discover  func(context.Context) (*oidc.IDTokenVerifier, error)
}

func newOIDCVerifier(c config.OIDC) *jwtVerifier {
	return &jwtVerifier{roles: c.RoleMap, audiences: append([]string{c.ClientID}, c.Audiences...), discover: func(ctx context.Context) (*oidc.IDTokenVerifier, error) {
		p, err := oidc.NewProvider(ctx, c.Issuer)
		if err != nil {
			return nil, fmt.Errorf("reaching the identity provider %s: %w", c.Issuer, err)
		}
		return p.Verifier(&oidc.Config{SkipClientIDCheck: true}), nil
	}}
}

func newJWTVerifier(c config.JWT) (*jwtVerifier, error) {
	var keys oidc.KeySet
	algs := []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "EdDSA"}
	switch {
	case c.JWKSURL != "":
		keys = oidc.NewRemoteKeySet(context.Background(), c.JWKSURL)
	case c.PublicKeyFile != "":
		raw, err := os.ReadFile(c.PublicKeyFile)
		if err != nil {
			return nil, fmt.Errorf("auth.jwt.public_key_file: %w", err)
		}
		block, _ := pem.Decode(raw)
		if block == nil {
			return nil, fmt.Errorf("auth.jwt.public_key_file: no PEM block in %s", c.PublicKeyFile)
		}
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("auth.jwt.public_key_file: %w", err)
		}
		keys = &oidc.StaticKeySet{PublicKeys: []crypto.PublicKey{key}}
	default:
		secret := os.Getenv(c.SecretEnv)
		if len(secret) < 32 {
			return nil, fmt.Errorf("auth.jwt.secret_env: %s must hold a secret of at least 32 bytes", c.SecretEnv)
		}
		return &jwtVerifier{issuer: c.Issuer, audiences: c.Audiences, hmac: []byte(secret), roles: c.RoleMap}, nil
	}
	return staticVerifier(c.Issuer, c.Audiences, keys, algs, c.RoleMap), nil
}

func staticVerifier(issuer string, audiences []string, keys oidc.KeySet, algs []string, roles config.RoleMap) *jwtVerifier {
	v := oidc.NewVerifier(issuer, keys, &oidc.Config{SkipClientIDCheck: true, SkipIssuerCheck: issuer == "", SupportedSigningAlgs: algs})
	return &jwtVerifier{roles: roles, audiences: audiences, verifier: v}
}

func (v *jwtVerifier) get(ctx context.Context) (*oidc.IDTokenVerifier, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.verifier != nil {
		return v.verifier, nil
	}
	if time.Since(v.lastTry) < 5*time.Second {
		return nil, errors.New("the identity provider isn't reachable yet")
	}
	v.lastTry = time.Now()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	verifier, err := v.discover(ctx)
	if err != nil {
		return nil, err
	}
	v.verifier = verifier
	return verifier, nil
}

func (v *jwtVerifier) verifyHMAC(raw string) (Caller, error) {
	tok, err := jwt.ParseSigned(raw, []jose.SignatureAlgorithm{jose.HS256, jose.HS384, jose.HS512})
	if err != nil {
		return Caller{}, fmt.Errorf("%w: %v", ErrBadToken, err)
	}
	var std jwt.Claims
	claims := map[string]any{}
	if err := tok.Claims(v.hmac, &std, &claims); err != nil {
		return Caller{}, fmt.Errorf("%w: %v", ErrBadToken, err)
	}
	if std.Expiry == nil {
		return Caller{}, fmt.Errorf("%w: no expiry", ErrBadToken)
	}
	want := jwt.Expected{Time: time.Now()}
	if v.issuer != "" {
		want.Issuer = v.issuer
	}
	if err := std.ValidateWithLeeway(want, time.Minute); err != nil {
		return Caller{}, fmt.Errorf("%w: %v", ErrBadToken, err)
	}
	if len(v.audiences) > 0 && !slices.ContainsFunc(std.Audience, func(a string) bool { return slices.Contains(v.audiences, a) }) {
		return Caller{}, fmt.Errorf("%w: issued for %v, not for %v", ErrBadToken, std.Audience, v.audiences)
	}
	scope, ok := scopeOf(v.roles, claims)
	if !ok {
		return Caller{}, ErrNotAllowed
	}
	return Caller{ID: "user:" + usernameOf(v.roles, claims, std.Subject), Scope: scope}, nil
}

func (v *jwtVerifier) verify(ctx context.Context, raw string) (Caller, error) {
	if v.hmac != nil {
		return v.verifyHMAC(raw)
	}
	verifier, err := v.get(context.WithoutCancel(ctx))
	if err != nil {
		return Caller{}, err
	}
	tok, err := verifier.Verify(ctx, raw)
	if err != nil {
		return Caller{}, fmt.Errorf("%w: %v", ErrBadToken, err)
	}
	if len(v.audiences) > 0 && !slices.ContainsFunc(tok.Audience, func(a string) bool { return slices.Contains(v.audiences, a) }) {
		return Caller{}, fmt.Errorf("%w: issued for %v, not for %v", ErrBadToken, tok.Audience, v.audiences)
	}
	var claims map[string]any
	if err := tok.Claims(&claims); err != nil {
		return Caller{}, fmt.Errorf("%w: %v", ErrBadToken, err)
	}
	scope, ok := scopeOf(v.roles, claims)
	if !ok {
		return Caller{}, ErrNotAllowed
	}
	return Caller{ID: "user:" + usernameOf(v.roles, claims, tok.Subject), Scope: scope}, nil
}

func usernameOf(r config.RoleMap, claims map[string]any, sub string) string {
	for _, c := range []string{r.UsernameClaim, "preferred_username", "upn", "unique_name", "email"} {
		if s, ok := claims[c].(string); c != "" && ok && s != "" {
			return s
		}
	}
	return sub
}

func scopeOf(r config.RoleMap, claims map[string]any) (Scope, bool) {
	claim := r.RolesClaim
	if claim == "" {
		claim = "groups"
	}
	return pickScope(claimValues(claims[claim]), r.Admin, r.Operator, r.Viewer)
}

func claimValues(v any) []string {
	var out []string
	switch vals := v.(type) {
	case string:
		out = strings.Fields(vals)
	case []any:
		for _, x := range vals {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
	}
	return out
}

// pickScope is the highest scope any of have grants.
func pickScope(have, admin, operator, viewer []string) (Scope, bool) {
	in := func(list []string) bool {
		return slices.ContainsFunc(have, func(h string) bool {
			return slices.ContainsFunc(list, func(l string) bool { return strings.EqualFold(l, h) })
		})
	}
	switch {
	case in(admin):
		return ScopeAdmin, true
	case in(operator):
		return ScopeOperator, true
	case len(viewer) == 0 || in(viewer):
		return ScopeViewer, true
	}
	return "", false
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
