package config

import (
	"fmt"
	"net/url"
	"strings"
)

type Auth struct {
	// Anonymous is the scope a request with no credentials gets.
	Anonymous    string      `yaml:"anonymous,omitempty"`
	MCPAnonymous string      `yaml:"mcp_anonymous,omitempty"`
	OIDC         *OIDC       `yaml:"oidc,omitempty"`
	JWT          *JWT        `yaml:"jwt,omitempty"`
	Accounts     bool        `yaml:"accounts,omitempty"`
	Users        []User      `yaml:"users,omitempty"`
	LDAP         *LDAP       `yaml:"ldap,omitempty"`
	Basic        bool        `yaml:"basic,omitempty"`
	Header       *HeaderAuth `yaml:"header,omitempty"`
	ClientCert   *ClientCert `yaml:"client_cert,omitempty"`
	SAML         *SAML       `yaml:"saml,omitempty"`
	OAuth2       []OAuth2    `yaml:"oauth2,omitempty"`
	SessionHours int         `yaml:"session_hours,omitempty"`
}

type HeaderAuth struct {
	Preset         string   `yaml:"preset,omitempty"`
	UserHeader     string   `yaml:"user_header,omitempty"`
	GroupsHeader   string   `yaml:"groups_header,omitempty"`
	TrustedProxies []string `yaml:"trusted_proxies"`
	RoleMap        `yaml:",inline"`
}

type ClientCert struct {
	CAFile  string `yaml:"ca_file"`
	RoleMap `yaml:",inline"`
}

type SAML struct {
	MetadataURL  string `yaml:"metadata_url,omitempty"`
	MetadataFile string `yaml:"metadata_file,omitempty"`
	// EntityID is how ARK names itself to the provider; default its URL.
	EntityID string `yaml:"entity_id,omitempty"`
	CertFile string `yaml:"cert_file,omitempty"`
	KeyFile  string `yaml:"key_file,omitempty"`
	RoleMap  `yaml:",inline"`
}

type OAuth2 struct {
	Name            string   `yaml:"name"`
	Preset          string   `yaml:"preset,omitempty"`
	ClientID        string   `yaml:"client_id"`
	ClientSecretEnv string   `yaml:"client_secret_env"`
	AuthURL         string   `yaml:"auth_url,omitempty"`
	TokenURL        string   `yaml:"token_url,omitempty"`
	UserInfoURL     string   `yaml:"userinfo_url,omitempty"`
	GroupsURL       string   `yaml:"groups_url,omitempty"`
	Scopes          []string `yaml:"scopes,omitempty"`
	RoleMap         `yaml:",inline"`
}

// RoleMap turns a token's claims into a person and a scope.
type RoleMap struct {
	UsernameClaim string   `yaml:"username_claim,omitempty"`
	RolesClaim    string   `yaml:"roles_claim,omitempty"`
	Admin         []string `yaml:"admin,omitempty"`
	Operator      []string `yaml:"operator,omitempty"`
	// Viewer empty lets anyone who signs in be a viewer.
	Viewer []string `yaml:"viewer,omitempty"`
}

type OIDC struct {
	Preset          string   `yaml:"preset,omitempty"`
	Tenant          string   `yaml:"tenant,omitempty"`
	Issuer          string   `yaml:"issuer"`
	ClientID        string   `yaml:"client_id"`
	ClientSecretEnv string   `yaml:"client_secret_env,omitempty"`
	Audiences       []string `yaml:"audiences,omitempty"`
	Scopes          []string `yaml:"scopes,omitempty"`
	RoleMap         `yaml:",inline"`
}

type JWT struct {
	Issuer        string   `yaml:"issuer,omitempty"`
	Audiences     []string `yaml:"audiences,omitempty"`
	JWKSURL       string   `yaml:"jwks_url,omitempty"`
	PublicKeyFile string   `yaml:"public_key_file,omitempty"`
	SecretEnv     string   `yaml:"secret_env,omitempty"`
	RoleMap       `yaml:",inline"`
}

// User signs in with a password kept as a bcrypt hash.
type User struct {
	Username     string `yaml:"username"`
	PasswordHash string `yaml:"password_hash"`
	Scope        string `yaml:"scope"`
}

type LDAP struct {
	URL             string   `yaml:"url"`
	StartTLS        bool     `yaml:"start_tls,omitempty"`
	BindDN          string   `yaml:"bind_dn,omitempty"`
	BindPasswordEnv string   `yaml:"bind_password_env,omitempty"`
	UserDN          string   `yaml:"user_dn,omitempty"`
	BaseDN          string   `yaml:"base_dn,omitempty"`
	UserFilter      string   `yaml:"user_filter,omitempty"`
	GroupBaseDN     string   `yaml:"group_base_dn,omitempty"`
	GroupFilter     string   `yaml:"group_filter,omitempty"`
	Admin           []string `yaml:"admin,omitempty"`
	Operator        []string `yaml:"operator,omitempty"`
	Viewer          []string `yaml:"viewer,omitempty"`
}

func validScope(s string) bool { return s == "viewer" || s == "operator" || s == "admin" }

func validURL(s string, schemes ...string) bool {
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return false
	}
	for _, sc := range schemes {
		if u.Scheme == sc {
			return true
		}
	}
	return false
}

func validateAuth(a Auth) error {
	for field, s := range map[string]string{"auth.anonymous": a.Anonymous, "auth.mcp_anonymous": a.MCPAnonymous} {
		if s != "" && !validScope(s) {
			return fmt.Errorf("%s must be viewer, operator or admin (or left out), got %q", field, s)
		}
	}
	if o := a.OIDC; o != nil {
		if o.Preset == "azure" && o.Issuer == "" && o.Tenant != "" {
			o.Issuer = "https://login.microsoftonline.com/" + o.Tenant + "/v2.0"
		}
		if !validURL(o.Issuer, "https", "http") {
			return fmt.Errorf("auth.oidc.issuer must be the provider's issuer URL, such as https://adfs.example.com/adfs, got %q", o.Issuer)
		}
		if o.ClientID == "" {
			return fmt.Errorf("auth.oidc.client_id is required")
		}
	}
	if j := a.JWT; j != nil {
		n := 0
		for _, s := range []string{j.JWKSURL, j.PublicKeyFile, j.SecretEnv} {
			if s != "" {
				n++
			}
		}
		if n != 1 {
			return fmt.Errorf("auth.jwt needs exactly one of jwks_url, public_key_file or secret_env")
		}
		if j.JWKSURL != "" && !validURL(j.JWKSURL, "https", "http") {
			return fmt.Errorf("auth.jwt.jwks_url must be a URL, got %q", j.JWKSURL)
		}
		if j.SecretEnv != "" && !envVarName.MatchString(j.SecretEnv) {
			return fmt.Errorf("auth.jwt.secret_env must name an environment variable")
		}
	}
	seen := map[string]bool{}
	for i, u := range a.Users {
		if u.Username == "" || seen[u.Username] {
			return fmt.Errorf("auth.users[%d]: username is empty or used twice", i)
		}
		seen[u.Username] = true
		if !strings.HasPrefix(u.PasswordHash, "$2") {
			return fmt.Errorf("auth.users[%d] (%s): password_hash must be a bcrypt hash, such as the output of: htpasswd -bnBC 12 \"\" <password>", i, u.Username)
		}
		if !validScope(u.Scope) {
			return fmt.Errorf("auth.users[%d] (%s): scope must be viewer, operator or admin", i, u.Username)
		}
	}
	if l := a.LDAP; l != nil {
		if !validURL(l.URL, "ldap", "ldaps") {
			return fmt.Errorf("auth.ldap.url must look like ldaps://ad.example.com:636, got %q", l.URL)
		}
		if l.UserDN == "" && (l.BindDN == "" || l.BaseDN == "") {
			return fmt.Errorf("auth.ldap needs user_dn (such as {username}@corp.example.com), or bind_dn and base_dn to search for the user")
		}
		if l.BindDN != "" && !envVarName.MatchString(l.BindPasswordEnv) {
			return fmt.Errorf("auth.ldap.bind_password_env must name an environment variable")
		}
	}
	if a.SessionHours < 0 || a.SessionHours > 24*30 {
		return fmt.Errorf("auth.session_hours must be between 1 and 720, or left out for 12")
	}
	return nil
}
