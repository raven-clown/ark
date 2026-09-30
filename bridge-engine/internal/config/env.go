package config

import (
	"os"
	"strconv"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

func env(k string) string { return strings.TrimSpace(os.Getenv(k)) }

func envList(k string) []string {
	var out []string
	for _, v := range strings.Split(os.Getenv(k), ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func envOrDefault(k, def string) string {
	if v := env(k); v != "" {
		return v
	}
	return def
}

func envBool(k string) bool {
	b, _ := strconv.ParseBool(env(k))
	return b
}

func setIf(dst *string, k string) {
	if v := env(k); v != "" {
		*dst = v
	}
}

func setListIf(dst *[]string, k string) {
	if v := envList(k); len(v) > 0 {
		*dst = v
	}
}

func roleMapFromEnv(r *RoleMap, prefix string) {
	setIf(&r.UsernameClaim, prefix+"_USERNAME_CLAIM")
	setIf(&r.RolesClaim, prefix+"_ROLES_CLAIM")
	setListIf(&r.Admin, prefix+"_ADMIN_GROUPS")
	setListIf(&r.Operator, prefix+"_OPERATOR_GROUPS")
	setListIf(&r.Viewer, prefix+"_VIEWER_GROUPS")
}

func ApplyEnv(c *Config) {
	setListIf(&c.Brokers, "ARK_BROKERS")
	setIf(&c.Timezone, "ARK_TIMEZONE")
	if n, err := strconv.Atoi(env("ARK_REPLICATION_FACTOR")); err == nil {
		c.Topics.ReplicationFactor = n
	}
	a := &c.Auth
	setIf(&a.Anonymous, "ARK_AUTH_ANONYMOUS")
	setIf(&a.MCPAnonymous, "ARK_AUTH_MCP_ANONYMOUS")
	a.Accounts = a.Accounts || envBool("ARK_AUTH_ACCOUNTS")
	a.Basic = a.Basic || envBool("ARK_AUTH_BASIC")
	if n, err := strconv.Atoi(env("ARK_SESSION_HOURS")); err == nil {
		a.SessionHours = n
	}
	if user, pass := env("ARK_ADMIN_USER"), os.Getenv("ARK_ADMIN_PASSWORD"); user != "" && pass != "" {
		a.Accounts = true
		found := false
		for _, u := range a.Users {
			found = found || u.Username == user
		}
		if !found {
			if hash, err := bcrypt.GenerateFromPassword([]byte(pass), 12); err == nil {
				a.Users = append(a.Users, User{Username: user, PasswordHash: string(hash), Scope: "admin"})
			}
		}
	}
	if env("ARK_OIDC_CLIENT_ID") != "" {
		if a.OIDC == nil {
			a.OIDC = &OIDC{}
		}
		o := a.OIDC
		setIf(&o.Preset, "ARK_OIDC_PRESET")
		setIf(&o.Tenant, "ARK_OIDC_TENANT")
		setIf(&o.Issuer, "ARK_OIDC_ISSUER")
		setIf(&o.ClientID, "ARK_OIDC_CLIENT_ID")
		if os.Getenv("ARK_OIDC_CLIENT_SECRET") != "" {
			o.ClientSecretEnv = "ARK_OIDC_CLIENT_SECRET"
		}
		setListIf(&o.Audiences, "ARK_OIDC_AUDIENCES")
		setListIf(&o.Scopes, "ARK_OIDC_SCOPES")
		roleMapFromEnv(&o.RoleMap, "ARK_OIDC")
	}
	if env("ARK_SAML_METADATA_URL") != "" || env("ARK_SAML_METADATA_FILE") != "" {
		if a.SAML == nil {
			a.SAML = &SAML{}
		}
		s := a.SAML
		setIf(&s.MetadataURL, "ARK_SAML_METADATA_URL")
		setIf(&s.MetadataFile, "ARK_SAML_METADATA_FILE")
		setIf(&s.EntityID, "ARK_SAML_ENTITY_ID")
		setIf(&s.CertFile, "ARK_SAML_CERT_FILE")
		setIf(&s.KeyFile, "ARK_SAML_KEY_FILE")
		roleMapFromEnv(&s.RoleMap, "ARK_SAML")
	}
	if env("ARK_LDAP_URL") != "" {
		if a.LDAP == nil {
			a.LDAP = &LDAP{}
		}
		l := a.LDAP
		setIf(&l.URL, "ARK_LDAP_URL")
		l.StartTLS = l.StartTLS || envBool("ARK_LDAP_START_TLS")
		setIf(&l.UserDN, "ARK_LDAP_USER_DN")
		setIf(&l.BindDN, "ARK_LDAP_BIND_DN")
		if os.Getenv("ARK_LDAP_BIND_PASSWORD") != "" {
			l.BindPasswordEnv = "ARK_LDAP_BIND_PASSWORD"
		}
		setIf(&l.BaseDN, "ARK_LDAP_BASE_DN")
		setIf(&l.UserFilter, "ARK_LDAP_USER_FILTER")
		setIf(&l.GroupBaseDN, "ARK_LDAP_GROUP_BASE_DN")
		setIf(&l.GroupFilter, "ARK_LDAP_GROUP_FILTER")
		setListIf(&l.Admin, "ARK_LDAP_ADMIN_GROUPS")
		setListIf(&l.Operator, "ARK_LDAP_OPERATOR_GROUPS")
		setListIf(&l.Viewer, "ARK_LDAP_VIEWER_GROUPS")
	}
	if env("ARK_JWT_JWKS_URL") != "" || env("ARK_JWT_PUBLIC_KEY_FILE") != "" || os.Getenv("ARK_JWT_SECRET") != "" {
		if a.JWT == nil {
			a.JWT = &JWT{}
		}
		j := a.JWT
		setIf(&j.Issuer, "ARK_JWT_ISSUER")
		setListIf(&j.Audiences, "ARK_JWT_AUDIENCES")
		setIf(&j.JWKSURL, "ARK_JWT_JWKS_URL")
		setIf(&j.PublicKeyFile, "ARK_JWT_PUBLIC_KEY_FILE")
		if os.Getenv("ARK_JWT_SECRET") != "" {
			j.SecretEnv = "ARK_JWT_SECRET"
		}
		roleMapFromEnv(&j.RoleMap, "ARK_JWT")
	}
	if env("ARK_HEADER_PRESET") != "" || env("ARK_HEADER_USER") != "" {
		if a.Header == nil {
			a.Header = &HeaderAuth{}
		}
		h := a.Header
		setIf(&h.Preset, "ARK_HEADER_PRESET")
		setIf(&h.UserHeader, "ARK_HEADER_USER")
		setIf(&h.GroupsHeader, "ARK_HEADER_GROUPS")
		setListIf(&h.TrustedProxies, "ARK_HEADER_TRUSTED_PROXIES")
		roleMapFromEnv(&h.RoleMap, "ARK_HEADER")
	}
	if env("ARK_CLIENT_CA") != "" {
		if a.ClientCert == nil {
			a.ClientCert = &ClientCert{}
		}
		setIf(&a.ClientCert.CAFile, "ARK_CLIENT_CA")
		roleMapFromEnv(&a.ClientCert.RoleMap, "ARK_CLIENT_CERT")
	}
	for _, p := range []string{"GITHUB", "GITLAB", "BITBUCKET", "OAUTH2"} {
		id := env("ARK_" + p + "_CLIENT_ID")
		if id == "" {
			continue
		}
		name := strings.ToLower(p)
		o := OAuth2{Name: name, Preset: name, ClientID: id, ClientSecretEnv: "ARK_" + p + "_CLIENT_SECRET"}
		if p == "OAUTH2" {
			o.Name, o.Preset = envOrDefault("ARK_OAUTH2_NAME", "oauth2"), ""
			setIf(&o.AuthURL, "ARK_OAUTH2_AUTH_URL")
			setIf(&o.TokenURL, "ARK_OAUTH2_TOKEN_URL")
			setIf(&o.UserInfoURL, "ARK_OAUTH2_USERINFO_URL")
			setIf(&o.GroupsURL, "ARK_OAUTH2_GROUPS_URL")
			setListIf(&o.Scopes, "ARK_OAUTH2_SCOPES")
			name = o.Name
		}
		roleMapFromEnv(&o.RoleMap, "ARK_"+p)
		replaced := false
		for i := range a.OAuth2 {
			if a.OAuth2[i].Name == name {
				a.OAuth2[i], replaced = o, true
			}
		}
		if !replaced {
			a.OAuth2 = append(a.OAuth2, o)
		}
	}
}
