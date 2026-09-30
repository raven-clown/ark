package authz

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

type headerAuth struct {
	cfg     config.HeaderAuth
	proxies []*net.IPNet
}

func newHeaderAuth(c config.HeaderAuth) (*headerAuth, error) {
	h := &headerAuth{cfg: c}
	switch c.Preset {
	case "":
		if c.UserHeader == "" {
			return nil, fmt.Errorf("auth.header needs user_header or a preset (azure-easy-auth, cloudflare-access, oauth2-proxy)")
		}
	case "oauth2-proxy":
		h.cfg.UserHeader, h.cfg.GroupsHeader = "X-Forwarded-Preferred-Username", "X-Forwarded-Groups"
	case "cloudflare-access":
		h.cfg.UserHeader = "Cf-Access-Authenticated-User-Email"
	case "azure-easy-auth":
		h.cfg.UserHeader = "X-Ms-Client-Principal-Name"
	default:
		return nil, fmt.Errorf("auth.header.preset %q is not one of azure-easy-auth, cloudflare-access, oauth2-proxy", c.Preset)
	}
	if len(c.TrustedProxies) == 0 {
		return nil, fmt.Errorf("auth.header.trusted_proxies must list the proxies' addresses (such as 10.0.0.0/8), or anyone could claim to be anyone")
	}
	for _, p := range c.TrustedProxies {
		if !strings.Contains(p, "/") {
			if strings.Contains(p, ":") {
				p += "/128"
			} else {
				p += "/32"
			}
		}
		_, n, err := net.ParseCIDR(p)
		if err != nil {
			return nil, fmt.Errorf("auth.header.trusted_proxies: %w", err)
		}
		h.proxies = append(h.proxies, n)
	}
	return h, nil
}

func (h *headerAuth) trusted(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	for _, n := range h.proxies {
		if ip != nil && n.Contains(ip) {
			return true
		}
	}
	return false
}

func (a *Authenticator) fromHeader(r *http.Request) (Caller, bool, error) {
	h := a.header
	if h == nil || !h.trusted(r) {
		return Caller{}, false, nil
	}
	user := strings.TrimSpace(r.Header.Get(h.cfg.UserHeader))
	if user == "" && h.cfg.Preset == "oauth2-proxy" {
		user = firstNonEmpty(r.Header.Get("X-Forwarded-Email"), r.Header.Get("X-Forwarded-User"))
	}
	if user == "" {
		return Caller{}, false, nil
	}
	var groups []string
	if h.cfg.GroupsHeader != "" {
		groups = strings.FieldsFunc(r.Header.Get(h.cfg.GroupsHeader), func(r rune) bool { return r == ',' || r == ' ' })
	}
	if h.cfg.Preset == "azure-easy-auth" {
		groups = append(groups, azurePrincipalRoles(r.Header.Get("X-Ms-Client-Principal"))...)
	}
	scope, ok := pickScope(groups, h.cfg.Admin, h.cfg.Operator, h.cfg.Viewer)
	if !ok {
		return Caller{}, true, ErrNotAllowed
	}
	return Caller{ID: "user:" + user, Scope: scope}, true, nil
}

func azurePrincipalRoles(header string) []string {
	raw, err := base64.StdEncoding.DecodeString(header)
	if err != nil {
		return nil
	}
	var p struct {
		RoleType string `json:"role_typ"`
		Claims   []struct {
			Type  string `json:"typ"`
			Value string `json:"val"`
		} `json:"claims"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return nil
	}
	var out []string
	for _, c := range p.Claims {
		if c.Type == p.RoleType || c.Type == "roles" || c.Type == "groups" || strings.HasSuffix(c.Type, "/role") || strings.HasSuffix(c.Type, "/groups") {
			out = append(out, c.Value)
		}
	}
	return out
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return ""
}

func ClientCAs(a config.Auth) (*x509.CertPool, error) {
	if a.ClientCert == nil {
		return nil, nil
	}
	pem, err := os.ReadFile(a.ClientCert.CAFile)
	if err != nil {
		return nil, fmt.Errorf("auth.client_cert.ca_file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("auth.client_cert.ca_file: no certificates in %s", a.ClientCert.CAFile)
	}
	return pool, nil
}

func (a *Authenticator) fromCert(r *http.Request) (Caller, bool, error) {
	if a.cert == nil || r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
		return Caller{}, false, nil
	}
	leaf := r.TLS.VerifiedChains[0][0]
	scope, ok := pickScope(leaf.Subject.OrganizationalUnit, a.cert.Admin, a.cert.Operator, a.cert.Viewer)
	if !ok {
		return Caller{}, true, ErrNotAllowed
	}
	return Caller{ID: "cert:" + leaf.Subject.CommonName, Scope: scope}, true, nil
}
