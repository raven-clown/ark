package authz

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/xml"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/crewjam/saml"
	"github.com/crewjam/saml/samlsp"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

type samlAuth struct {
	cfg     config.SAML
	key     *rsa.PrivateKey
	cert    *x509.Certificate
	idp     *saml.EntityDescriptor
	mu      sync.Mutex
	pending map[string]pendingSAML
}

type pendingSAML struct {
	request string
	expires time.Time
}

func (s *samlAuth) remember(requestID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil {
		s.pending = map[string]pendingSAML{}
	}
	for k, p := range s.pending {
		if time.Now().After(p.expires) {
			delete(s.pending, k)
		}
	}
	relay := randomString()[:22]
	s.pending[relay] = pendingSAML{requestID, time.Now().Add(10 * time.Minute)}
	return relay
}

func (s *samlAuth) take(relay string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pending[relay]
	delete(s.pending, relay)
	return p.request, ok && time.Now().Before(p.expires)
}

func newSAML(c config.SAML) (*samlAuth, error) {
	s := &samlAuth{cfg: c}
	if c.CertFile != "" {
		pair, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("auth.saml cert_file/key_file: %w", err)
		}
		key, ok := pair.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("auth.saml.key_file must be an RSA key")
		}
		s.key, s.cert = key, pair.Leaf
		if s.cert == nil {
			s.cert, _ = x509.ParseCertificate(pair.Certificate[0])
		}
	} else {
		// A new key each start: fine unless the provider encrypts assertions.
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, err
		}
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ark"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(10, 0, 0)}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			return nil, err
		}
		s.key = key
		s.cert, _ = x509.ParseCertificate(der)
	}
	return s, nil
}

func (s *samlAuth) provider(ctx context.Context, r *http.Request) (*saml.ServiceProvider, error) {
	if s.idp == nil {
		var err error
		if s.cfg.MetadataURL != "" {
			u, perr := url.Parse(s.cfg.MetadataURL)
			if perr != nil {
				return nil, perr
			}
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			s.idp, err = samlsp.FetchMetadata(ctx, http.DefaultClient, *u)
		} else {
			var raw []byte
			if raw, err = os.ReadFile(s.cfg.MetadataFile); err == nil {
				s.idp, err = samlsp.ParseMetadata(raw)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("reading the SAML provider's metadata: %w", err)
		}
	}
	base, _ := url.Parse(PublicURL(r))
	acs, meta := *base, *base
	acs.Path, meta.Path = "/auth/saml/acs", "/auth/saml/metadata"
	sp := &saml.ServiceProvider{Key: s.key, Certificate: s.cert, MetadataURL: meta, AcsURL: acs, IDPMetadata: s.idp,
		EntityID: s.cfg.EntityID, AllowIDPInitiated: false, SignatureMethod: "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256",
		AuthnNameIDFormat: saml.UnspecifiedNameIDFormat}
	if sp.EntityID == "" {
		sp.EntityID = meta.String()
	}
	return sp, nil
}

func (a *Authenticator) samlMetadata(w http.ResponseWriter, r *http.Request) {
	sp, err := a.saml.provider(r.Context(), r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	out, _ := xml.MarshalIndent(sp.Metadata(), "", "  ")
	w.Header().Set("Content-Type", "application/samlmetadata+xml")
	_, _ = w.Write(out) // #nosec G705 nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter.no-direct-write-to-responsewriter -- XML marshalled from ARK's own SAML metadata
}

func (a *Authenticator) samlLogin(w http.ResponseWriter, r *http.Request) {
	sp, err := a.saml.provider(r.Context(), r)
	if err != nil {
		failed(w, r, err)
		return
	}
	req, err := sp.MakeAuthenticationRequest(sp.GetSSOBindingLocation(saml.HTTPRedirectBinding), saml.HTTPRedirectBinding, saml.HTTPPostBinding)
	if err != nil {
		failed(w, r, err)
		return
	}
	to, err := req.Redirect(a.saml.remember(req.ID), sp)
	if err != nil {
		failed(w, r, err)
		return
	}
	redirect(w, r, to.String())
}

func (a *Authenticator) samlACS(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		failed(w, r, err)
		return
	}
	requestID, ok := a.saml.take(r.PostFormValue("RelayState"))
	if !ok {
		failed(w, r, errors.New("the sign-in expired or wasn't started here; try again"))
		return
	}
	sp, err := a.saml.provider(r.Context(), r)
	if err != nil {
		failed(w, r, err)
		return
	}
	assertion, err := sp.ParseResponse(r, []string{requestID})
	if err != nil {
		var ie *saml.InvalidResponseError
		if errors.As(err, &ie) {
			err = ie.PrivateErr
		}
		failed(w, r, fmt.Errorf("the SAML response didn't check out: %v", err))
		return
	}
	claims := map[string]any{}
	name := ""
	if assertion.Subject != nil && assertion.Subject.NameID != nil {
		name = assertion.Subject.NameID.Value
	}
	for _, st := range assertion.AttributeStatements {
		for _, at := range st.Attributes {
			var vals []any
			for _, v := range at.Values {
				vals = append(vals, v.Value)
			}
			for _, key := range []string{at.Name, at.FriendlyName} {
				if key != "" {
					claims[key] = vals
				}
			}
		}
	}
	roles := a.saml.cfg.RoleMap
	if roles.RolesClaim == "" {
		roles.RolesClaim = "http://schemas.microsoft.com/ws/2008/06/identity/claims/groups"
		if _, ok := claims[roles.RolesClaim]; !ok {
			roles.RolesClaim = "groups"
		}
	}
	scope, ok := scopeOf(roles, claims)
	if !ok {
		failed(w, r, ErrNotAllowed)
		return
	}
	transient := assertion.Subject != nil && assertion.Subject.NameID != nil && assertion.Subject.NameID.Format == string(saml.TransientNameIDFormat)
	for _, c := range []string{roles.UsernameClaim, "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/upn", "upn",
		"http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress", "email", "username"} {
		if v, ok := claims[c].([]any); c != "" && ok && len(v) > 0 && (c == roles.UsernameClaim || transient || name == "") {
			name = fmt.Sprint(v[0])
			break
		}
	}
	a.finish(w, r, name, scope)
}
