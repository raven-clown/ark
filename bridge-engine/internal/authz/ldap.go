package authz

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

func ldapLogin(_ context.Context, c config.LDAP, username, password string) (Scope, error) {
	conn, err := ldap.DialURL(c.URL, ldap.DialWithDialer(nil), ldap.DialWithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	if err != nil {
		return "", fmt.Errorf("reaching the directory %s: %w", c.URL, err)
	}
	defer conn.Close()
	conn.SetTimeout(10 * time.Second)
	if c.StartTLS {
		if err := conn.StartTLS(&tls.Config{MinVersion: tls.VersionTLS12, ServerName: hostOf(c.URL)}); err != nil {
			return "", fmt.Errorf("starting TLS with the directory: %w", err)
		}
	}

	userDN := strings.ReplaceAll(c.UserDN, "{username}", ldap.EscapeDN(username))
	if c.BindDN != "" {
		if err := conn.Bind(c.BindDN, os.Getenv(c.BindPasswordEnv)); err != nil {
			return "", fmt.Errorf("the directory refused ARK's bind_dn: %w", err)
		}
		filter := c.UserFilter
		if filter == "" {
			filter = "(|(sAMAccountName={username})(uid={username})(userPrincipalName={username}))"
		}
		res, err := conn.Search(ldap.NewSearchRequest(c.BaseDN, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 2, 10, false,
			strings.ReplaceAll(filter, "{username}", ldap.EscapeFilter(username)), []string{"dn"}, nil))
		if err != nil || len(res.Entries) != 1 {
			return "", ErrBadPassword
		}
		userDN = res.Entries[0].DN
	}
	if err := conn.Bind(userDN, password); err != nil {
		return "", ErrBadPassword
	}
	if c.BindDN != "" {
		// Many directories let a person read little about themselves.
		if err := conn.Bind(c.BindDN, os.Getenv(c.BindPasswordEnv)); err != nil {
			return "", fmt.Errorf("the directory refused ARK's bind_dn: %w", err)
		}
	}

	groups := groupsOf(conn, c, userDN, username)
	scope, ok := pickScope(groups, c.Admin, c.Operator, c.Viewer)
	if !ok {
		return "", ErrNotAllowed
	}
	return scope, nil
}

func groupsOf(conn *ldap.Conn, c config.LDAP, userDN, username string) []string {
	var out []string
	if c.GroupBaseDN != "" {
		filter := c.GroupFilter
		if filter == "" {
			filter = "(|(member={dn})(uniqueMember={dn})(memberUid={username}))"
		}
		filter = strings.NewReplacer("{dn}", ldap.EscapeFilter(userDN), "{username}", ldap.EscapeFilter(username)).Replace(filter)
		res, err := conn.Search(ldap.NewSearchRequest(c.GroupBaseDN, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 500, 10, false, filter, []string{"cn"}, nil))
		if err == nil {
			for _, e := range res.Entries {
				out = append(out, e.GetAttributeValue("cn"), e.DN)
			}
		}
		return out
	}
	req := ldap.NewSearchRequest(userDN, ldap.ScopeBaseObject, ldap.NeverDerefAliases, 1, 10, false, "(objectClass=*)", []string{"memberOf"}, nil)
	if !strings.Contains(userDN, "=") {
		// An Active Directory UPN such as alice@corp.example.com.
		req = ldap.NewSearchRequest(c.BaseDN, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 2, 10, false, "(userPrincipalName="+ldap.EscapeFilter(userDN)+")", []string{"memberOf"}, nil)
	}
	res, err := conn.Search(req)
	if err != nil || len(res.Entries) == 0 {
		return out
	}
	for _, g := range res.Entries[0].GetAttributeValues("memberOf") {
		out = append(out, g)
		if dn, err := ldap.ParseDN(g); err == nil && len(dn.RDNs) > 0 && len(dn.RDNs[0].Attributes) > 0 {
			out = append(out, dn.RDNs[0].Attributes[0].Value)
		}
	}
	return out
}

func hostOf(url string) string {
	h := url[strings.Index(url, "://")+3:]
	if i := strings.IndexAny(h, ":/"); i >= 0 {
		h = h[:i]
	}
	return h
}
