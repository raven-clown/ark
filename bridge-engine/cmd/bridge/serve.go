package main

import (
	"crypto/tls"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/acme/autocert"

	"github.com/raven-clown/ark/bridge-engine/internal/authz"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/webui"
)

func envOr(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func listenAddr() string {
	if v := os.Getenv("ARK_LISTEN"); v != "" {
		return v
	}
	return ":" + envOr("ARK_PORT", "8080")
}

func setupTLS(srv *http.Server, a config.Auth, dataDir string) (bool, error) {
	cas, err := authz.ClientCAs(a)
	if err != nil {
		return false, err
	}
	var cfg *tls.Config
	switch {
	case os.Getenv("ARK_TLS_CERT") != "":
		cfg = &tls.Config{MinVersion: tls.VersionTLS12}
	case os.Getenv("ARK_TLS_DOMAINS") != "":
		var domains []string
		for _, d := range strings.Split(os.Getenv("ARK_TLS_DOMAINS"), ",") {
			if d = strings.TrimSpace(d); d != "" {
				domains = append(domains, d)
			}
		}
		m := &autocert.Manager{Prompt: autocert.AcceptTOS, HostPolicy: autocert.HostWhitelist(domains...),
			Cache: autocert.DirCache(filepath.Join(dataDir, "acme")), Email: os.Getenv("ARK_TLS_EMAIL")}
		cfg = m.TLSConfig()
		cfg.MinVersion = tls.VersionTLS12
	default:
		if cas != nil {
			return false, errString("auth.client_cert needs HTTPS: set ARK_TLS_CERT and ARK_TLS_KEY, or ARK_TLS_DOMAINS")
		}
		return false, nil
	}
	if cas != nil {
		cfg.ClientCAs, cfg.ClientAuth = cas, tls.VerifyClientCertIfGiven
	}
	srv.TLSConfig = cfg
	return true, nil
}

type errString string

func (e errString) Error() string { return string(e) }

func route(api http.Handler) http.Handler {
	web := webui.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if strings.HasPrefix(p, "/api/") || p == "/healthz" || p == "/metrics" {
			api.ServeHTTP(w, r)
			return
		}
		web.ServeHTTP(w, r)
	})
}
