// Package webui serves the ARK Console built into the binary.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed all:dist
var dist embed.FS

// Handler serves the console and falls back to index.html for its routes.
func Handler() http.Handler {
	site, _ := fs.Sub(dist, "dist")
	files := http.FileServerFS(site)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		p := strings.TrimPrefix(path.Clean("/"+strings.Trim(r.URL.Path, "/")), "/")
		if p == "" {
			p = "index.html"
		}
		if _, err := fs.Stat(site, p); err != nil {
			if _, err := fs.Stat(site, "index.html"); err != nil {
				http.Error(w, "this ARK binary was built without the console; use the ghcr.io/raven-clown/ark image or build bridge-ui into internal/webui/dist", http.StatusNotFound)
				return
			}
			r.URL.Path = "/"
		}
		if strings.HasPrefix(p, "assets/") {
			h.Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			h.Set("Cache-Control", "no-cache")
		}
		files.ServeHTTP(w, r)
	})
}
