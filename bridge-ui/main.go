// Command console serves the ARK Console: the built web app, plus a proxy
// that forwards /api/v1/ to an ARK engine, so the browser only ever talks
// to this origin and never to Kafka.
package main

import (
	"embed"
	"io/fs"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

//go:embed all:dist
var dist embed.FS

func main() {
	engine := envOr("ARK_ENGINE_URL", "http://127.0.0.1:8080")
	addr := envOr("ARK_CONSOLE_ADDR", ":8088")
	target, err := url.Parse(engine)
	if err != nil || target.Scheme == "" || target.Host == "" {
		log.Fatalf("ARK_ENGINE_URL must be an absolute URL, got %q", engine)
	}
	site, _ := fs.Sub(dist, "dist")

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.FlushInterval = -1 // stream live tail events as they arrive
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("engine unreachable: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"the console can't reach the ARK engine at ` + engine + `"}`))
	}

	mux := http.NewServeMux()
	mux.Handle("/api/v1/", proxy)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("/", spa(site))

	srv := &http.Server{Addr: addr, Handler: secure(mux), ReadHeaderTimeout: 10 * time.Second}
	log.Printf("ARK Console on %s, engine %s", addr, engine) // #nosec G706 -- operator-set env values
	log.Fatal(srv.ListenAndServe())
}

// spa serves the built app and falls back to index.html for client routes.
func spa(site fs.FS) http.Handler {
	files := http.FileServerFS(site)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if p == "" {
			p = "index.html"
		}
		if _, err := fs.Stat(site, p); err != nil {
			if _, err := fs.Stat(site, "index.html"); err != nil {
				http.Error(w, "the console web app was not built into this binary (run npm run build in bridge-ui)", http.StatusNotFound)
				return
			}
			r.URL.Path = "/"
		}
		if strings.HasPrefix(p, "assets/") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-cache")
		}
		files.ServeHTTP(w, r)
	})
}

func secure(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
