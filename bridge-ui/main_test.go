package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func TestSPAFallsBackToIndex(t *testing.T) {
	site := fstest.MapFS{
		"index.html":    {Data: []byte("<html>console</html>")},
		"assets/app.js": {Data: []byte("js")},
	}
	h := secure(spa(site))
	for _, p := range []string{"/", "/pipelines/orders", "/assets/app.js"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", p, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("%s: %d", p, w.Code)
		}
		if w.Header().Get("Content-Security-Policy") == "" {
			t.Fatalf("%s: no CSP", p)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/assets/app.js", nil))
	if !strings.Contains(w.Header().Get("Cache-Control"), "immutable") {
		t.Fatal("hashed assets should be cached")
	}
}

func TestSPAWithoutBuildSaysSo(t *testing.T) {
	w := httptest.NewRecorder()
	spa(fstest.MapFS{}).ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != http.StatusNotFound || !strings.Contains(w.Body.String(), "npm run build") {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}
