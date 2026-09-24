package authz

import (
	"net/http/httptest"
	"testing"
)

func TestScopeOrdering(t *testing.T) {
	if !ScopeAdmin.AtLeast(ScopeOperator) || !ScopeOperator.AtLeast(ScopeViewer) {
		t.Error("higher scopes must include lower ones")
	}
	if ScopeViewer.AtLeast(ScopeOperator) {
		t.Error("viewer must not pass an operator check")
	}
	if Scope("").AtLeast(ScopeViewer) || Scope("bogus").AtLeast(ScopeViewer) {
		t.Error("unknown scopes must not pass any check")
	}
}

func TestLookup(t *testing.T) {
	t.Setenv("T_VIEWER_TOKENS", "v1, v2")
	t.Setenv("T_OPERATOR_TOKENS", "op")
	store := LoadFromEnv("T")

	if s, ok := store.Lookup("v2"); !ok || s != ScopeViewer {
		t.Errorf("v2: got %q %v", s, ok)
	}
	if s, ok := store.Lookup("op"); !ok || s != ScopeOperator {
		t.Errorf("op: got %q %v", s, ok)
	}
	if _, ok := store.Lookup("nope"); ok {
		t.Error("unknown token must not authenticate")
	}
	if _, ok := store.Lookup(""); ok {
		t.Error("empty token must not authenticate")
	}
}

func TestIsLoopback(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "127.0.0.1:5555"
	if !IsLoopback(r) {
		t.Error("127.0.0.1 is loopback")
	}
	r.RemoteAddr = "10.1.2.3:5555"
	if IsLoopback(r) {
		t.Error("10.1.2.3 is not loopback")
	}
}
