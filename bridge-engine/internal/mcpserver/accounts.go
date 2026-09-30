package mcpserver

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"

	"golang.org/x/crypto/bcrypt"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

// AccountStore keeps the username/password accounts in the config.
type AccountStore interface {
	Users() []config.User
	ApplyUsers(ctx context.Context, users []config.User) error
}

type accountOut struct {
	Username string `json:"username"`
	Scope    string `json:"scope"`
}

var usernamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._@-]{0,63}$`)

func (c *Console) accountRoutes(mux *http.ServeMux) {
	store := c.d.Accounts
	if store == nil {
		return
	}
	mux.HandleFunc("GET /api/v1/config/users", func(w http.ResponseWriter, r *http.Request) {
		out := []accountOut{}
		for _, u := range store.Users() {
			out = append(out, accountOut{u.Username, u.Scope})
		}
		writeJSON(w, http.StatusOK, out)
	})
	mux.HandleFunc("PUT /api/v1/config/users/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		var in struct {
			Scope    string `json:"scope"`
			Password string `json:"password"`
		}
		if !readJSON(w, r, &in) {
			return
		}
		if !usernamePattern.MatchString(name) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "a username is letters, digits, . _ @ or -, up to 64"})
			return
		}
		if in.Scope != "viewer" && in.Scope != "operator" && in.Scope != "admin" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "scope must be viewer, operator or admin"})
			return
		}
		users := slices.Clone(store.Users())
		i := slices.IndexFunc(users, func(u config.User) bool { return u.Username == name })
		if i < 0 && in.Password == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "a new account needs a password"})
			return
		}
		if in.Password != "" && len(in.Password) < 8 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "a password needs at least 8 characters"})
			return
		}
		if self(r, name) && i >= 0 && users[i].Scope == "admin" && in.Scope != "admin" {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "you can't take admin away from your own account; ask another admin"})
			return
		}
		u := config.User{Username: name, Scope: in.Scope}
		if i >= 0 {
			u.PasswordHash = users[i].PasswordHash
		}
		if in.Password != "" {
			hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), 12)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, errBody(err))
				return
			}
			u.PasswordHash = string(hash)
		}
		if i >= 0 {
			users[i] = u
		} else {
			users = append(users, u)
		}
		if err := store.ApplyUsers(r.Context(), users); err != nil {
			writeJSON(w, http.StatusConflict, errBody(err))
			return
		}
		c.audit(r, fmt.Sprintf("account %s saved as %s", name, in.Scope), "")
		writeJSON(w, http.StatusOK, accountOut{name, in.Scope})
	})
	mux.HandleFunc("DELETE /api/v1/config/users/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if self(r, name) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "you can't delete your own account"})
			return
		}
		users := slices.DeleteFunc(slices.Clone(store.Users()), func(u config.User) bool { return u.Username == name })
		if len(users) == len(store.Users()) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no account named " + name})
			return
		}
		if err := store.ApplyUsers(r.Context(), users); err != nil {
			writeJSON(w, http.StatusConflict, errBody(err))
			return
		}
		c.audit(r, "account "+name+" deleted", "")
		writeJSON(w, http.StatusOK, map[string]string{"deleted": name})
	})
}

func self(r *http.Request, name string) bool {
	return strings.TrimPrefix(callerOf(r).ID, "user:") == name
}
