package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Handler struct {
	DB     *pgxpool.Pool
	Secret string
	Redis  *redis.Client
	// OnLogout runs teardown (content wipe + broadcasts + socket kill).
	// Wired in main; auth stays hub-free.
	OnLogout func(ctx context.Context, uid int64, username string)
}

type User struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	CreatedAt string `json:"created_at"`
}

type credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (h *Handler) dbOK(w http.ResponseWriter) bool {
	if h.DB == nil || h.Secret == "" {
		writeErr(w, http.StatusServiceUnavailable, "auth not configured (database/JWT_SECRET)")
		return false
	}
	return true
}

// POST /auth/register { username, password } -> 201 { user, token }
func (h *Handler) Register(w http.ResponseWriter, r *http.Request) {
	if !h.dbOK(w) {
		return
	}
	var c credentials
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&c); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return
	}
	username, err := NormalizeUsername(c.Username)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := CheckPassword(c.Password); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	hash, err := HashPassword(c.Password)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not hash password")
		return
	}
	var u User
	err = h.DB.QueryRow(r.Context(),
		`INSERT INTO users (username, password_hash) VALUES ($1, $2)
		 RETURNING id, username, created_at::text`, username, hash).Scan(&u.ID, &u.Username, &u.CreatedAt)
	if err != nil {
		if strings.Contains(err.Error(), "users_username_unique") || strings.Contains(err.Error(), "duplicate key") {
			writeErr(w, http.StatusConflict, "username taken")
			return
		}
		writeErr(w, http.StatusInternalServerError, "could not create user")
		return
	}
	tok, err := IssueToken(h.Secret, u.ID, u.Username)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not issue token")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"user": u, "token": tok})
}

// POST /auth/login { username, password } -> 200 { user, token }
func (h *Handler) Login(w http.ResponseWriter, r *http.Request) {
	if !h.dbOK(w) {
		return
	}
	var c credentials
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&c); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return
	}
	username := strings.TrimSpace(c.Username)
	var u User
	var hash string
	err := h.DB.QueryRow(r.Context(),
		`SELECT id, username, password_hash, created_at::text FROM users WHERE lower(username) = lower($1)`,
		username).Scan(&u.ID, &u.Username, &hash, &u.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, http.StatusUnauthorized, "invalid username or password")
			return
		}
		writeErr(w, http.StatusInternalServerError, "login failed")
		return
	}
	if !VerifyPassword(hash, c.Password) {
		writeErr(w, http.StatusUnauthorized, "invalid username or password")
		return
	}
	tok, err := IssueToken(h.Secret, u.ID, u.Username)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not issue token")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": u, "token": tok})
}

type ctxKey struct{}
type rawKey struct{}

// Authenticate is middleware: Authorization: Bearer <token> -> claims + raw
// token in context. Denied (logged-out/deleted) tokens are rejected here,
// so logout takes effect on every route at once.
func (h *Handler) Authenticate(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.Secret == "" {
			writeErr(w, http.StatusServiceUnavailable, "auth not configured (JWT_SECRET)")
			return
		}
		parts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") || parts[1] == "" {
			writeErr(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		claims, err := ParseToken(h.Secret, parts[1])
		if err != nil {
			writeErr(w, http.StatusUnauthorized, "invalid or expired token")
			return
		}
		if IsDenied(r.Context(), h.Redis, parts[1]) {
			writeErr(w, http.StatusUnauthorized, "logged out")
			return
		}
		ctx := context.WithValue(r.Context(), ctxKey{}, claims)
		ctx = context.WithValue(ctx, rawKey{}, parts[1])
		next(w, r.WithContext(ctx))
	}
}

// TokenFrom returns the raw bearer token, if present.
func TokenFrom(r *http.Request) (string, bool) {
	t, ok := r.Context().Value(rawKey{}).(string)
	return t, ok && t != ""
}

// ClaimsFrom returns the authenticated claims, if present.
func ClaimsFrom(r *http.Request) (*Claims, bool) {
	c, ok := r.Context().Value(ctxKey{}).(*Claims)
	return c, ok
}

// GET /me -> 200 { user } (requires bearer token)
func (h *Handler) Me(w http.ResponseWriter, r *http.Request) {
	if !h.dbOK(w) {
		return
	}
	claims, ok := ClaimsFrom(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	var u User
	err := h.DB.QueryRow(r.Context(),
		`SELECT id, username, created_at::text FROM users WHERE id = $1`, claims.UserID).Scan(&u.ID, &u.Username, &u.CreatedAt)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "user no longer exists")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": u})
}
