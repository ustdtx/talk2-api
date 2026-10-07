package auth

// Session end: logout denies the token, tears down presence immediately,
// and kills the user's sockets. Account deletion does all that plus
// removes the user row (ephemeral content dies via the same wipe path).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

func denyKey(raw string) string {
	return DeniedKey(raw)
}

// DeniedKey is the Redis key blacklisting a logged-out token.
// Shared with realtime so sockets reject denied tokens at upgrade.
func DeniedKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return "auth:denied:" + hex.EncodeToString(sum[:])
}

// DenyToken blacklists a token until its natural expiry.
func DenyToken(ctx context.Context, rdb *redis.Client, secret, raw string) error {
	if rdb == nil || raw == "" {
		return nil
	}
	claims, err := ParseToken(secret, raw)
	if err != nil {
		return nil // already invalid; nothing to deny
	}
	ttl := time.Until(claims.ExpiresAt.Time)
	if ttl < time.Second {
		ttl = time.Second
	}
	return rdb.Set(ctx, denyKey(raw), 1, ttl).Err()
}

// IsDenied reports whether a token was logged out / deleted.
func IsDenied(ctx context.Context, rdb *redis.Client, raw string) bool {
	if rdb == nil || raw == "" {
		return false
	}
	n, _ := rdb.Exists(ctx, denyKey(raw)).Result()
	return n > 0
}

// Logout: deny token + immediate offline teardown + socket kill.
// Requires auth middleware (needs claims + raw token).
func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	claims, _ := ClaimsFrom(r)
	raw, _ := TokenFrom(r)
	_ = DenyToken(r.Context(), h.Redis, h.Secret, raw)
	if h.OnLogout != nil {
		h.OnLogout(r.Context(), claims.UserID, claims.Username)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type deleteBody struct {
	Password string `json:"password"`
}

// DeleteAccount verifies the password, then wipes everything: ephemeral
// content (broadcast retracts), the user row, the token, and live sockets.
func (h *Handler) DeleteAccount(w http.ResponseWriter, r *http.Request) {
	if !h.dbOK(w) {
		return
	}
	claims, _ := ClaimsFrom(r)
	raw, _ := TokenFrom(r)
	var b deleteBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return
	}
	var hash string
	var username string
	err := h.DB.QueryRow(r.Context(),
		`SELECT username, password_hash FROM users WHERE id = $1`,
		claims.UserID).Scan(&username, &hash)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "user no longer exists")
		return
	}
	if !VerifyPassword(hash, b.Password) {
		writeErr(w, http.StatusUnauthorized, "invalid password")
		return
	}
	// Content first (retract broadcasts), then identity, then token.
	if h.OnLogout != nil {
		h.OnLogout(r.Context(), claims.UserID, username)
	}
	if _, err := h.DB.Exec(r.Context(), `DELETE FROM users WHERE id = $1`, claims.UserID); err != nil {
		writeErr(w, http.StatusInternalServerError, "could not delete account")
		return
	}
	_ = DenyToken(r.Context(), h.Redis, h.Secret, raw)
	writeJSON(w, http.StatusOK, map[string]string{"deleted": username})
}
