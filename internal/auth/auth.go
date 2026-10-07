package auth

import (
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

var validUsername = regexp.MustCompile(`^[a-zA-Z0-9_]{3,32}$`)

var (
	ErrBadUsername = errors.New("username must be 3-32 chars: letters, numbers, underscore")
	ErrBadPassword = errors.New("password must be at least 8 characters")
)

// NormalizeUsername trims and validates. Usernames are case-insensitively
// unique (see users_username_unique index); we keep the original casing.
func NormalizeUsername(raw string) (string, error) {
	u := strings.TrimSpace(raw)
	if !validUsername.MatchString(u) {
		return "", ErrBadUsername
	}
	return u, nil
}

func CheckPassword(raw string) error {
	if len(raw) < 8 {
		return ErrBadPassword
	}
	return nil
}

func HashPassword(raw string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(raw), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

func VerifyPassword(hash, raw string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(raw)) == nil
}

type Claims struct {
	UserID   int64  `json:"uid"`
	Username string `json:"un"`
	jwt.RegisteredClaims
}

// IssueToken mints an HS256 token, 7 day expiry (v1; refresh flow is later work).
func IssueToken(secret string, userID int64, username string) (string, error) {
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, Claims{
		UserID:   userID,
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   strings.TrimSpace(strings.ToLower(username)),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(7 * 24 * time.Hour)),
		},
	})
	return tok.SignedString([]byte(secret))
}

// ParseToken validates and returns the claims.
func ParseToken(secret, raw string) (*Claims, error) {
	tok, err := jwt.ParseWithClaims(raw, &Claims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return []byte(secret), nil
	})
	if err != nil {
		return nil, err
	}
	claims, ok := tok.Claims.(*Claims)
	if !ok || !tok.Valid {
		return nil, errors.New("invalid token")
	}
	return claims, nil
}
