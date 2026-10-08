package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// Config holds everything TASK-0 needs. Later tasks extend this.
type Config struct {
	Port            string
	DatabaseURL     string
	RedisURL        string
	JWTSecret       string
	PresenceTTL     int    // seconds a heartbeat keeps you online (default 60)
	PresenceSweep   int    // reaper interval seconds (default 10)
	PresenceGrace   int    // seconds after last-socket disconnect before offline teardown (default 10, 0 = reaper only)
	RateLimits      bool   // false disables all rate limits (dev); default true
	CORSOrigins     string // comma-separated allowed browser origins
	FeedMaxPosts    int    // cap of live posts in feed:global (default 300)
	FeedBatchMinSec int64  // shortest batch window: quiet crowds stay fresh (default 15)
	FeedBatchMaxSec int64  // longest batch window: packed crowds stay efficient (default 300)
	S3Bucket        string
	S3Region        string
	S3EndpointURL   string
	S3PublicURL     string
	S3AccessKeyID   string
	S3SecretKey     string
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// getenvBool parses 1/true/yes/on (default when unset); anything else disables.
func getenvBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// LoadDotEnv loads a very small subset of dotenv (KEY=VALUE, ignores # comments).
// Missing file is fine — env vars / real environment win.
func LoadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		kv := strings.SplitN(line, "=", 2)
		if len(kv) != 2 {
			continue
		}
		k := strings.TrimSpace(kv[0])
		v := strings.TrimSpace(kv[1])
		v = strings.Trim(v, `"'`)
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}

func Load() Config {
	return Config{
		Port:            getenv("PORT", "8080"),
		DatabaseURL:     getenv("DATABASE_URL", ""),
		RedisURL:        getenv("REDIS_URL", ""),
		JWTSecret:       getenv("JWT_SECRET", ""),
		PresenceTTL:     getenvInt("PRESENCE_TTL_SEC", 60),
		PresenceSweep:   getenvInt("PRESENCE_SWEEP_SEC", 10),
		PresenceGrace:   getenvInt("PRESENCE_GRACE_SEC", 10),
		RateLimits:      getenvBool("RATE_LIMITS_ENABLED", true),
		CORSOrigins:     getenv("CORS_ORIGINS", "http://localhost:3000"),
		FeedMaxPosts:    getenvInt("FEED_MAX_POSTS", 300),
		FeedBatchMinSec: int64(getenvInt("FEED_BATCH_MIN_SEC", 15)),
		FeedBatchMaxSec: int64(getenvInt("FEED_BATCH_MAX_SEC", 300)),
		S3Bucket:        getenv("S3_BUCKET", ""),
		S3Region:        getenv("S3_REGION", "auto"),
		S3EndpointURL:   getenv("S3_ENDPOINT_URL", ""),
		S3PublicURL:     getenv("S3_PUBLIC_URL", ""),
		S3AccessKeyID:   getenv("S3_ACCESS_KEY_ID", ""),
		S3SecretKey:     getenv("S3_SECRET_ACCESS_KEY", ""),
	}
}
