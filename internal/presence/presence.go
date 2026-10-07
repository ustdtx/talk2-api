package presence

// Tracker keeps live presence in Redis Cluster (shared across pods).
//
//   - presence:{uid} HASH {username, last_heartbeat} with TTL (default 60s).
//   - presence:online SET of uids (index for listing + reaping).
//
// Offline = socket gone AND heartbeat stale. The reaper sweeps every N
// seconds; whoever has no fresh key gets MarkOffline + OfflineHook.
// Heartbeats landing mid-sweep self-heal (client re-marks online).
// Wipe tasks (posts/DMs) plug into OfflineHook later.

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Member struct {
	UserID   int64  `json:"user_id"`
	Username string `json:"username"`
}

// OfflineHook fires once per user when they go offline.
// Later tasks attach content-wipe sagas here (plan.md section 6).
type OfflineHook func(ctx context.Context, userID int64, username string)

type Tracker struct {
	rdb   *redis.Client
	ttl   time.Duration
	sweep time.Duration
	hook  OfflineHook
}

func key(uid int64) string { return "presence:" + strconv.FormatInt(uid, 10) }

// nameKey outlives the heartbeat key so the offline event still knows
// who left even when the reaper fires after expiry.
func nameKey(uid int64) string { return "presence:name:" + strconv.FormatInt(uid, 10) }

const onlineSet = "presence:online"

func New(rdb *redis.Client, ttl, sweep time.Duration, hook OfflineHook) *Tracker {
	return &Tracker{rdb: rdb, ttl: ttl, sweep: sweep, hook: hook}
}

// MarkOnline registers/refreshes a user (connect + heartbeat share this).
func (t *Tracker) MarkOnline(ctx context.Context, uid int64, username string) error {
	now := time.Now().Unix()
	pipe := t.rdb.Pipeline()
	pipe.HSet(ctx, key(uid), "username", username, "last_heartbeat", now)
	pipe.Expire(ctx, key(uid), t.ttl)
	pipe.Set(ctx, nameKey(uid), username, 2*t.ttl)
	pipe.SAdd(ctx, onlineSet, uid)
	_, err := pipe.Exec(ctx)
	return err
}

// Heartbeat refreshes only if the user is known (unknown uid -> false,
// so stray/old tokens can't resurrect presence).
func (t *Tracker) Heartbeat(ctx context.Context, uid int64) (bool, error) {
	exists, err := t.rdb.Exists(ctx, key(uid)).Result()
	if err != nil || exists == 0 {
		return false, err
	}
	now := time.Now().Unix()
	pipe := t.rdb.Pipeline()
	pipe.HSet(ctx, key(uid), "last_heartbeat", now)
	pipe.Expire(ctx, key(uid), t.ttl)
	pipe.Expire(ctx, nameKey(uid), 2*t.ttl)
	pipe.SAdd(ctx, onlineSet, uid)
	_, err = pipe.Exec(ctx)
	return err == nil, err
}

func (t *Tracker) IsOnline(ctx context.Context, uid int64) (bool, error) {
	n, err := t.rdb.Exists(ctx, key(uid)).Result()
	return n > 0, err
}

// MarkOffline removes presence. Returns the last known username ("" if none).
func (t *Tracker) MarkOffline(ctx context.Context, uid int64) string {
	username, _ := t.rdb.Get(ctx, nameKey(uid)).Result()
	if username == "" {
		username, _ = t.rdb.HGet(ctx, key(uid), "username").Result()
	}
	pipe := t.rdb.Pipeline()
	pipe.Del(ctx, key(uid))
	pipe.Del(ctx, nameKey(uid))
	pipe.SRem(ctx, onlineSet, uid)
	_, _ = pipe.Exec(ctx)
	return username
}

func (t *Tracker) ListOnline(ctx context.Context) ([]Member, error) {
	ids, err := t.rdb.SMembers(ctx, onlineSet).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Member, 0, len(ids))
	for _, s := range ids {
		uid, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			continue
		}
		m, err := t.rdb.HGetAll(ctx, key(uid)).Result()
		if err != nil || len(m) == 0 {
			continue // stale set entry; reaper cleans it
		}
		out = append(out, Member{UserID: uid, Username: m["username"]})
	}
	return out, nil
}

// RunReaper loops until ctx is done: anyone without a fresh key goes offline.
func (t *Tracker) RunReaper(ctx context.Context) {
	ticker := time.NewTicker(t.sweep)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.sweepOnce(ctx)
		}
	}
}

func (t *Tracker) sweepOnce(ctx context.Context) {
	ids, err := t.rdb.SMembers(ctx, onlineSet).Result()
	if err != nil || len(ids) == 0 {
		return
	}
	for _, s := range ids {
		uid, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			continue
		}
		// The existence check and removal must be one Redis operation.
		// Otherwise a heartbeat between the check and MarkOffline can be
		// incorrectly treated as an offline transition.
		removed, err := t.rdb.Eval(ctx, `
			local presence = KEYS[1]
			local name = KEYS[2]
			local online = KEYS[3]
			if redis.call("EXISTS", presence) == 1 then
				return 0
			end
			local username = redis.call("GET", name) or ""
			redis.call("DEL", presence, name)
			redis.call("SREM", online, ARGV[1])
			return username
		`, []string{key(uid), nameKey(uid), onlineSet}, sValue(uid)).Result()
		if err != nil {
			continue
		}
		username, ok := removed.(string)
		if !ok {
			continue
		}
		if t.hook != nil {
			t.hook(ctx, uid, username)
		}
	}
}

func sValue(uid int64) string {
	return strconv.FormatInt(uid, 10)
}

// Client builds a go-redis client from a redis:// or rediss:// URL.
// Returns nil when url is "" (caller runs degraded).
func Client(redisURL string) *redis.Client {
	if redisURL == "" {
		return nil
	}
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil
	}
	return redis.NewClient(opt)
}
