package media

// Tracking aligns with plan.md section 11:
//   media:{key} -> HASH {author_id, parent_type, parent_id, created_at}
//   media:by_author:{uid} SET of keys (already used by wipe paths)
//
// Uploads land as parent=pending; attach (CreatePost/AddComment/DM send)
// marks parent=posts|comments|dm. The sweeper deletes pending
// tmp/ keys older than 24h (orphan guard: uploaded but never attached).
// R2 lifecycle on prefix tmp/ should mirror this in Cloudflare as backup.

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// OrphanAge is when a pending tmp/ upload counts as orphaned.
	OrphanAge = 24 * time.Hour
	// metaTTL bounds tracking memory; 26h slightly exceeds OrphanAge so the
	// sweeper sees the entry before Redis expires it.
	metaTTL = 26 * time.Hour
)

func metaKey(objectKey string) string { return "media:" + objectKey }

// TrackPending records an upload with no parent yet. Idempotent.
func TrackPending(ctx context.Context, rdb *redis.Client, authorID int64, objectKey string) {
	if rdb == nil || objectKey == "" {
		return
	}
	now := time.Now().UnixMilli()
	pipe := rdb.Pipeline()
	pipe.HSet(ctx, metaKey(objectKey), map[string]any{
		"author_id":   strconv.FormatInt(authorID, 10),
		"parent_type": "pending",
		"parent_id":   "",
		"created_at":  now,
	})
	pipe.Expire(ctx, metaKey(objectKey), metaTTL)
	pipe.SAdd(ctx, "media:by_author:"+strconv.FormatInt(authorID, 10), objectKey)
	_, _ = pipe.Exec(ctx)
}

// MarkAttached points tracked keys at their parent entity.
// parentType is one of posts|comments|dm; parentID is the entity id.
func MarkAttached(ctx context.Context, rdb *redis.Client, objectKey, parentType, parentID string) {
	if rdb == nil || objectKey == "" {
		return
	}
	pipe := rdb.Pipeline()
	pipe.HSet(ctx, metaKey(objectKey), map[string]any{
		"parent_type": parentType,
		"parent_id":   parentID,
	})
	pipe.Expire(ctx, metaKey(objectKey), metaTTL)
	_, _ = pipe.Exec(ctx)
}

// Untrack removes index entries for deleted keys (R2 delete happens via Storage).
func Untrack(ctx context.Context, rdb *redis.Client, authorID int64, keys []string) {
	if rdb == nil || len(keys) == 0 {
		return
	}
	pipe := rdb.Pipeline()
	for _, k := range keys {
		pipe.Del(ctx, metaKey(k))
		if authorID != 0 {
			pipe.SRem(ctx, "media:by_author:"+strconv.FormatInt(authorID, 10), k)
		}
	}
	_, _ = pipe.Exec(ctx)
}

// SweepPendingOrphans deletes pending tmp/ uploads older than OrphanAge.
// Returns deleted keys. Attached keys (parent != pending) are never touched,
// even when old — their lifetime follows the parent entity + offline wipe.
func SweepPendingOrphans(ctx context.Context, rdb *redis.Client, store Storage) []string {
	if rdb == nil || store == nil {
		return nil
	}
	cutoff := time.Now().Add(-OrphanAge).UnixMilli()
	var cursor uint64
	var deleted []string
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "media:tmp/*", 200).Result()
		if err != nil {
			return deleted
		}
		for _, mk := range keys {
			hm, _ := rdb.HGetAll(ctx, mk).Result()
			if len(hm) == 0 {
				continue
			}
			if hm["parent_type"] != "" && hm["parent_type"] != "pending" {
				continue
			}
			created, _ := strconv.ParseInt(hm["created_at"], 10, 64)
			if created == 0 || created > cutoff {
				continue
			}
			// mk is "media:{objectKey}" -> strip prefix.
			objectKey := mk[len("media:"):]
			aid, _ := strconv.ParseInt(hm["author_id"], 10, 64)
			_ = store.Delete(ctx, objectKey)
			Untrack(ctx, rdb, aid, []string{objectKey})
			deleted = append(deleted, objectKey)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return deleted
}
