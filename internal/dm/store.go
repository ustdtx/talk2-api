package dm

// Store holds 1:1 DM threads in Redis Cluster (ephemeral, plan.md F5).
//
// A thread exists only while BOTH parties are online (plan decision 3):
// sending requires both online, reading requires both online, and when
// either side goes offline the ENTIRE thread (both directions) is deleted.
//
// Keys:
//   dm:{minID}:{maxID}   ZSET score=created_ms member=zero-padded msg id
//   dmmsg:{id}           HASH {from_id, from_name, to_id, text, images(json), created_at}
//   dmmsg:seq            INCR id generator
//   dm:threads:{uid}     SET of counterpart ids (list + wipe enumeration)
//   rate:dm:{uid}        SET NX EX 3s (gated by RATE_LIMITS_ENABLED)

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"talk2-api/internal/media"
	"talk2-api/internal/presence"
)

var (
	ErrRateLimited = errors.New("rate limited")
	ErrOffline     = errors.New("you are offline: connect the socket or POST /presence/heartbeat first")
	ErrUnreachable = errors.New("unreachable: user is offline (DMs only exist while both are online)")
	ErrSelf        = errors.New("cannot DM yourself")
)

const (
	maxImages = 4
	dmRateTTL = 3 * time.Second
	// threadCap bounds a single live-thread read. Threads vanish on offline
	// so they stay small; 500 covers conversation without unbounded reads.
	threadCap = 500
)

type Image struct {
	Key string `json:"key"`
	URL string `json:"url"`
}

type Message struct {
	ID           int64   `json:"id"`
	FromID       int64   `json:"from_id"`
	FromUsername string  `json:"from_username"`
	ToID         int64   `json:"to_id"`
	Text         string  `json:"text"`
	Images       []Image `json:"images"`
	CreatedAt    string  `json:"created_at"`
}

type Thread struct {
	WithUserID   int64  `json:"with_user_id"`
	WithUsername string `json:"with_username"`
	LastText     string `json:"last_text"`
	LastAt       string `json:"last_at"`
	MessageCount int    `json:"message_count"`
}

type Store struct {
	rdb        *redis.Client
	presence   *presence.Tracker
	media      media.Storage
	rateLimits bool
}

func NewStore(rdb *redis.Client, p *presence.Tracker, m media.Storage, rateLimits bool) *Store {
	return &Store{rdb: rdb, presence: p, media: m, rateLimits: rateLimits}
}

func threadKey(a, b int64) string {
	if a > b {
		a, b = b, a
	}
	return fmt.Sprintf("dm:%d:%d", a, b)
}

func pad(id int64) string { return fmt.Sprintf("%019d", id) }

func checkText(text string) (string, error) {
	t := strings.TrimSpace(text)
	if t == "" {
		return "", errors.New("text cannot be empty")
	}
	return t, nil
}

func checkImages(keys []string, uid int64) ([]string, error) {
	if len(keys) > maxImages {
		return nil, errors.New("max 4 images per message")
	}
	prefix := fmt.Sprintf("tmp/%d/", uid)
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k == "" || !strings.HasPrefix(k, prefix) {
			return nil, errors.New("image keys must be your own uploads (tmp/{your-id}/...)")
		}
	}
	return keys, nil
}

func encodeImages(keys []string, pub func(string) string) string {
	imgs := make([]Image, 0, len(keys))
	for _, k := range keys {
		imgs = append(imgs, Image{Key: k, URL: pub(k)})
	}
	raw, _ := json.Marshal(imgs)
	return string(raw)
}

func decodeImages(raw string, pub func(string) string) []Image {
	if raw == "" {
		return []Image{}
	}
	var imgs []Image
	if err := json.Unmarshal([]byte(raw), &imgs); err != nil {
		return []Image{}
	}
	for i := range imgs {
		if imgs[i].URL == "" && imgs[i].Key != "" {
			imgs[i].URL = pub(imgs[i].Key)
		}
	}
	return imgs
}

// Send stores a message. Both parties must be online, else 422.
func (s *Store) Send(ctx context.Context, fromID int64, fromName string, toID int64, text string, imgKeys []string) (*Message, error) {
	if toID == fromID {
		return nil, ErrSelf
	}
	t, err := checkText(text)
	if err != nil {
		return nil, err
	}
	imgKeys, err = checkImages(imgKeys, fromID)
	if err != nil {
		return nil, err
	}
	if ok, err := s.presence.IsOnline(ctx, fromID); err != nil || !ok {
		return nil, ErrOffline
	}
	if ok, err := s.presence.IsOnline(ctx, toID); err != nil || !ok {
		return nil, ErrUnreachable
	}
	if s.rateLimits && !s.rdb.SetNX(ctx, fmt.Sprintf("rate:dm:%d", fromID), 1, dmRateTTL).Val() {
		return nil, ErrRateLimited
	}
	id, err := s.rdb.Incr(ctx, "dmmsg:seq").Result()
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	imgs := encodeImages(imgKeys, s.media.PublicURL)
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, fmt.Sprintf("dmmsg:%d", id), map[string]any{
		"from_id": id2s(fromID), "from_name": fromName, "to_id": id2s(toID),
		"text": t, "images": imgs, "created_at": now,
	})
	pipe.ZAdd(ctx, threadKey(fromID, toID), redis.Z{Score: float64(now), Member: pad(id)})
	pipe.SAdd(ctx, fmt.Sprintf("dm:threads:%d", fromID), toID)
	pipe.SAdd(ctx, fmt.Sprintf("dm:threads:%d", toID), fromID)
	if len(imgKeys) > 0 {
		pipe.SAdd(ctx, fmt.Sprintf("media:by_author:%d", fromID), imgKeys)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	for _, k := range imgKeys {
		media.MarkAttached(ctx, s.rdb, k, "dm", id2s(id))
	}
	created := time.UnixMilli(now).UTC().Format(time.RFC3339)
	return &Message{ID: id, FromID: fromID, FromUsername: fromName, ToID: toID,
		Text: t, Images: decodeImages(imgs, s.media.PublicURL), CreatedAt: created}, nil
}

func id2s(id int64) string { return strconv.FormatInt(id, 10) }

// onlineNames maps online user id -> username for list filtering.
func (s *Store) onlineNames(ctx context.Context) map[int64]string {
	members, err := s.presence.ListOnline(ctx)
	if err != nil {
		return map[int64]string{}
	}
	out := make(map[int64]string, len(members))
	for _, m := range members {
		out[m.UserID] = m.Username
	}
	return out
}

// ListThreads returns conversations whose counterpart is online, newest first.
func (s *Store) ListThreads(ctx context.Context, uid int64) ([]Thread, error) {
	parts, err := s.rdb.SMembers(ctx, fmt.Sprintf("dm:threads:%d", uid)).Result()
	if err != nil {
		return nil, err
	}
	names := s.onlineNames(ctx)
	type scored struct {
		t  Thread
		ms int64
	}
	var tmp []scored
	for _, sp := range parts {
		vid, err := strconv.ParseInt(sp, 10, 64)
		if err != nil {
			continue
		}
		name, ok := names[vid]
		if !ok {
			continue // counterpart offline: thread invisible (decision 3)
		}
		last, err := s.rdb.ZRevRange(ctx, threadKey(uid, vid), 0, 0).Result()
		if err != nil || len(last) == 0 {
			continue
		}
		mid, err := strconv.ParseInt(strings.TrimLeft(last[0], "0"), 10, 64)
		if err != nil || mid == 0 {
			continue
		}
		hm, _ := s.rdb.HGetAll(ctx, fmt.Sprintf("dmmsg:%d", mid)).Result()
		if len(hm) == 0 {
			continue
		}
		created, _ := strconv.ParseInt(hm["created_at"], 10, 64)
		n, _ := s.rdb.ZCard(ctx, threadKey(uid, vid)).Result()
		tmp = append(tmp, scored{t: Thread{WithUserID: vid, WithUsername: name,
			LastText: hm["text"], LastAt: time.UnixMilli(created).UTC().Format(time.RFC3339),
			MessageCount: int(n)}, ms: created})
	}
	out := make([]Thread, 0, len(tmp))
	// newest first (thread counts are small; simple selection is fine)
	for len(tmp) > 0 {
		best := 0
		for i := 1; i < len(tmp); i++ {
			if tmp[i].ms > tmp[best].ms {
				best = i
			}
		}
		out = append(out, tmp[best].t)
		tmp = append(tmp[:best], tmp[best+1:]...)
	}
	return out, nil
}

// GetThread returns oldest-first messages. Errors when either side is offline.
func (s *Store) GetThread(ctx context.Context, uid, with int64) ([]Message, error) {
	if ok, err := s.presence.IsOnline(ctx, uid); err != nil || !ok {
		return nil, ErrOffline
	}
	if ok, err := s.presence.IsOnline(ctx, with); err != nil || !ok {
		return nil, ErrUnreachable
	}
	members, err := s.rdb.ZRange(ctx, threadKey(uid, with), 0, int64(threadCap)-1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Message, 0, len(members))
	for _, m := range members {
		mid, err := strconv.ParseInt(strings.TrimLeft(m, "0"), 10, 64)
		if err != nil || mid == 0 {
			continue
		}
		hm, _ := s.rdb.HGetAll(ctx, fmt.Sprintf("dmmsg:%d", mid)).Result()
		if len(hm) == 0 {
			continue
		}
		fid, _ := strconv.ParseInt(hm["from_id"], 10, 64)
		tid, _ := strconv.ParseInt(hm["to_id"], 10, 64)
		created, _ := strconv.ParseInt(hm["created_at"], 10, 64)
		out = append(out, Message{ID: mid, FromID: fid, FromUsername: hm["from_name"],
			ToID: tid, Text: hm["text"], Images: decodeImages(hm["images"], s.media.PublicURL),
			CreatedAt: time.UnixMilli(created).UTC().Format(time.RFC3339)})
	}
	return out, nil
}

// WipeUser deletes every 1:1 thread uid participates in - BOTH directions
// (plan decision 3: the thread vanishes entirely). Returns counterpart ids
// so the caller can broadcast dm:thread_retracted to survivors.
func (s *Store) WipeUser(ctx context.Context, uid int64) ([]int64, error) {
	parts, err := s.rdb.SMembers(ctx, fmt.Sprintf("dm:threads:%d", uid)).Result()
	if err != nil {
		return nil, err
	}
	var wiped []int64
	var r2keys []string
	for _, sp := range parts {
		vid, err := strconv.ParseInt(sp, 10, 64)
		if err != nil {
			continue
		}
		tk := threadKey(uid, vid)
		mids, _ := s.rdb.ZRange(ctx, tk, 0, -1).Result()
		for _, m := range mids {
			mid, err := strconv.ParseInt(strings.TrimLeft(m, "0"), 10, 64)
			if err != nil || mid == 0 {
				continue
			}
			hm, _ := s.rdb.HGetAll(ctx, fmt.Sprintf("dmmsg:%d", mid)).Result()
			if len(hm) > 0 {
				var im []Image
				_ = json.Unmarshal([]byte(hm["images"]), &im)
				for _, one := range im {
					if one.Key != "" {
						r2keys = append(r2keys, one.Key)
					}
				}
				_, _ = s.rdb.Del(ctx, fmt.Sprintf("dmmsg:%d", mid)).Result()
			}
		}
		pipe := s.rdb.Pipeline()
		pipe.Del(ctx, tk)
		pipe.SRem(ctx, fmt.Sprintf("dm:threads:%d", vid), uid)
		_, _ = pipe.Exec(ctx)
		wiped = append(wiped, vid)
	}
	_, _ = s.rdb.Del(ctx, fmt.Sprintf("dm:threads:%d", uid)).Result()
	if len(r2keys) > 0 {
		_, _ = s.rdb.SRem(ctx, fmt.Sprintf("media:by_author:%d", uid), r2keys).Result()
		// Thread wipe kills BOTH directions (plan §6): drop counterpart
		// index entries too so no orphan media:by_author entries remain.
		for _, vid := range wiped {
			_, _ = s.rdb.SRem(ctx, fmt.Sprintf("media:by_author:%d", vid), r2keys).Result()
		}
		_ = s.media.DeleteMany(ctx, r2keys)
		for _, k := range r2keys {
			_, _ = s.rdb.Del(ctx, "media:"+k).Result()
		}
	}
	return wiped, nil
}
