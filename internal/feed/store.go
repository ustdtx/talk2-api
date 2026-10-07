package feed

// Store holds all ephemeral social data in Redis Cluster.
// Postgres keeps users only; everything here dies on offline (plan.md section 6).
//
// Keys:
//   post:{id}            HASH {author_id, author_name, text, images(json), created_at}
//   feed:global          ZSET score=created_ms member=zero-padded post id
//   post:{id}:comments   ZSET score=created_ms member=zero-padded comment id
//   comment:{id}         HASH {post_id, author_id, author_name, text, images(json), created_at}
//   post:seq / comment:seq  INCR id generators
//   posts:by_author:{uid} / comments:by_author:{uid}  SETs for O(1) offline wipe
//   media:by_author:{uid} SET of R2 keys (posts+comments; DMs add theirs later)
//   rate:post:{uid}      SET NX EX 300  (1 post / 5 min, plan decision 5)
//   rate:comment:{uid}   SET NX EX 10   (anti-spam; modest, noted in TASK-3 report)

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
	ErrNotFound    = errors.New("not found")
	ErrGone        = errors.New("post is gone (author offline or deleted)")
	ErrForbidden   = errors.New("not yours")
)

const (
	maxImages   = 4
	postRateTTL = 5 * time.Minute
	cmtRateTTL  = 10 * time.Second
	// feedBatch is the Redis window per scan round. ListFeed pages through
	// the whole feed:global ZSET until limit is filled or the index is
	// exhausted, so there is no fixed feed size limit (plan decision 5).
	feedBatch = 200
	// commentCap bounds a single thread read (live window). Threads are
	// small; 1000 covers discussion without unbounded reads.
	commentCap = 1000
)

type Image struct {
	Key string `json:"key"`
	URL string `json:"url"`
}

type Post struct {
	ID             int64   `json:"id"`
	AuthorID       int64   `json:"author_id"`
	AuthorUsername string  `json:"author_username"`
	Text           string  `json:"text"`
	Images         []Image `json:"images"`
	CreatedAt      string  `json:"created_at"`
	CommentCount   int     `json:"comment_count,omitempty"`
}

type Comment struct {
	ID             int64   `json:"id"`
	PostID         int64   `json:"post_id"`
	AuthorID       int64   `json:"author_id"`
	AuthorUsername string  `json:"author_username"`
	Text           string  `json:"text"`
	Images         []Image `json:"images"`
	CreatedAt      string  `json:"created_at"`
	// Facebook-style threading: replies nest one level under a top-level
	// comment. ParentID is the top-level comment (0 = top-level itself);
	// ReplyTo names the direct target ("Replying to @x").
	ParentID    int64  `json:"parent_id,omitempty"`
	ReplyToID   int64  `json:"reply_to_id,omitempty"`
	ReplyToName string `json:"reply_to_name,omitempty"`
}

// ThreadedComment is a top-level comment with its replies (oldest first).
type ThreadedComment struct {
	Comment
	Replies []Comment `json:"replies"`
}

// WipeResult lists what an offline wipe removed (caller broadcasts retracts).
type WipeResult struct {
	Posts    []int64
	Comments []CommentRef
}

type CommentRef struct {
	ID     int64 `json:"comment_id"`
	PostID int64 `json:"post_id"`
}

type Store struct {
	rdb      *redis.Client
	presence *presence.Tracker
	media    media.Storage
	// rateLimits gates the post/comment throttles (RATE_LIMITS_ENABLED).
	// Dev sets false; prod keeps true. The checks stay in code, just skipped.
	rateLimits bool
}

func NewStore(rdb *redis.Client, p *presence.Tracker, m media.Storage, rateLimits bool) *Store {
	return &Store{rdb: rdb, presence: p, media: m, rateLimits: rateLimits}
}

func pad(id int64) string { return fmt.Sprintf("%019d", id) }

func ms() int64 { return time.Now().UnixMilli() }

func iso(millis int64) string { return time.UnixMilli(millis).UTC().Format(time.RFC3339) }

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

func imageKeys(imgs []Image) []string {
	out := make([]string, 0, len(imgs))
	for _, im := range imgs {
		if im.Key != "" {
			out = append(out, im.Key)
		}
	}
	return out
}

func checkText(text string) (string, error) {
	t := strings.TrimSpace(text)
	if t == "" {
		return "", errors.New("text cannot be empty")
	}
	return t, nil
}

// checkImages enforces count + ownership (keys must live under the author's
// own tmp/ prefix; entity-slot renames are later work).
func checkImages(keys []string, uid int64) ([]string, error) {
	if len(keys) > maxImages {
		return nil, errors.New("max 4 images per post/comment")
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

func (s *Store) requireOnline(ctx context.Context, uid int64) error {
	ok, err := s.presence.IsOnline(ctx, uid)
	if err != nil || !ok {
		return ErrOffline
	}
	return nil
}

// onlineSet returns the currently-online user ids for filtering reads.
func (s *Store) onlineSet(ctx context.Context) map[int64]bool {
	members, err := s.presence.ListOnline(ctx)
	if err != nil {
		return map[int64]bool{}
	}
	set := make(map[int64]bool, len(members))
	for _, m := range members {
		set[m.UserID] = true
	}
	return set
}

// CreatePost validates, rate-limits, stores, and indexes a post.
// Text may be empty when images are attached (standalone image posts).
func (s *Store) CreatePost(ctx context.Context, uid int64, username, text string, imgKeys []string) (*Post, error) {
	imgKeys, err := checkImages(imgKeys, uid)
	if err != nil {
		return nil, err
	}
	t := strings.TrimSpace(text)
	if t == "" && len(imgKeys) == 0 {
		return nil, errors.New("say something or attach an image")
	}
	if err := s.requireOnline(ctx, uid); err != nil {
		return nil, err
	}
	if s.rateLimits && !s.rdb.SetNX(ctx, fmt.Sprintf("rate:post:%d", uid), 1, postRateTTL).Val() {
		return nil, ErrRateLimited
	}
	id, err := s.rdb.Incr(ctx, "post:seq").Result()
	if err != nil {
		return nil, err
	}
	now := ms()
	imgs := encodeImages(imgKeys, s.media.PublicURL)
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, fmt.Sprintf("post:%d", id), map[string]any{
		"author_id": id2s(uid), "author_name": username, "text": t,
		"images": imgs, "created_at": now,
	})
	pipe.ZAdd(ctx, "feed:global", redis.Z{Score: float64(now), Member: pad(id)})
	pipe.SAdd(ctx, fmt.Sprintf("posts:by_author:%d", uid), id)
	if len(imgKeys) > 0 {
		pipe.SAdd(ctx, fmt.Sprintf("media:by_author:%d", uid), imgKeys)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	for _, k := range imgKeys {
		media.MarkAttached(ctx, s.rdb, k, "posts", id2s(id))
	}
	return &Post{ID: id, AuthorID: uid, AuthorUsername: username, Text: t,
		Images:    decodeImages(imgs, s.media.PublicURL),
		CreatedAt: iso(now)}, nil
}

func id2s(id int64) string { return strconv.FormatInt(id, 10) }

// ListFeed returns newest-first posts from currently-online authors.
// Full scrollable history (plan decision 5: no feed size limit): pages
// through feed:global until limit is filled or the index is exhausted.
// beforeID is a cursor: only posts with id < beforeID are returned.
func (s *Store) ListFeed(ctx context.Context, limit int, beforeID int64) ([]Post, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	total, err := s.rdb.ZCard(ctx, "feed:global").Result()
	if err != nil {
		return nil, err
	}
	online := s.onlineSet(ctx)
	out := make([]Post, 0, limit)
	var offset int64
	for int64(len(out)) < int64(limit) && offset < total {
		end := offset + feedBatch - 1
		if end >= total {
			end = total - 1
		}
		members, err := s.rdb.ZRevRange(ctx, "feed:global", offset, end).Result()
		if err != nil {
			return nil, err
		}
		if len(members) == 0 {
			break
		}
		offset += int64(len(members))
		for _, m := range members {
			id, err := strconv.ParseInt(strings.TrimLeft(m, "0"), 10, 64)
			if err != nil {
				continue
			}
			if id == 0 {
				continue
			}
			if beforeID > 0 && id >= beforeID {
				continue
			}
			p, ok := s.getPost(ctx, id)
			if !ok {
				continue
			}
			if !online[p.AuthorID] {
				continue
			}
			p.CommentCount = s.commentCount(ctx, id)
			out = append(out, *p)
			if len(out) >= limit {
				break
			}
		}
		// Short page = exhausted.
		if len(members) < feedBatch {
			break
		}
	}
	return out, nil
}

func (s *Store) getPost(ctx context.Context, id int64) (*Post, bool) {
	m, err := s.rdb.HGetAll(ctx, fmt.Sprintf("post:%d", id)).Result()
	if err != nil || len(m) == 0 {
		return nil, false
	}
	uid, _ := strconv.ParseInt(m["author_id"], 10, 64)
	created, _ := strconv.ParseInt(m["created_at"], 10, 64)
	return &Post{ID: id, AuthorID: uid, AuthorUsername: m["author_name"], Text: m["text"],
		Images:    decodeImages(m["images"], s.media.PublicURL),
		CreatedAt: iso(created)}, true
}

// GetPost returns the post only if it exists and its author is online.
func (s *Store) GetPost(ctx context.Context, id int64) (*Post, error) {
	p, ok := s.getPost(ctx, id)
	if !ok {
		return nil, ErrNotFound
	}
	online, err := s.presence.IsOnline(ctx, p.AuthorID)
	if err != nil || !online {
		return nil, ErrGone
	}
	p.CommentCount = s.commentCount(ctx, id)
	return p, nil
}

func (s *Store) commentCount(ctx context.Context, postID int64) int {
	n, _ := s.rdb.ZCard(ctx, fmt.Sprintf("post:%d:comments", postID)).Result()
	return int(n)
}

// DeletePost removes a post + its entire thread (plan decision 2).
// Returns the removed post id and all cascaded comment refs.
func (s *Store) DeletePost(ctx context.Context, id int64, actorUID int64) ([]CommentRef, error) {
	p, ok := s.getPost(ctx, id)
	if !ok {
		return nil, ErrNotFound
	}
	if p.AuthorID != actorUID {
		return nil, ErrForbidden
	}
	return s.deletePostCascade(ctx, p)
}

func (s *Store) deletePostCascade(ctx context.Context, p *Post) ([]CommentRef, error) {
	cids, _ := s.rdb.ZRange(ctx, fmt.Sprintf("post:%d:comments", p.ID), 0, -1).Result()
	refs := make([]CommentRef, 0, len(cids))
	var r2keys []string
	r2keys = append(r2keys, imageKeys(p.Images)...)
	for _, m := range cids {
		cid, err := strconv.ParseInt(strings.TrimLeft(m, "0"), 10, 64)
		if err != nil || cid == 0 {
			continue
		}
		cm, _ := s.rdb.HGetAll(ctx, fmt.Sprintf("comment:%d", cid)).Result()
		if len(cm) == 0 {
			continue
		}
		caid, _ := strconv.ParseInt(cm["author_id"], 10, 64)
		pid, _ := strconv.ParseInt(cm["post_id"], 10, 64)
		refs = append(refs, CommentRef{ID: cid, PostID: pid})
		r2keys = append(r2keys, imageKeys(decodeImages(cm["images"], s.media.PublicURL))...)
		pipe := s.rdb.Pipeline()
		pipe.Del(ctx, fmt.Sprintf("comment:%d", cid))
		pipe.SRem(ctx, fmt.Sprintf("comments:by_author:%d", caid), cid)
		_, _ = pipe.Exec(ctx)
	}
	pipe := s.rdb.Pipeline()
	pipe.Del(ctx, fmt.Sprintf("post:%d:comments", p.ID))
	pipe.ZRem(ctx, "feed:global", pad(p.ID))
	pipe.Del(ctx, fmt.Sprintf("post:%d", p.ID))
	pipe.SRem(ctx, fmt.Sprintf("posts:by_author:%d", p.AuthorID), p.ID)
	if len(r2keys) > 0 {
		pipe.SRem(ctx, fmt.Sprintf("media:by_author:%d", p.AuthorID), r2keys)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}
	if len(r2keys) > 0 {
		_ = s.media.DeleteMany(ctx, r2keys)
		media.Untrack(ctx, s.rdb, p.AuthorID, r2keys)
	}
	return refs, nil
}

// AddComment appends to a live post's thread. Text may be empty when images
// are attached. parentID optionally nests under a top-level comment;
// replying to a reply stays in the same thread, targeted at its author.
func (s *Store) AddComment(ctx context.Context, postID, uid int64, username, text string, imgKeys []string, parentID int64) (*Comment, error) {
	imgKeys, err := checkImages(imgKeys, uid)
	if err != nil {
		return nil, err
	}
	t := strings.TrimSpace(text)
	if t == "" && len(imgKeys) == 0 {
		return nil, errors.New("say something or attach an image")
	}
	if err := s.requireOnline(ctx, uid); err != nil {
		return nil, err
	}
	p, ok := s.getPost(ctx, postID)
	if !ok {
		return nil, ErrNotFound
	}
	paOnline, err := s.presence.IsOnline(ctx, p.AuthorID)
	if err != nil || !paOnline {
		return nil, ErrGone
	}
	var topID, replyToID int64
	var replyToName string
	if parentID != 0 {
		pm, _ := s.rdb.HGetAll(ctx, fmt.Sprintf("comment:%d", parentID)).Result()
		if len(pm) == 0 {
			return nil, errors.New("parent comment not found")
		}
		pPost, _ := strconv.ParseInt(pm["post_id"], 10, 64)
		if pPost != postID {
			return nil, errors.New("parent comment is on another post")
		}
		if pp, _ := strconv.ParseInt(pm["parent_id"], 10, 64); pp != 0 {
			topID = pp // reply-to-reply: stay in the same top-level thread
		} else {
			topID = parentID
		}
		replyToID, _ = strconv.ParseInt(pm["author_id"], 10, 64)
		replyToName = pm["author_name"]
	}
	if s.rateLimits && !s.rdb.SetNX(ctx, fmt.Sprintf("rate:comment:%d", uid), 1, cmtRateTTL).Val() {
		return nil, ErrRateLimited
	}
	id, err := s.rdb.Incr(ctx, "comment:seq").Result()
	if err != nil {
		return nil, err
	}
	now := ms()
	imgs := encodeImages(imgKeys, s.media.PublicURL)
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, fmt.Sprintf("comment:%d", id), map[string]any{
		"post_id": id2s(postID), "author_id": id2s(uid), "author_name": username,
		"text": t, "images": imgs, "created_at": now,
		"parent_id": id2s(topID), "reply_to_id": id2s(replyToID), "reply_to_name": replyToName,
	})
	pipe.ZAdd(ctx, fmt.Sprintf("post:%d:comments", postID), redis.Z{Score: float64(now), Member: pad(id)})
	pipe.SAdd(ctx, fmt.Sprintf("comments:by_author:%d", uid), id)
	if len(imgKeys) > 0 {
		pipe.SAdd(ctx, fmt.Sprintf("media:by_author:%d", uid), imgKeys)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	for _, k := range imgKeys {
		media.MarkAttached(ctx, s.rdb, k, "comments", id2s(id))
	}
	return &Comment{ID: id, PostID: postID, AuthorID: uid, AuthorUsername: username,
		Text: t, Images: decodeImages(imgs, s.media.PublicURL), CreatedAt: iso(now),
		ParentID: topID, ReplyToID: replyToID, ReplyToName: replyToName}, nil
}

// ListComments returns top-level comments oldest-first, each with its
// replies nested (oldest first). Authors offline are filtered out.
// Errors when the post is gone (deleted or author offline).
func (s *Store) ListComments(ctx context.Context, postID int64) ([]ThreadedComment, error) {
	p, ok := s.getPost(ctx, postID)
	if !ok {
		return nil, ErrNotFound
	}
	paOnline, err := s.presence.IsOnline(ctx, p.AuthorID)
	if err != nil || !paOnline {
		return nil, ErrGone
	}
	members, err := s.rdb.ZRange(ctx, fmt.Sprintf("post:%d:comments", postID), 0, int64(commentCap)-1).Result()
	if err != nil {
		return nil, err
	}
	online := s.onlineSet(ctx)
	out := make([]ThreadedComment, 0, len(members))
	byID := map[int64]int{} // top-level id -> index in out
	for _, m := range members {
		cid, err := strconv.ParseInt(strings.TrimLeft(m, "0"), 10, 64)
		if err != nil || cid == 0 {
			continue
		}
		cm, _ := s.rdb.HGetAll(ctx, fmt.Sprintf("comment:%d", cid)).Result()
		if len(cm) == 0 {
			continue
		}
		caid, _ := strconv.ParseInt(cm["author_id"], 10, 64)
		if !online[caid] {
			continue
		}
		pid, _ := strconv.ParseInt(cm["post_id"], 10, 64)
		created, _ := strconv.ParseInt(cm["created_at"], 10, 64)
		parID, _ := strconv.ParseInt(cm["parent_id"], 10, 64)
		rToID, _ := strconv.ParseInt(cm["reply_to_id"], 10, 64)
		c := Comment{ID: cid, PostID: pid, AuthorID: caid,
			AuthorUsername: cm["author_name"], Text: cm["text"],
			Images: decodeImages(cm["images"], s.media.PublicURL), CreatedAt: iso(created),
			ParentID: parID, ReplyToID: rToID, ReplyToName: cm["reply_to_name"]}
		if parID == 0 {
			byID[cid] = len(out)
			out = append(out, ThreadedComment{Comment: c, Replies: []Comment{}})
		} else if idx, ok := byID[parID]; ok {
			out[idx].Replies = append(out[idx].Replies, c)
		}
		// Replies whose top-level was filtered (author offline) are dropped
		// with it: no orphan threads.
	}
	return out, nil
}

// WipeUser deletes everything uid authored: all posts (with full thread
// cascade) then remaining standalone comments. Returns ids for retract events.
// R2 bytes die with the rows (plan section 11).
func (s *Store) WipeUser(ctx context.Context, uid int64) (*WipeResult, error) {
	res := &WipeResult{}
	pids, err := s.rdb.SMembers(ctx, fmt.Sprintf("posts:by_author:%d", uid)).Result()
	if err != nil {
		return nil, err
	}
	for _, sp := range pids {
		pid, err := strconv.ParseInt(sp, 10, 64)
		if err != nil {
			continue
		}
		p, ok := s.getPost(ctx, pid)
		if !ok {
			_, _ = s.rdb.SRem(ctx, fmt.Sprintf("posts:by_author:%d", uid), pid).Result()
			continue
		}
		refs, err := s.deletePostCascade(ctx, p)
		if err != nil {
			continue
		}
		res.Posts = append(res.Posts, pid)
		res.Comments = append(res.Comments, refs...)
	}
	cids, err := s.rdb.SMembers(ctx, fmt.Sprintf("comments:by_author:%d", uid)).Result()
	if err != nil {
		return res, err
	}
	var r2keys []string
	for _, sc := range cids {
		cid, err := strconv.ParseInt(sc, 10, 64)
		if err != nil {
			continue
		}
		cm, _ := s.rdb.HGetAll(ctx, fmt.Sprintf("comment:%d", cid)).Result()
		if len(cm) == 0 {
			_, _ = s.rdb.SRem(ctx, fmt.Sprintf("comments:by_author:%d", uid), cid).Result()
			continue
		}
		pid, _ := strconv.ParseInt(cm["post_id"], 10, 64)
		r2keys = append(r2keys, imageKeys(decodeImages(cm["images"], s.media.PublicURL))...)
		pipe := s.rdb.Pipeline()
		pipe.ZRem(ctx, fmt.Sprintf("post:%d:comments", pid), pad(cid))
		pipe.Del(ctx, fmt.Sprintf("comment:%d", cid))
		pipe.SRem(ctx, fmt.Sprintf("comments:by_author:%d", uid), cid)
		_, _ = pipe.Exec(ctx)
		res.Comments = append(res.Comments, CommentRef{ID: cid, PostID: pid})
	}
	_, _ = s.rdb.Del(ctx, fmt.Sprintf("posts:by_author:%d", uid)).Result()
	_, _ = s.rdb.Del(ctx, fmt.Sprintf("comments:by_author:%d", uid)).Result()
	if len(r2keys) > 0 {
		_, _ = s.rdb.SRem(ctx, fmt.Sprintf("media:by_author:%d", uid), r2keys).Result()
		_ = s.media.DeleteMany(ctx, r2keys)
		media.Untrack(ctx, s.rdb, uid, r2keys)
	}
	_, _ = s.rdb.Del(ctx, fmt.Sprintf("media:by_author:%d", uid)).Result()
	return res, nil
}
