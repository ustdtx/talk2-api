package room

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"talk2-api/internal/presence"
)

var (
	ErrOffline   = errors.New("you are offline: connect the socket or POST /presence/heartbeat first")
	ErrNotFound  = errors.New("room not found")
	ErrNotMember = errors.New("you are not in this room")
	ErrEmptyName = errors.New("room name cannot be empty")
)

const messageCap = 500

type Room struct {
	ID              int64  `json:"id"`
	Name            string `json:"name"`
	CreatorID       int64  `json:"creator_id"`
	CreatorUsername string `json:"creator_username"`
	MemberCount     int    `json:"member_count"`
}

type Message struct {
	ID             int64  `json:"id"`
	RoomID         int64  `json:"room_id"`
	AuthorID       int64  `json:"author_id"`
	AuthorUsername string `json:"author_username"`
	Text           string `json:"text"`
	CreatedAt      string `json:"created_at"`
}

type WipeResult struct {
	RoomIDs         []int64
	DeletedRooms    []int64
	DeletedMessages []MessageRef
}

type MessageRef struct {
	RoomID    int64 `json:"room_id"`
	MessageID int64 `json:"message_id"`
}

type Store struct {
	rdb      *redis.Client
	presence *presence.Tracker
}

func NewStore(rdb *redis.Client, p *presence.Tracker) *Store {
	return &Store{rdb: rdb, presence: p}
}

func roomKey(id int64) string       { return fmt.Sprintf("room:%d", id) }
func membersKey(id int64) string    { return fmt.Sprintf("room:%d:members", id) }
func messagesKey(id int64) string   { return fmt.Sprintf("room:%d:messages", id) }
func userRoomsKey(uid int64) string { return fmt.Sprintf("rooms:by_user:%d", uid) }
func messageKey(id int64) string    { return fmt.Sprintf("roommsg:%d", id) }
func pad(id int64) string           { return fmt.Sprintf("%019d", id) }

func (s *Store) requireOnline(ctx context.Context, uid int64) error {
	ok, err := s.presence.IsOnline(ctx, uid)
	if err != nil || !ok {
		return ErrOffline
	}
	return nil
}

func (s *Store) Create(ctx context.Context, uid int64, username, name string) (*Room, error) {
	if err := s.requireOnline(ctx, uid); err != nil {
		return nil, err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, ErrEmptyName
	}
	id, err := s.rdb.Incr(ctx, "room:seq").Result()
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, roomKey(id), map[string]any{
		"name": name, "creator_id": uid, "creator_username": username, "created_at": now,
	})
	pipe.ZAdd(ctx, "rooms:all", redis.Z{Score: float64(now), Member: pad(id)})
	pipe.SAdd(ctx, membersKey(id), uid)
	pipe.SAdd(ctx, userRoomsKey(uid), id)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	return &Room{ID: id, Name: name, CreatorID: uid, CreatorUsername: username, MemberCount: 1}, nil
}

func (s *Store) hydrate(ctx context.Context, id int64) (*Room, error) {
	h, err := s.rdb.HGetAll(ctx, roomKey(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(h) == 0 {
		return nil, ErrNotFound
	}
	creator, _ := strconv.ParseInt(h["creator_id"], 10, 64)
	count, _ := s.rdb.SCard(ctx, membersKey(id)).Result()
	if count == 0 {
		return nil, ErrNotFound
	}
	return &Room{ID: id, Name: h["name"], CreatorID: creator, CreatorUsername: h["creator_username"], MemberCount: int(count)}, nil
}

func (s *Store) Get(ctx context.Context, id int64) (*Room, error) {
	return s.hydrate(ctx, id)
}

func (s *Store) List(ctx context.Context) ([]Room, error) {
	ids, err := s.rdb.ZRevRange(ctx, "rooms:all", 0, -1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Room, 0, len(ids))
	for _, raw := range ids {
		id, err := strconv.ParseInt(strings.TrimLeft(raw, "0"), 10, 64)
		if err != nil || id == 0 {
			continue
		}
		r, err := s.hydrate(ctx, id)
		if err == nil {
			out = append(out, *r)
		}
	}
	return out, nil
}

func (s *Store) Join(ctx context.Context, uid, id int64) (*Room, error) {
	if err := s.requireOnline(ctx, uid); err != nil {
		return nil, err
	}
	if _, err := s.hydrate(ctx, id); err != nil {
		return nil, err
	}
	pipe := s.rdb.Pipeline()
	pipe.SAdd(ctx, membersKey(id), uid)
	pipe.SAdd(ctx, userRoomsKey(uid), id)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	return s.hydrate(ctx, id)
}

func (s *Store) GetMessages(ctx context.Context, uid, id int64) ([]Message, error) {
	if err := s.requireOnline(ctx, uid); err != nil {
		return nil, err
	}
	if ok, _ := s.rdb.SIsMember(ctx, membersKey(id), uid).Result(); !ok {
		return nil, ErrNotMember
	}
	ids, err := s.rdb.ZRange(ctx, messagesKey(id), -messageCap, -1).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Message, 0, len(ids))
	for _, raw := range ids {
		mid, err := strconv.ParseInt(strings.TrimLeft(raw, "0"), 10, 64)
		if err != nil || mid == 0 {
			continue
		}
		h, _ := s.rdb.HGetAll(ctx, messageKey(mid)).Result()
		if len(h) == 0 {
			continue
		}
		author, _ := strconv.ParseInt(h["author_id"], 10, 64)
		created, _ := strconv.ParseInt(h["created_at"], 10, 64)
		out = append(out, Message{ID: mid, RoomID: id, AuthorID: author, AuthorUsername: h["author_username"], Text: h["text"], CreatedAt: time.UnixMilli(created).UTC().Format(time.RFC3339)})
	}
	return out, nil
}

func (s *Store) Send(ctx context.Context, uid int64, username string, roomID int64, text string) (*Message, error) {
	if err := s.requireOnline(ctx, uid); err != nil {
		return nil, err
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, errors.New("text cannot be empty")
	}
	if ok, _ := s.rdb.SIsMember(ctx, membersKey(roomID), uid).Result(); !ok {
		return nil, ErrNotMember
	}
	id, err := s.rdb.Incr(ctx, "roommsg:seq").Result()
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	pipe := s.rdb.Pipeline()
	pipe.HSet(ctx, messageKey(id), map[string]any{"room_id": roomID, "author_id": uid, "author_username": username, "text": text, "created_at": now})
	pipe.ZAdd(ctx, messagesKey(roomID), redis.Z{Score: float64(now), Member: pad(id)})
	pipe.SAdd(ctx, fmt.Sprintf("roommsgs:by_author:%d", uid), id)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	return &Message{ID: id, RoomID: roomID, AuthorID: uid, AuthorUsername: username, Text: text, CreatedAt: time.UnixMilli(now).UTC().Format(time.RFC3339)}, nil
}

func (s *Store) Leave(ctx context.Context, uid, id int64) (*WipeResult, error) {
	if ok, _ := s.rdb.SIsMember(ctx, membersKey(id), uid).Result(); !ok {
		return nil, ErrNotMember
	}
	res := &WipeResult{RoomIDs: []int64{id}}
	s.removeUserMessages(ctx, uid, id, res)
	s.removeMembership(ctx, uid, id)
	s.removeIfEmpty(ctx, id, res)
	return res, nil
}

func (s *Store) removeUserMessages(ctx context.Context, uid, roomID int64, res *WipeResult) {
	ids, _ := s.rdb.SMembers(ctx, fmt.Sprintf("roommsgs:by_author:%d", uid)).Result()
	for _, raw := range ids {
		mid, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			continue
		}
		h, _ := s.rdb.HGetAll(ctx, messageKey(mid)).Result()
		if h["room_id"] != strconv.FormatInt(roomID, 10) {
			continue
		}
		s.rdb.ZRem(ctx, messagesKey(roomID), pad(mid))
		s.rdb.Del(ctx, messageKey(mid))
		s.rdb.SRem(ctx, fmt.Sprintf("roommsgs:by_author:%d", uid), mid)
		res.DeletedMessages = append(res.DeletedMessages, MessageRef{RoomID: roomID, MessageID: mid})
	}
}

func (s *Store) removeMembership(ctx context.Context, uid, id int64) {
	s.rdb.SRem(ctx, membersKey(id), uid)
	s.rdb.SRem(ctx, userRoomsKey(uid), id)
}

func (s *Store) removeIfEmpty(ctx context.Context, id int64, res *WipeResult) {
	n, _ := s.rdb.SCard(ctx, membersKey(id)).Result()
	if n != 0 {
		return
	}
	ids, _ := s.rdb.ZRange(ctx, messagesKey(id), 0, -1).Result()
	for _, raw := range ids {
		mid, err := strconv.ParseInt(strings.TrimLeft(raw, "0"), 10, 64)
		if err != nil || mid == 0 {
			continue
		}
		h, _ := s.rdb.HGetAll(ctx, messageKey(mid)).Result()
		if author, err := strconv.ParseInt(h["author_id"], 10, 64); err == nil {
			s.rdb.SRem(ctx, fmt.Sprintf("roommsgs:by_author:%d", author), mid)
		}
		s.rdb.Del(ctx, messageKey(mid))
	}
	s.rdb.Del(ctx, roomKey(id), membersKey(id), messagesKey(id))
	s.rdb.ZRem(ctx, "rooms:all", pad(id))
	res.DeletedRooms = append(res.DeletedRooms, id)
}

// WipeUser removes only this user's messages and membership in every room.
func (s *Store) WipeUser(ctx context.Context, uid int64) (*WipeResult, error) {
	ids, err := s.rdb.SMembers(ctx, userRoomsKey(uid)).Result()
	if err != nil {
		return nil, err
	}
	res := &WipeResult{}
	for _, raw := range ids {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			continue
		}
		res.RoomIDs = append(res.RoomIDs, id)
		s.removeUserMessages(ctx, uid, id, res)
		s.removeMembership(ctx, uid, id)
		s.removeIfEmpty(ctx, id, res)
	}
	s.rdb.Del(ctx, userRoomsKey(uid), fmt.Sprintf("roommsgs:by_author:%d", uid))
	return res, nil
}
