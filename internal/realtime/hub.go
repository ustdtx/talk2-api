package realtime

// Hub is the per-process socket registry.
// Single-pod now (dev); multi-pod fan-out moves to NATS in TASK-7,
// the broadcast surface (Broadcast) stays the same.

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"

	"talk2-api/internal/auth"
	"talk2-api/internal/presence"
)

// Wire event names (plan.md section 7 + F2 presence:update alias).
// Canonical mutations are REST; the socket is push + heartbeat.
// REST error mapping covers the error:* contract: 422 unreachable,
// 429 rate_limited, 410 gone (see feed/dm mapStoreErr).
const (
	EvWelcome        = "welcome"
	EvPresenceOnline = "presence:online"
	EvPresenceOffln  = "presence:offline"
	EvPresenceUpdate = "presence:update"
	EvHeartbeatAck   = "heartbeat_ack"
	EvPong           = "pong"
	EvError          = "error"

	EvFeedNewPost          = "feed:new_post"
	EvPostRetracted        = "post:retracted"
	EvCommentNew           = "comment:new"
	EvCommentRetracted     = "comment:retracted"
	EvDMReceive            = "dm:receive"
	EvDMNew                = "dm:new" // compat alias of dm:receive
	EvDMThreadRetr         = "dm:thread_retracted"
	EvRoomCreated          = "room:created"
	EvRoomUpdated          = "room:updated"
	EvRoomDeleted          = "room:deleted"
	EvRoomMessage          = "room:message"
	EvRoomMessageRetracted = "room:message_retracted"
)

type envelope struct {
	Type    string `json:"type"`
	Payload any    `json:"payload,omitempty"`
}

type Client struct {
	UserID   int64
	Username string
	conn     *websocket.Conn
	send     chan []byte
}

type Hub struct {
	mu      sync.Mutex
	clients map[*Client]struct{}
}

func NewHub() *Hub { return &Hub{clients: map[*Client]struct{}{}} }

func (h *Hub) Add(c *Client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) Remove(c *Client) {
	h.mu.Lock()
	delete(h.clients, c)
	close(c.send)
	h.mu.Unlock()
}

func (h *Hub) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// DisconnectUser closes all sockets of a user (logout / account delete).
// Map cleanup happens in each conn's deferred Remove; this only closes.
func (h *Hub) DisconnectUser(uid int64, reason string) {
	h.mu.Lock()
	var targets []*Client
	for c := range h.clients {
		if c.UserID == uid {
			targets = append(targets, c)
		}
	}
	h.mu.Unlock()
	for _, c := range targets {
		_ = c.conn.Close(websocket.StatusNormalClosure, reason)
	}
}

// Broadcast sends to all connected sockets (best-effort, drops slow clients).
func (h *Hub) Broadcast(msgType string, payload any) {
	raw, err := json.Marshal(envelope{Type: msgType, Payload: payload})
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		select {
		case c.send <- raw:
		default:
		}
	}
}

// BroadcastPresenceOnline announces a user coming online, plus the
// presence:update alias (plan F2: GET /presence/online + presence:update).
func (h *Hub) BroadcastPresenceOnline(userID int64, username string) {
	p := map[string]any{"user_id": userID, "username": username}
	h.Broadcast(EvPresenceOnline, p)
	h.Broadcast(EvPresenceUpdate, map[string]any{"online": []any{p}})
}

// BroadcastPresenceOffline announces a user going offline, plus alias.
func (h *Hub) BroadcastPresenceOffline(userID int64, username string) {
	p := map[string]any{"user_id": userID, "username": username}
	h.Broadcast(EvPresenceOffln, p)
	h.Broadcast(EvPresenceUpdate, map[string]any{"offline": []any{p}})
}

func sendTo(c *Client, msgType string, payload any) {
	raw, err := json.Marshal(envelope{Type: msgType, Payload: payload})
	if err != nil {
		return
	}
	select {
	case c.send <- raw:
	default:
	}
}

// Handler owns the /ws upgrade + read loop.
type Handler struct {
	Hub     *Hub
	Tracker *presence.Tracker
	Secret  string
	Redis   *redis.Client
}

// ServeWS upgrades with JWT auth (query ?token= for browsers, else
// Authorization header). Origin is open for dev; lock down for prod.
func (h *Handler) ServeWS(w http.ResponseWriter, r *http.Request) {
	if h.Tracker == nil || h.Secret == "" {
		http.Error(w, "realtime not configured", http.StatusServiceUnavailable)
		return
	}
	raw := r.URL.Query().Get("token")
	if raw == "" {
		ah := r.Header.Get("Authorization")
		if len(ah) > 7 && (ah[:7] == "Bearer " || ah[:7] == "bearer ") {
			raw = ah[7:]
		}
	}
	claims, err := auth.ParseToken(h.Secret, raw)
	if err != nil {
		http.Error(w, "invalid or expired token", http.StatusUnauthorized)
		return
	}
	if h.Redis != nil {
		if n, _ := h.Redis.Exists(r.Context(), auth.DeniedKey(raw)).Result(); n > 0 {
			http.Error(w, "logged out", http.StatusUnauthorized)
			return
		}
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
	if err != nil {
		return
	}
	ctx := r.Context()
	c := &Client{UserID: claims.UserID, Username: claims.Username, conn: conn, send: make(chan []byte, 64)}
	h.Hub.Add(c)
	defer h.Hub.Remove(c)

	if err := h.Tracker.MarkOnline(ctx, c.UserID, c.Username); err != nil {
		log.Printf("ws: mark online failed for %d: %v", c.UserID, err)
		_ = conn.Close(websocket.StatusInternalError, "presence unavailable")
		return
	}
	h.Hub.BroadcastPresenceOnline(c.UserID, c.Username)

	online, _ := h.Tracker.ListOnline(ctx)
	sendTo(c, EvWelcome, map[string]any{
		"you":    map[string]any{"user_id": c.UserID, "username": c.Username},
		"online": online,
	})

	go h.writeLoop(ctx, conn, c)
	h.readLoop(ctx, conn, c)
	// NOTE: no immediate offline here. Presence lapses via TTL + reaper,
	// so a dropped socket with a live HTTP-heartbeating client stays online.
}

func (h *Handler) writeLoop(ctx context.Context, conn *websocket.Conn, c *Client) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.send:
			if !ok {
				return
			}
			if err := conn.Write(ctx, websocket.MessageText, msg); err != nil {
				return
			}
		}
	}
}

type clientMsg struct {
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload,omitempty"`
}

func (h *Handler) readLoop(ctx context.Context, conn *websocket.Conn, c *Client) {
	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			return // disconnect; presence left to TTL/reaper
		}
		var m clientMsg
		if err := json.Unmarshal(raw, &m); err != nil {
			sendTo(c, EvError, map[string]string{"error": "invalid json"})
			continue
		}
		switch m.Type {
		case "heartbeat":
			ok, err := h.Tracker.Heartbeat(ctx, c.UserID)
			if err != nil || !ok {
				// Key lapsed between connect and now; re-mark (self-heal).
				_ = h.Tracker.MarkOnline(ctx, c.UserID, c.Username)
				h.Hub.BroadcastPresenceOnline(c.UserID, c.Username)
			}
			sendTo(c, EvHeartbeatAck, map[string]bool{"online": true})
		case "ping":
			sendTo(c, EvPong, nil)
		default:
			sendTo(c, EvError, map[string]string{"error": "unknown message type: " + m.Type})
		}
	}
}
