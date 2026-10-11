package feed

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"talk2-api/internal/auth"
	"talk2-api/internal/realtime"
)

type Handler struct {
	Store *Store
	Hub   *realtime.Hub
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func mapStoreErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrRateLimited):
		writeErr(w, http.StatusTooManyRequests, "rate limited: 1 post per 5 min")
	case errors.Is(err, ErrOffline):
		writeErr(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, ErrNotFound):
		writeErr(w, http.StatusNotFound, "not found")
	case errors.Is(err, ErrGone):
		writeErr(w, http.StatusGone, err.Error())
	case errors.Is(err, ErrForbidden):
		writeErr(w, http.StatusForbidden, err.Error())
	default:
		writeErr(w, http.StatusBadRequest, err.Error())
	}
}

type postBody struct {
	Text      string   `json:"text"`
	ImageKeys []string `json:"image_keys"`
	ParentID  int64    `json:"parent_id"`
}

// POST /posts
func (h *Handler) CreatePost(w http.ResponseWriter, r *http.Request) {
	claims, _ := auth.ClaimsFrom(r)
	var b postBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return
	}
	p, err := h.Store.CreatePost(r.Context(), claims.UserID, claims.Username, b.Text, b.ImageKeys)
	if err != nil {
		mapStoreErr(w, err)
		return
	}
	// Live push with client-side pacing: every post is broadcast to all
	// sockets, but clients drain it through a paced reveal queue (time
	// budget + adaptive rate + overflow pill), so a 100-posts/sec storm
	// never forces 100 renders/sec on any device. Delivery is best-effort
	// (slow clients drop, see Hub.deliver); REST stays source of truth and
	// the after-cursor stays the reconnect/catch-up path. Retracts are
	// pushed too — they are rare.
	h.Hub.Broadcast(realtime.EvFeedNewPost, p)
	writeJSON(w, http.StatusCreated, p)
}

// GET /feed?limit=&before= (older history, newest-first) or
// GET /feed?limit=&after= (everything newer than id, oldest-first — the
// reconnect/catch-up path for clients that missed socket pushes).
func (h *Handler) ListFeed(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	var before int64
	if v := r.URL.Query().Get("before"); v != "" {
		before, _ = strconv.ParseInt(v, 10, 64)
	}
	var after int64
	if v := r.URL.Query().Get("after"); v != "" {
		after, _ = strconv.ParseInt(v, 10, 64)
	}
	var posts []Post
	var err error
	if after > 0 {
		posts, err = h.Store.ListFeedAfter(r.Context(), limit, after)
	} else {
		posts, err = h.Store.ListFeed(r.Context(), limit, before)
	}
	if err != nil {
		writeErr(w, http.StatusBadGateway, "feed unavailable")
		return
	}
	if posts == nil {
		posts = []Post{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"posts": posts})
}

// GET /posts/{id}
func (h *Handler) GetPost(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad post id")
		return
	}
	p, err := h.Store.GetPost(r.Context(), id)
	if err != nil {
		mapStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// DELETE /posts/{id} (owner only, cascades the thread)
func (h *Handler) DeletePost(w http.ResponseWriter, r *http.Request) {
	claims, _ := auth.ClaimsFrom(r)
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad post id")
		return
	}
	refs, err := h.Store.DeletePost(r.Context(), id, claims.UserID)
	if err != nil {
		mapStoreErr(w, err)
		return
	}
	h.Hub.Broadcast(realtime.EvPostRetracted, map[string]any{"post_id": id})
	// Cascade retracts so open threads drop without tombstones (plan §6).
	for _, c := range refs {
		h.Hub.Broadcast(realtime.EvCommentRetracted,
			map[string]any{"comment_id": c.ID, "post_id": c.PostID})
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": id})
}

// POST /posts/{id}/comments
func (h *Handler) AddComment(w http.ResponseWriter, r *http.Request) {
	claims, _ := auth.ClaimsFrom(r)
	postID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad post id")
		return
	}
	var b postBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return
	}
	c, err := h.Store.AddComment(r.Context(), postID, claims.UserID, claims.Username, b.Text, b.ImageKeys, b.ParentID)
	if err != nil {
		mapStoreErr(w, err)
		return
	}
	// Live threads: broadcast globally, clients filter by open post id.
	// Comment payloads are tiny; retracts ride the same socket (rare).
	h.Hub.Broadcast(realtime.EvCommentNew, c)
	writeJSON(w, http.StatusCreated, c)
}

// GET /posts/{id}/comments
func (h *Handler) ListComments(w http.ResponseWriter, r *http.Request) {
	postID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad post id")
		return
	}
	comments, err := h.Store.ListComments(r.Context(), postID)
	if err != nil {
		mapStoreErr(w, err)
		return
	}
	if comments == nil {
		comments = []ThreadedComment{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"comments": comments})
}
