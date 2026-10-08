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
	// Pull-based feed (scale): no global feed:new_post push. The author gets
	// the POST response (optimistic) and everyone else polls GET /feed.
	// Retracts (delete/offline) are still pushed — they are rare.
	writeJSON(w, http.StatusCreated, p)
}

// GET /feed?limit=&before= (older history, newest-first),
// GET /feed?limit=&after= (legacy id cursor, oldest-first), or
// GET /feed?batch=n|latest (sealed time-window batches, oldest-first).
func (h *Handler) ListFeed(w http.ResponseWriter, r *http.Request) {
	if v := r.URL.Query().Get("batch"); v != "" {
		var n int64 = -1
		if v == "latest" {
			if batches, err := h.Store.ListBatches(r.Context()); err == nil && len(batches) > 0 {
				n = batches[len(batches)-1]
			}
			if n < 0 {
				writeJSON(w, http.StatusOK, map[string]any{
					"posts": []Post{}, "batch": -1, "complete": false,
				})
				return
			}
		} else {
			var err error
			n, err = strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				writeErr(w, http.StatusBadRequest, "bad batch number")
				return
			}
		}
		res, err := h.Store.ListBatch(r.Context(), n)
		if err != nil {
			writeErr(w, http.StatusBadGateway, "feed unavailable")
			return
		}
		if res.Posts == nil {
			res.Posts = []Post{}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"posts": res.Posts, "batch": res.Batch, "complete": res.Complete,
		})
		return
	}
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

// GET /feed/batches — sealed, non-empty batch ids ascending. The client
// starts at n-2 (clamped to oldest) and walks n+1 down / n-1 up.
func (h *Handler) ListBatches(w http.ResponseWriter, r *http.Request) {
	batches, err := h.Store.ListBatches(r.Context())
	if err != nil {
		writeErr(w, http.StatusBadGateway, "feed unavailable")
		return
	}
	if batches == nil {
		batches = []int64{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"batches": batches})
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
	// Pull-based threads (scale): no global comment:new push. Open threads
	// poll GET /posts/{id}/comments; retracts are still pushed (rare).
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
