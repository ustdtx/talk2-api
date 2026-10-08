package dm

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
		writeErr(w, http.StatusTooManyRequests, "rate limited")
	case errors.Is(err, ErrOffline):
		writeErr(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, ErrUnreachable):
		writeErr(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, ErrSelf):
		writeErr(w, http.StatusBadRequest, err.Error())
	default:
		writeErr(w, http.StatusBadRequest, err.Error())
	}
}

type sendBody struct {
	ToUserID  int64    `json:"to_user_id"`
	Text      string   `json:"text"`
	ImageKeys []string `json:"image_keys"`
}

// POST /dms — both parties must be online, else 422.
func (h *Handler) Send(w http.ResponseWriter, r *http.Request) {
	claims, _ := auth.ClaimsFrom(r)
	var b sendBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if b.ToUserID <= 0 {
		writeErr(w, http.StatusBadRequest, "to_user_id required")
		return
	}
	m, err := h.Store.Send(r.Context(), claims.UserID, claims.Username, b.ToUserID, b.Text, b.ImageKeys)
	if err != nil {
		mapStoreErr(w, err)
		return
	}
	// Canonical per plan §7 is dm:receive; dm:new kept as compat alias.
	// Targeted: only the 2 participants ever receive it (not a broadcast).
	h.Hub.SendToUsers([]int64{claims.UserID, b.ToUserID}, realtime.EvDMReceive, m)
	h.Hub.SendToUsers([]int64{claims.UserID, b.ToUserID}, realtime.EvDMNew, m)
	writeJSON(w, http.StatusCreated, m)
}

// GET /dms — conversations with online counterparts only.
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	claims, _ := auth.ClaimsFrom(r)
	threads, err := h.Store.ListThreads(r.Context(), claims.UserID)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "dms unavailable")
		return
	}
	if threads == nil {
		threads = []Thread{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"threads": threads})
}

// GET /dms/{with} — thread visible only while both are online.
func (h *Handler) Thread(w http.ResponseWriter, r *http.Request) {
	claims, _ := auth.ClaimsFrom(r)
	with, err := strconv.ParseInt(r.PathValue("with"), 10, 64)
	if err != nil || with <= 0 {
		writeErr(w, http.StatusBadRequest, "bad user id")
		return
	}
	msgs, err := h.Store.GetThread(r.Context(), claims.UserID, with)
	if err != nil {
		if errors.Is(err, ErrUnreachable) {
			writeErr(w, http.StatusGone, err.Error())
			return
		}
		mapStoreErr(w, err)
		return
	}
	if msgs == nil {
		msgs = []Message{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": msgs})
}
