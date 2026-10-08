package room

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

func (h *Handler) mapErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrOffline):
		writeErr(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, ErrNotFound):
		writeErr(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrNotMember):
		writeErr(w, http.StatusForbidden, err.Error())
	default:
		writeErr(w, http.StatusBadRequest, err.Error())
	}
}

type createBody struct {
	Name string `json:"name"`
	// Topic is accepted only for compatibility with older clients. Rooms
	// have one user-facing value: the name.
	Topic string `json:"topic"`
}
type messageBody struct {
	Text string `json:"text"`
}

func roomID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("bad room id")
	}
	return id, nil
}

func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
	claims, _ := auth.ClaimsFrom(r)
	var b createBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return
	}
	name := b.Name
	if name == "" {
		name = b.Topic
	}
	rm, err := h.Store.Create(r.Context(), claims.UserID, claims.Username, name)
	if err != nil {
		h.mapErr(w, err)
		return
	}
	h.Hub.Broadcast(realtime.EvRoomCreated, rm)
	writeJSON(w, http.StatusCreated, rm)
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	rooms, err := h.Store.List(r.Context())
	if err != nil {
		writeErr(w, http.StatusBadGateway, "rooms unavailable")
		return
	}
	if rooms == nil {
		rooms = []Room{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"rooms": rooms})
}

func (h *Handler) Join(w http.ResponseWriter, r *http.Request) {
	claims, _ := auth.ClaimsFrom(r)
	id, err := roomID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	rm, err := h.Store.Join(r.Context(), claims.UserID, id)
	if err != nil {
		h.mapErr(w, err)
		return
	}
	h.Hub.Broadcast(realtime.EvRoomUpdated, rm)
	writeJSON(w, http.StatusOK, rm)
}

func (h *Handler) Messages(w http.ResponseWriter, r *http.Request) {
	claims, _ := auth.ClaimsFrom(r)
	id, err := roomID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	msgs, err := h.Store.GetMessages(r.Context(), claims.UserID, id)
	if err != nil {
		h.mapErr(w, err)
		return
	}
	if msgs == nil {
		msgs = []Message{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": msgs})
}

func (h *Handler) Send(w http.ResponseWriter, r *http.Request) {
	claims, _ := auth.ClaimsFrom(r)
	id, err := roomID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	var b messageBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json body")
		return
	}
	msg, err := h.Store.Send(r.Context(), claims.UserID, claims.Username, id, b.Text)
	if err != nil {
		h.mapErr(w, err)
		return
	}
	// Targeted: only current members receive the message (not a broadcast).
	h.Hub.SendToUsers(h.Store.Members(r.Context(), id), realtime.EvRoomMessage, msg)
	writeJSON(w, http.StatusCreated, msg)
}

func (h *Handler) Leave(w http.ResponseWriter, r *http.Request) {
	claims, _ := auth.ClaimsFrom(r)
	id, err := roomID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := h.Store.Leave(r.Context(), claims.UserID, id)
	if err != nil {
		h.mapErr(w, err)
		return
	}
	// Message retracts go only to remaining members; room list changes
	// (deleted/updated member count) stay global — they are rare.
	members := h.Store.Members(r.Context(), id)
	for _, message := range res.DeletedMessages {
		h.Hub.SendToUsers(members, realtime.EvRoomMessageRetracted, message)
	}
	if len(res.DeletedRooms) > 0 {
		h.Hub.Broadcast(realtime.EvRoomDeleted, map[string]any{"room_id": id})
	} else {
		if rm, err := h.Store.List(r.Context()); err == nil {
			for _, current := range rm {
				if current.ID == id {
					h.Hub.Broadcast(realtime.EvRoomUpdated, current)
					break
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, res)
}
