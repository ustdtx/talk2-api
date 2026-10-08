package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"talk2-api/internal/auth"
	"talk2-api/internal/config"
	"talk2-api/internal/db"
	"talk2-api/internal/dm"
	"talk2-api/internal/feed"
	"talk2-api/internal/health"
	"talk2-api/internal/media"
	"talk2-api/internal/presence"
	"talk2-api/internal/realtime"
	"talk2-api/internal/room"
)

func main() {
	config.LoadDotEnv(".env")
	cfg := config.Load()

	// Storage wiring mirrors Rewardly-apiV2 Program.cs:
	// singleton S3 client when configured, else MissingStorage (503 on use).
	var store media.Storage = media.MissingStorage{}
	s3settings := media.Settings{
		Bucket:          cfg.S3Bucket,
		Region:          cfg.S3Region,
		EndpointURL:     cfg.S3EndpointURL,
		PublicURL:       cfg.S3PublicURL,
		AccessKeyID:     cfg.S3AccessKeyID,
		SecretAccessKey: cfg.S3SecretKey,
	}
	if s3settings.Configured() {
		store = media.NewS3Storage(s3settings)
		log.Printf("media storage: r2 bucket %s", cfg.S3Bucket)
	} else {
		log.Printf("media storage: NOT configured (set S3_BUCKET + related env)")
	}

	mux := http.NewServeMux()

	// Auth (TASK-1): users table in Postgres. Nil pool when unconfigured
	// -> handlers answer 503, server still runs degraded.
	authH := &auth.Handler{Secret: cfg.JWTSecret}
	if pool, err := db.Connect(context.Background(), cfg.DatabaseURL); err != nil {
		log.Printf("auth: database unavailable (%v) - /auth/* will 503", err)
	} else {
		authH.DB = pool
		defer pool.Close()
		log.Printf("auth: postgres connected, users table ready")
	}
	if cfg.JWTSecret == "" {
		log.Printf("auth: JWT_SECRET not set - /auth/* will 503")
	}
	mux.HandleFunc("/auth/register", postOnly(authH.Register))
	mux.HandleFunc("/auth/login", postOnly(authH.Login))
	mux.HandleFunc("/auth/logout", postOnly(authH.Authenticate(authH.Logout)))
	mux.HandleFunc("/auth/account", authH.Authenticate(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		authH.DeleteAccount(w, r)
	}))
	mux.HandleFunc("/me", authH.Authenticate(authH.Me))

	// Presence + realtime (TASK-2). Degraded without Redis/JWT.
	hub := realtime.NewHub()
	rdb := presence.Client(cfg.RedisURL)
	var tracker *presence.Tracker
	var feedStore *feed.Store
	var dmStore *dm.Store
	var roomStore *room.Store
	if rdb != nil && cfg.JWTSecret != "" {
		tracker = presence.New(rdb,
			time.Duration(cfg.PresenceTTL)*time.Second,
			time.Duration(cfg.PresenceSweep)*time.Second,
			func(ctx context.Context, uid int64, username string) {
				onOffline(ctx, hub, feedStore, dmStore, roomStore, uid, username)
			})
		feedStore = feed.NewStore(rdb, tracker, store, cfg.RateLimits, cfg.FeedMaxPosts, cfg.FeedBatchMinSec, cfg.FeedBatchMaxSec)
		dmStore = dm.NewStore(rdb, tracker, store, cfg.RateLimits)
		roomStore = room.NewStore(rdb, tracker)
		go tracker.RunReaper(context.Background())
		log.Printf("presence: redis connected (ttl=%ds sweep=%ds)", cfg.PresenceTTL, cfg.PresenceSweep)
	} else {
		log.Printf("presence: NOT configured (need REDIS_URL + JWT_SECRET) - /ws and /presence/* will 503")
	}
	rtH := &realtime.Handler{Hub: hub, Tracker: tracker, Secret: cfg.JWTSecret, Redis: rdb}
	// Session teardown: logout/delete wipes content now (not on TTL lapse)
	// and kills live sockets.
	authH.Redis = rdb
	authH.OnLogout = func(ctx context.Context, uid int64, username string) {
		onOffline(ctx, hub, feedStore, dmStore, roomStore, uid, username)
		hub.DisconnectUser(uid, "logged out")
	}
	mux.HandleFunc("GET /ws", rtH.ServeWS)
	mux.HandleFunc("GET /presence/online", authH.Authenticate(func(w http.ResponseWriter, r *http.Request) {
		if tracker == nil {
			http.Error(w, "presence not configured", http.StatusServiceUnavailable)
			return
		}
		online, err := tracker.ListOnline(r.Context())
		if err != nil {
			http.Error(w, "presence unavailable", http.StatusBadGateway)
			return
		}
		if online == nil {
			online = []presence.Member{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"online": online})
	}))
	mux.HandleFunc("GET /presence/{id}", authH.Authenticate(func(w http.ResponseWriter, r *http.Request) {
		if tracker == nil {
			http.Error(w, "presence not configured", http.StatusServiceUnavailable)
			return
		}
		uid, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "bad user id", http.StatusBadRequest)
			return
		}
		ok, err := tracker.IsOnline(r.Context(), uid)
		if err != nil {
			http.Error(w, "presence unavailable", http.StatusBadGateway)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"user_id": uid, "online": ok})
	}))
	mux.HandleFunc("POST /presence/heartbeat", authH.Authenticate(func(w http.ResponseWriter, r *http.Request) {
		if tracker == nil {
			http.Error(w, "presence not configured", http.StatusServiceUnavailable)
			return
		}
		claims, _ := auth.ClaimsFrom(r)
		ok, _ := tracker.Heartbeat(r.Context(), claims.UserID)
		if !ok {
			// HTTP-only client (no socket): establish presence.
			_ = tracker.MarkOnline(r.Context(), claims.UserID, claims.Username)
		}
		writeJSON(w, http.StatusOK, map[string]bool{"online": true})
	}))

	// Feed + discussion (TASK-3). Requires presence (Redis); auth on all routes.
	feedH := &feed.Handler{Store: feedStore, Hub: hub}
	requireFeed := func(next http.HandlerFunc) http.HandlerFunc {
		return authH.Authenticate(func(w http.ResponseWriter, r *http.Request) {
			if feedStore == nil {
				http.Error(w, "feed not configured", http.StatusServiceUnavailable)
				return
			}
			next(w, r)
		})
	}
	mux.HandleFunc("POST /posts", requireFeed(feedH.CreatePost))
	mux.HandleFunc("GET /feed", requireFeed(feedH.ListFeed))
	mux.HandleFunc("GET /feed/batches", requireFeed(feedH.ListBatches))
	mux.HandleFunc("GET /posts/{id}", requireFeed(feedH.GetPost))
	mux.HandleFunc("DELETE /posts/{id}", requireFeed(feedH.DeletePost))
	mux.HandleFunc("POST /posts/{id}/comments", requireFeed(feedH.AddComment))
	mux.HandleFunc("GET /posts/{id}/comments", requireFeed(feedH.ListComments))

	// DMs (TASK-4). Requires presence (Redis); auth on all routes.
	dmH := &dm.Handler{Store: dmStore, Hub: hub}
	requireDM := func(next http.HandlerFunc) http.HandlerFunc {
		return authH.Authenticate(func(w http.ResponseWriter, r *http.Request) {
			if dmStore == nil {
				http.Error(w, "dms not configured", http.StatusServiceUnavailable)
				return
			}
			next(w, r)
		})
	}
	mux.HandleFunc("POST /dms", requireDM(dmH.Send))
	mux.HandleFunc("GET /dms", requireDM(dmH.List))
	mux.HandleFunc("GET /dms/{with}", requireDM(dmH.Thread))

	// Rooms: online-only group chats. Opening a room joins it; leaving or
	// going offline removes only the user's messages and membership.
	roomH := &room.Handler{Store: roomStore, Hub: hub}
	requireRoom := func(next http.HandlerFunc) http.HandlerFunc {
		return authH.Authenticate(func(w http.ResponseWriter, r *http.Request) {
			if roomStore == nil {
				http.Error(w, "rooms not configured", http.StatusServiceUnavailable)
				return
			}
			next(w, r)
		})
	}
	mux.HandleFunc("POST /rooms", requireRoom(roomH.Create))
	mux.HandleFunc("GET /rooms", requireRoom(roomH.List))
	mux.HandleFunc("POST /rooms/{id}/join", requireRoom(roomH.Join))
	mux.HandleFunc("GET /rooms/{id}/messages", requireRoom(roomH.Messages))
	mux.HandleFunc("POST /rooms/{id}/messages", requireRoom(roomH.Send))
	mux.HandleFunc("POST /rooms/{id}/leave", requireRoom(roomH.Leave))

	// Liveness: always 200 if the process is up. Use this to verify bootstrap.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "ok",
			"service": "talk2-api",
		})
	})

	// Readiness: checks Postgres + Redis + R2 reachability.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pg := health.PostgresCheck(cfg.DatabaseURL)
		rd := health.RedisCheck(cfg.RedisURL)
		r2 := health.R2Check(cfg.S3PublicURL, cfg.S3EndpointURL, cfg.S3Bucket)

		status := "ready"
		code := http.StatusOK
		if !pg.Reachable || !rd.Reachable || !r2.Reachable {
			status = "degraded"
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, health.Report{
			Status: status,
			Checks: map[string]health.Check{
				"postgres": pg,
				"redis":    rd,
				"r2":       r2,
			},
		})
	})

	// POST /media/upload — multipart { key?, file }. Authenticated (plan §11):
	// keys must live under the caller's own tmp/{uid}/ prefix; empty key
	// mints tmp/{uid}/{rand}{ext}. Entity-slot renames are future work;
	// create-time ownership is enforced in feed/dm checkImages.
	mux.HandleFunc("/media/upload", authH.Authenticate(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		claims, _ := auth.ClaimsFrom(r)
		// Cap total body slightly above the 5 MB file limit (multipart overhead).
		r.Body = http.MaxBytesReader(w, r.Body, media.MaxImageBytes+1<<20)
		if err := r.ParseMultipartForm(media.MaxImageBytes + 1<<20); err != nil {
			http.Error(w, "body too large or not multipart (max 5 MB file)", http.StatusRequestEntityTooLarge)
			return
		}
		f, hdr, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "missing multipart field 'file'", http.StatusBadRequest)
			return
		}
		defer f.Close()
		if hdr.Size > media.MaxImageBytes {
			http.Error(w, "image must be 5 MB or smaller", http.StatusRequestEntityTooLarge)
			return
		}
		ct := hdr.Header.Get("Content-Type")
		if ct == "" || ct == "application/octet-stream" {
			// Sniff when the client didn't set a type.
			head := make([]byte, 512)
			n, _ := io.ReadFull(f, head)
			ct = http.DetectContentType(head[:n])
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				// Non-seekable in-memory file; re-read path not needed for *bytes.Buffer,
				// but guard anyway.
				http.Error(w, "could not read file", http.StatusBadRequest)
				return
			}
		}
		if !media.ContentTypeOK(ct) {
			http.Error(w, "only jpeg, png, webp images allowed", http.StatusUnsupportedMediaType)
			return
		}
		key := strings.TrimSpace(r.FormValue("key"))
		ownPrefix := "tmp/" + strconv.FormatInt(claims.UserID, 10) + "/"
		if key == "" {
			key = media.TmpKey(strconv.FormatInt(claims.UserID, 10), media.ExtFor(ct))
		} else if !strings.HasPrefix(key, ownPrefix) {
			http.Error(w, "image keys must be your own uploads (tmp/{your-id}/...)", http.StatusForbidden)
			return
		}
		url, err := store.Upload(r.Context(), key, f, ct)
		if err != nil {
			if errors.Is(err, media.ErrNotConfigured) {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			log.Printf("upload %s failed: %v", key, err)
			http.Error(w, "upload failed", http.StatusBadGateway)
			return
		}
		// Plan §11 tracking: pending until attached via image_keys.
		media.TrackPending(r.Context(), rdb, claims.UserID, key)
		writeJSON(w, http.StatusCreated, map[string]string{"key": key, "url": url})
	}))

	// DELETE /media?key=... — own tmp/ keys only, while online.
	// Wipe paths delete via Storage directly (no HTTP auth needed).
	mux.HandleFunc("/media", authH.Authenticate(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		claims, _ := auth.ClaimsFrom(r)
		key := strings.TrimSpace(r.URL.Query().Get("key"))
		if key == "" {
			http.Error(w, "missing ?key=", http.StatusBadRequest)
			return
		}
		ownPrefix := "tmp/" + strconv.FormatInt(claims.UserID, 10) + "/"
		if !strings.HasPrefix(key, ownPrefix) {
			http.Error(w, "only your own uploads can be deleted", http.StatusForbidden)
			return
		}
		if err := store.Delete(r.Context(), key); err != nil {
			if errors.Is(err, media.ErrNotConfigured) {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			log.Printf("delete %s failed: %v", key, err)
			http.Error(w, "delete failed", http.StatusBadGateway)
			return
		}
		media.Untrack(r.Context(), rdb, claims.UserID, []string{key})
		writeJSON(w, http.StatusOK, map[string]string{"deleted": key})
	}))

	// POST /media/sweep — delete pending tmp/ uploads older than 24h
	// (plan §11 orphan guard: uploaded but never attached). R2 lifecycle on
	// prefix tmp/ should mirror this in Cloudflare as backup. Runs nightly
	// in-process too (see ticker below); this endpoint allows manual run.
	mux.HandleFunc("/media/sweep", authH.Authenticate(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if rdb == nil {
			http.Error(w, "media index not configured", http.StatusServiceUnavailable)
			return
		}
		deleted := media.SweepPendingOrphans(r.Context(), rdb, store)
		writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted, "count": len(deleted)})
	}))

	// Feed timing loop: seal ticks + adaptive windows.
	// Windows follow crowd size — short when quiet (fresh), long when packed
	// (efficient). At most one window change per few minutes (no flapping);
	// renumbering after a change is silent and clients rebase losslessly.
	if feedStore != nil && tracker != nil {
		go func() {
			announced := feedStore.CurrentBatch() - 1
			var lastWindowChange time.Time
			tune := func() {
				n, err := tracker.OnlineCount(context.Background())
				if err != nil {
					return
				}
				want := feed.DesiredWindow(n, cfg.FeedBatchMinSec, cfg.FeedBatchMaxSec)
				if want == feedStore.WindowSec() {
					return
				}
				if time.Since(lastWindowChange) < 3*time.Minute {
					return
				}
				feedStore.SetWindowSec(want)
				lastWindowChange = time.Now()
				announced = feedStore.CurrentBatch() - 1
				hub.Broadcast(realtime.EvFeedWindowChanged, map[string]any{"window_sec": want})
				log.Printf("feed: window %ds for %d online", want, n)
			}
			tune()
			sealTick := time.NewTicker(10 * time.Second)
			defer sealTick.Stop()
			tuneTick := time.NewTicker(30 * time.Second)
			defer tuneTick.Stop()
			for {
				select {
				case <-sealTick.C:
					batch, ok := feedStore.SealCheck(context.Background(), announced)
					if batch <= announced {
						continue
					}
					announced = batch
					if ok {
						hub.Broadcast(realtime.EvFeedBatchSealed, map[string]any{"batch": batch})
						log.Printf("feed: sealed batch %d", batch)
					}
				case <-tuneTick.C:
					tune()
				}
			}
		}()
	}
	// Nightly orphan sweeper (plan §11). Hourly tick, 24h age gate inside.
	if rdb != nil {
		go func() {
			t := time.NewTicker(time.Hour)
			defer t.Stop()
			for range t.C {
				if n := media.SweepPendingOrphans(context.Background(), rdb, store); len(n) > 0 {
					log.Printf("media sweep: deleted %d orphan uploads", len(n))
				}
			}
		}()
	}

	// Root hint.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"service": "talk2-api",
			"health":  "/healthz",
			"ready":   "/readyz",
			"upload":  "POST /media/upload (multipart: key?, file)",
		})
	})

	addr := ":" + cfg.Port
	log.Printf("talk2-api listening on %s", addr)
	log.Printf("health: http://localhost:%s/healthz", cfg.Port)
	log.Printf("ready:  http://localhost:%s/readyz", cfg.Port)
	log.Fatal(http.ListenAndServe(addr, cors(mux, cfg.CORSOrigins)))
}

// cors allows browser UIs (the Next.js client) to call the API cross-origin.
// Origins are allow-listed via CORS_ORIGINS; the request origin is echoed
// back (wildcard is illegal with Authorization headers).
func cors(next http.Handler, allowList string) http.Handler {
	allowed := map[string]bool{}
	for _, o := range strings.Split(allowList, ",") {
		if o = strings.TrimSpace(o); o != "" {
			allowed[o] = true
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if allowed[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			w.Header().Set("Access-Control-Max-Age", "86400")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// onOffline is the presence teardown (plan.md section 6): wipe the user's
// content FIRST so clients retract it, then announce the user is gone.
func onOffline(ctx context.Context, hub *realtime.Hub, fs *feed.Store, ds *dm.Store, rs *room.Store, uid int64, username string) {
	if fs != nil {
		if res, err := fs.WipeUser(ctx, uid); err == nil {
			for _, pid := range res.Posts {
				hub.Broadcast(realtime.EvPostRetracted, map[string]any{"post_id": pid})
			}
			for _, c := range res.Comments {
				hub.Broadcast(realtime.EvCommentRetracted,
					map[string]any{"comment_id": c.ID, "post_id": c.PostID})
			}
			if len(res.Posts)+len(res.Comments) > 0 {
				log.Printf("wipe: user %d removed %d posts %d comments",
					uid, len(res.Posts), len(res.Comments))
			}
		} else {
			log.Printf("wipe: user %d feed failed: %v", uid, err)
		}
	}
	if ds != nil {
		// Full thread wipe (plan decision 3): both directions die, the
		// survivor gets dm:thread_retracted per counterpart — targeted to the
		// survivor only, never a broadcast.
		if parts, err := ds.WipeUser(ctx, uid); err == nil {
			for _, vid := range parts {
				hub.SendToUser(vid, realtime.EvDMThreadRetr,
					map[string]any{"with_user_id": uid, "for_user_id": vid})
			}
			if len(parts) > 0 {
				log.Printf("wipe: user %d removed %d dm threads", uid, len(parts))
			}
		} else {
			log.Printf("wipe: user %d dm failed: %v", uid, err)
		}
	}
	if rs != nil {
		if wiped, err := rs.WipeUser(ctx, uid); err == nil {
			// Message retracts go only to remaining members of each room.
			// Deleted/updated room list events stay global (rare).
			byRoom := map[int64][]int64{}
			for _, roomID := range wiped.RoomIDs {
				if containsInt64(wiped.DeletedRooms, roomID) {
					continue
				}
				byRoom[roomID] = rs.Members(ctx, roomID)
			}
			for _, message := range wiped.DeletedMessages {
				hub.SendToUsers(byRoom[message.RoomID], realtime.EvRoomMessageRetracted, message)
			}
			for _, roomID := range wiped.DeletedRooms {
				hub.Broadcast(realtime.EvRoomDeleted, map[string]any{"room_id": roomID})
			}
			for _, roomID := range wiped.RoomIDs {
				if containsInt64(wiped.DeletedRooms, roomID) {
					continue
				}
				if current, err := rs.Get(ctx, roomID); err == nil {
					hub.Broadcast(realtime.EvRoomUpdated, current)
				}
			}
			if len(wiped.RoomIDs) > 0 {
				log.Printf("wipe: user %d removed from %d rooms", uid, len(wiped.RoomIDs))
			}
		} else {
			log.Printf("wipe: user %d rooms failed: %v", uid, err)
		}
	}
	hub.BroadcastPresenceOffline(uid, username)
	log.Printf("presence: user %d (%s) went offline", uid, username)
}

func containsInt64(values []int64, target int64) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// postOnly rejects non-POST with 405.
func postOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		next(w, r)
	}
}
