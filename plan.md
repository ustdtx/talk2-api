# Talk2 - Experimental Transient Social Media - Plan

## 1. Vision

Talk2 is an experimental social media built around transient connection.

Inspiration: speech. Once you say something, it exists only for the duration of your speech. When you leave a room, no one can see you.

Feeling target: moving through the stream of consciousness of a cluster of humans - live, unranked, unarchived. No profiles to stalk, no history to scroll. You are only here while you are here.

This is the opposite of current social media: no persistence, no algorithm, no friend graph, no clout accumulation.

## 2. Core Principles

1. Presence = existence. You exist in the system only while online in the app. Offline = unreachable + authored content gone.
2. No algorithm, no graph. One global chronological feed. Posts stack on top of each other. No follows, no friends, no likes-driven ranking.
3. Conversation over content. Posts are conversation starters. Value is in comments, DMs, and live rooms - not in posts as artifacts.
4. Ephemerality is enforced server-side. Deletion on offline is not a client hide. Data is deleted.
5. Low identity. Username + password only. No profiles, bios, avatars, follower counts in v1.

Non-goals for v1:
- No algorithmic feed, no recommendations
- No message history, search, or replay after offline
- No push notifications for offline users (you can't reach the unreachable)
- No moderation tooling beyond delete-on-offline + manual report stub

## 3. User Flows

### 3.1 Auth
1. Register with username + password. Username is unique, public handle.
2. Login with username + password -> session + socket connection.
3. You are now online. You appear as reachable for DMs/rooms.
4. Logout / close app / timeout -> offline. See section 6 for teardown.

### 3.2 Global Feed (no algo)
- Feed is a single global reverse-chronological list of posts from currently-online users only.
- New post appears on top for everyone online.
- No size limit: full scrollable history of currently-live posts (only posts from online users).
- No filtering by friends. No boosting.
- Rate limit: 1 post per user per 5 minutes (anti-spam without algo).

### 3.3 Post + Comments (discussion)
- Post: short text (e.g. max 500 chars).
- Anyone online can open a post and comment in a flat/stack thread.
- Comments are part of the live discussion, also presence-bound to their author.

### 3.4 DMs (poster <-> viewer)
- From any post or comment, viewer can Talk to poster -> 1:1 DM thread.
- DM is only possible if both parties are online.
- DM thread exists only while BOTH parties are online. If either goes offline, the whole thread disappears for the remaining user (complete wipe, not just one side).
- No offline delivery. If recipient is offline, you cannot send - sender sees unreachable.


## 4. Functional Requirements

### F1 - Auth (username/pass)
- POST /auth/register { username, password }
- POST /auth/login { username, password } -> token
- POST /auth/logout
- Passwords hashed (argon2/bcrypt). Sessions are token + socket auth.
- Single identity: user_id, username. Users are persisted. No auto-delete of accounts on offline.

### F2 - Presence
- Client maintains socket + heartbeat (e.g. every 20s).
- Server marks online if socket connected + heartbeat fresh (< e.g. 60s).
- GET /presence/online (debug) / socket event presence:update.
- Transition online -> offline triggers teardown job (section 6). No grace archive.

### F3 - Posts (feed)
- POST /posts { text } - only when online. Rate-limited: 1 per 5 min per user, else 429.
- GET /feed?limit&before=cursor - returns only posts whose authors are currently online, newest first. No fixed size limit.
- DELETE /posts/:id - author can delete manually while online (cascades to all comments on that post).
- Post model: id, author_id, author_username, text, created_at.
- No edit. No likes. No repost in v1.

### F4 - Comments
- POST /posts/:id/comments { text }
- GET /posts/:id/comments
- Comment model: id, post_id, author_id, author_username, text, created_at.
- Comments visible only while both post and comment author are online.
- DECIDED: wipe scope = delete ALL comments by author everywhere on offline. Plus cascade: deleting a post deletes its entire comment thread.

### F5 - DMs
- POST /dms { to_user_id, text } - allowed only if sender online AND recipient online, else 422 unreachable.
- GET /dms/:with_user_id - live thread, visible only while both parties online.
- GET /dms - conversation list: only conversations where other party is online.
- DECIDED: DMs completely go away if one person is offline. On either party offline, server deletes ALL dm_messages where (from_id=U and to_id=V) OR (from_id=V and to_id=U) for every V counterpart? Minimal: delete all messages from offline user AND hide/hide/delete thread for online user. V1 semantics: thread vanishes entirely for the remaining online user (all messages in that 1:1 thread deleted). Broadcast dm:thread_retracted.
- DM events over socket: dm:send, dm:receive, dm:retracted / dm:thread_retracted.
- No offline queue. No read receipts beyond live ack in v1.


## 5. Data Model (minimal persistent + mostly ephemeral)

Persistent tables:
- users(id, username UNIQUE, password_hash, created_at) - survives offline forever. No auto-delete.

Ephemeral tables (fast wipe by author_id):
- posts(id, author_id, text, room_id NULL, created_at)
- comments(id, post_id, author_id, text, created_at)
- dm_messages(id, from_id, to_id, text, created_at)
- room_messages(id, room_id, author_id, text, created_at)
- rooms(id, topic, created_by, created_at) + ephemeral room_members(room_id, user_id)
- presence(user_id, last_heartbeat, socket_id)

Indexes for wipe:
- posts(author_id), comments(author_id, post_id), dm_messages(from_id, to_id), room_messages(author_id, room_id)

All ephemeral rows must be deletable by single DELETE WHERE author_id = ? per table. No foreign-key blocking of wipe, except post-delete cascades comments by post_id.

## 6. Offline Semantics - All Gone (explicit) - DECIDED

Definition of offline: explicit logout OR socket disconnect + heartbeat miss beyond threshold (e.g. 60s) OR session expiry.

On transition to offline for user U, server MUST atomically:

1. Mark U offline, broadcast presence:offline { user_id }.
2. Delete ALL posts WHERE author_id = U. Broadcast post:retracted for each. Attached invites die with the post (join via dead invite -> 410 Gone). CASCADE: also delete ALL comments WHERE post_id in deleted posts (entire thread gone) - DECIDED Q2 yes.
3. Delete ALL comments WHERE author_id = U everywhere (all posts) - DECIDED Q1 confirmed. Broadcast comment:retracted.
4. DMs - DECIDED: full thread wipe if one side offline. For every counterpart V where a thread U<->V exists: delete ALL dm_messages where (from_id=U and to_id=V) or (from_id=V and to_id=U). Broadcast dm:thread_retracted { with_user_id: U } to V if V online. Result: remaining online user sees the conversation vanish completely.
5. Delete ALL room_messages WHERE author_id = U only (other users messages stay) - DECIDED. Broadcast room:message_retracted per message.
6. Remove U from all room_members, broadcast room:left. DECIDED: if room has zero online members after removal, delete room immediately + delete any leftover messages + broadcast room:deleted. No TTL.
7. Close U sockets. Further POST /dms to U or POST as U fail with 401/422 until re-login.

Consequences:
- Alice posts, Bob comments, Alice goes offline: post + entire comment thread disappears (including Bob comments on that post).
- Bob goes offline, Alice stays: Alice post stays, all Bob comments everywhere vanish, full DM thread Alice<->Bob vanishes for Alice, Bob room messages vanish (Alice room messages stay).
- Re-login restores NOTHING. Clean slate. Users themselves persist (username/password kept), only content is gone - DECIDED Q6.

Client MUST handle retract events by removing nodes without tombstones (no message deleted placeholders in v1).

## 7. Realtime Contract (WebSocket)

Single socket per session. Auth on connect.

Client -> Server: heartbeat, post:create, comment:create, dm:send, room:join, room:leave, room:send
Server -> Client: feed:new_post, post:retracted, comment:new, comment:retracted, dm:receive, dm:retracted, dm:thread_retracted, room:message, room:message_retracted, room:joined, room:left, room:members, room:deleted, presence:offline/online, error:unreachable, error:rate_limited

Feed socket-pushed + REST bootstrapped. Source of truth is server filtered by online authors.

## 8. API Sketch (MVP REST + socket)

- Auth: register/login/logout/me
- Feed: GET /feed, POST /posts (1/5min), DELETE /posts/:id
- Discussion: POST /posts/:id/comments, GET /posts/:id/comments
- DM: POST /dms, GET /dms, GET /dms/:with_user_id
- Rooms: POST /rooms, GET /rooms, GET /rooms/:id, POST /rooms/:id/join|leave, GET /rooms/:id/members|messages, POST /rooms/:id/messages

All mutating routes require online auth. All reads filter to online authors only (except users).

## 9. MVP Build Phases

Phase 0 - Foundation:
- Repo setup, plan.md, DB + ephemeral store choice, auth + presence + heartbeat + offline wipe worker + socket skeleton.

Phase 1 - Feed + Posts + Comments:
- Global chronological feed (no size limit), 1-post-per-5-min limit, create/delete post + full thread cascade, comment + wipe-everywhere, live retract.

Phase 2 - DMs (online-only + full thread wipe):
- 1:1 send/list/thread, unreachable guard, full-thread delete on either offline + dm:thread_retracted.

Phase 3 - Rooms via post invite:
- Create/join/leave public room, live messages + members, post-attached invite, delete-only-own-messages on offline, immediate delete-empty-room.

Phase 4 - Hardening:
- Heartbeat tuning, disconnect vs logout parity, load test of wipe fan-out, minimal web client to feel the stream.

## 10. Decisions Log (all locked)

1. Comment wipe scope: CONFIRMED - wipe author's comments everywhere on offline.
2. Post-delete cascade: YES - deleting a post (via author offline or manual delete) deletes its entire comment thread.
3. DMs: DECIDED - full thread goes away if either person offline. Thread vanishes entirely for remaining user.
4. Rooms: DECIDED - only delete offline user's messages; room deleted immediately when zero users online in it.
5. Feed/limits: DECIDED - no feed size limit, rate limit 1 post per 5 min per user. Post max ~500 chars (to lock in impl).
6. Users: DECIDED - persisted, no auto-delete. Gone means content gone, identity stays.


## 11. Images (posts, comments, DMs, rooms) - DECIDED

Users can attach images to: posts, comments, DM messages, room messages.

Constraints (v1):
- Max 4 images per post/comment/message, max 5MB each, jpg/png/webp only.
- Rate limit inherits parent (e.g. post 1/5min covers its images).

Storage: Cloudflare R2 (S3-compatible), bucket `honeybadgerb1`.
- Upload flow (backend-proxied PutObject, drop-in replace, no presigning — same as Rewardly `S3StorageService.UploadAsync`):
  1. `POST /media/upload` multipart `{ key?, file }` -> `{ object_key, public_url }` (bytes go through API pods; pods are stateless so this scales by adding pods).
  2. Keys are deterministic per entity slot: `posts/{postID}/{idx}.jpg`, `comments/{commentID}/{idx}.jpg`, `dm/{a}-{b}/{msgID}-{idx}.jpg`, `rooms/{roomID}/{msgID}-{idx}.jpg` (Rewardly: `categories/{slug}.jpg`, `vendors/{id}.jpg`, `products/{vendorId}/{productId}.jpg`). Re-upload overwrites the same key.
  3. Client creates post/comment/DM/room-message with `image_keys: [...]` (keys must belong to the same author (enforced at create time)).
- Serve: `public_url = {S3_PUBLIC_URL}/{object_key}`. No auth needed to view (if you can see the parent, you can see its images).

Tracking (required for auto-delete):
- `media:{object_key} -> { author_id, parent_type, parent_id }` in Redis + `media_by_author:{author_id}` SET of keys.
- Parent rows store `image_keys TEXT[]` too (so feed/thread render without extra lookup).

Auto-deletion (ephemerality applies to bytes, not just rows) - DECIDED:
- Manual delete of post/comment/message -> delete its R2 objects + Redis index entries.
- Post cascade delete -> delete post images + all comment images in thread.
- Offline wipe for U -> delete ALL R2 objects in `media_by_author:{U}` (posts + comments + sent DMs + room msgs), all in background wipe workers (parallel S3 deletes, idempotent).
- DM thread wipe (either side offline) -> delete ALL images in that 1:1 thread (both sides), since thread vanishes entirely.
- Room empty-delete -> delete any leftover images in that room (should only be none, since per-user wipe already ran, but GC is safety net).
- Orphan guard: orphan `tmp/` keys (uploaded but never attached) expire via lifecycle rule (prefix `tmp/` unreferenced >24h -> delete) + nightly sweeper comparing R2 vs Redis index.

API additions:
- Rooms: `POST /rooms` creates a live room and adds the creator, `GET /rooms`
  lists rooms with members, `POST /rooms/:id/join` opens a room for the
  current user, `GET/POST /rooms/:id/messages` reads and sends group chat,
  and `POST /rooms/:id/leave` removes the user's membership and messages.
  Users may remain members of multiple rooms; offline teardown removes their
  messages and memberships from every room, and an empty room is deleted.
- Room socket events: `room:created`, `room:updated`, `room:deleted`,
  `room:message`, and `room:message_retracted`.
- `POST /media/upload` (multipart key?+file, 5 MB, jpeg/png/webp), `DELETE /media\?key=` (own only, while online)
- `image_keys` field on POST /posts, POST /posts/:id/comments, POST /dms, POST /rooms/:id/messages
- Socket payloads include `images: [{ key, url }]` and retract events need no extra handling (parent retract removes images from UI).

Limits for viral scale: upload is a stateless PutObject through any API pod (no affinity needed). R2 handles infinite bytes. Wipe path is async and batched (e.g. 100 keys per S3 batch).
7. Images: DECIDED - posts/comments/DMs/room-msgs support up to 4 images (R2 direct upload (Rewardly method, no presigning)); all bytes auto-deleted on parent delete / offline wipe / DM thread wipe / room GC.
8. Sessions: DECIDED - logout denies the JWT (Redis denylist to expiry), runs full offline teardown immediately, and kills live sockets; DELETE /auth/account {password} does all that plus deletes the user row. Re-login starts clean.
