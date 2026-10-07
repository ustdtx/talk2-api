package media

// Key scheme — same drop-in-replace idea as Rewardly-apiV2:
// one deterministic key per entity slot, re-upload overwrites.
// Posts/comments/DMs are ephemeral (UUID ids), so keys are
// unique per message; offline wipe deletes the keys (see plan.md §11).

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// MaxImageBytes mirrors Rewardly (5 MB).
const MaxImageBytes = 5 * 1024 * 1024

var allowedTypes = map[string]string{
	"image/jpeg": ".jpg",
	"image/png":  ".png",
	"image/webp": ".webp",
}

// ContentTypeOK reports whether the upload content type is allowed.
func ContentTypeOK(ct string) bool {
	_, ok := allowedTypes[strings.ToLower(strings.TrimSpace(ct))]
	return ok
}

// ExtFor returns the canonical extension for an allowed content type.
func ExtFor(ct string) string { return allowedTypes[strings.ToLower(strings.TrimSpace(ct))] }

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Deterministic per-entity slots (idx 0..3, max 4 images per parent).
func PostImageKey(postID string, idx int) string { return fmt.Sprintf("posts/%s/%d.jpg", postID, idx) }
func CommentImageKey(commentID string, idx int) string {
	return fmt.Sprintf("comments/%s/%d.jpg", commentID, idx)
}
func DMImageKey(a, b, msgID string, idx int) string {
	if a > b {
		a, b = b, a
	}
	return fmt.Sprintf("dm/%s-%s/%s-%d.jpg", a, b, msgID, idx)
}

// TmpKey for standalone uploads via POST /media/upload without a parent yet.
func TmpKey(authorID, ext string) string {
	if ext == "" {
		ext = ".jpg"
	}
	return fmt.Sprintf("tmp/%s/%s%s", authorID, newID(), ext)
}
