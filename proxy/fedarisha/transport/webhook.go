package transport

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
)

// WebhookHub routes S3 event notifications to transport connections and listeners.
// It implements http.Handler and should be mounted on the path that S3 sends
// webhook POSTs to (e.g. /webhook).
//
// Supported S3 events: s3:ObjectCreated:Put (VK Cloud S3 webhook API).
type WebhookHub struct {
	prefix     string // S3 key prefix to strip (e.g. "projects/")
	webhookURL string // public URL of this endpoint (for signature validation)

	mu       sync.RWMutex
	sessions map[string]chan struct{} // sessionID -> wake-up channel

	// New session notifications for Listener.
	newSessions chan string // sends sessDir (e.g. "sessions/abc123")
}

// NewWebhookHub creates a webhook hub.
// prefix is the S3 key prefix (same as S3Store.Prefix).
// webhookURL is the public URL of the webhook endpoint (used for subscription confirmation).
func NewWebhookHub(prefix, webhookURL string) *WebhookHub {
	return &WebhookHub{
		prefix:      prefix,
		webhookURL:  webhookURL,
		sessions:    make(map[string]chan struct{}),
		newSessions: make(chan string, 64),
	}
}

// Register creates a notification channel for a session.
// The returned channel receives a signal whenever a new file is created
// in that session's directory.
func (h *WebhookHub) Register(sessionID string) <-chan struct{} {
	ch := make(chan struct{}, 16)
	h.mu.Lock()
	h.sessions[sessionID] = ch
	h.mu.Unlock()
	return ch
}

// Unregister removes and closes a session's notification channel.
func (h *WebhookHub) Unregister(sessionID string) {
	h.mu.Lock()
	if ch, ok := h.sessions[sessionID]; ok {
		close(ch)
		delete(h.sessions, sessionID)
	}
	h.mu.Unlock()
}

// NewSessions returns a channel that receives session directory paths
// when new hello files are detected via webhook.
func (h *WebhookHub) NewSessions() <-chan string {
	return h.newSessions
}

// ServeHTTP handles incoming S3 webhook requests.
// It processes both SubscriptionConfirmation (validation) and event notifications.
func (h *WebhookHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Only accept requests to /webhook — reject scanners and bots.
	if r.URL.Path != "/webhook" {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	var raw json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	// Try subscription confirmation first.
	var conf subscriptionConfirmation
	if json.Unmarshal(raw, &conf) == nil && conf.Type == "SubscriptionConfirmation" {
		sig := computeSignature(conf.Token, conf.Timestamp, conf.TopicArn, h.webhookURL)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"signature": sig})
		log.Printf("[webhook] subscription confirmed for topic %s", conf.TopicArn)
		return
	}

	// Parse as event notification.
	var notif s3Notification
	if err := json.Unmarshal(raw, &notif); err != nil {
		http.Error(w, "bad notification", http.StatusBadRequest)
		return
	}

	for _, rec := range notif.Records {
		h.handleRecord(rec)
	}

	w.WriteHeader(http.StatusOK)
}

func (h *WebhookHub) handleRecord(rec s3Record) {
	key := rec.S3.Object.Key
	if h.prefix != "" && !strings.HasPrefix(key, h.prefix) {
		return
	}

	// Strip S3 prefix (e.g. "projects/").
	rel := strings.TrimPrefix(key, h.prefix)

	// Expected path formats:
	//   sessions/{sessionID}/{filename}
	//   {user}/sessions/{sessionID}/{filename}   (multi-user)
	parts := strings.Split(rel, "/")

	var sessionsIdx int = -1
	for i, p := range parts {
		if p == "sessions" && i+2 < len(parts) {
			sessionsIdx = i
			break
		}
	}
	if sessionsIdx < 0 {
		return
	}

	sessionID := parts[sessionsIdx+1]
	fileName := parts[sessionsIdx+2]

	// Hello file → new session notification.
	if fileName == HelloFile {
		sessDir := strings.Join(parts[:sessionsIdx+2], "/")
		select {
		case h.newSessions <- sessDir:
		default:
			log.Printf("[webhook] new session channel full, dropping %s", shortID(sessionID))
		}
		return
	}

	// Data file → wake up the session's poll loop.
	h.mu.RLock()
	ch, ok := h.sessions[sessionID]
	h.mu.RUnlock()

	if ok {
		select {
		case ch <- struct{}{}:
		default: // already notified, no need to queue more
		}
	}
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// ---------- S3 webhook JSON structures ----------

type s3Notification struct {
	Records []s3Record `json:"Records"`
}

type s3Record struct {
	EventName string   `json:"eventName"`
	S3        s3Detail `json:"s3"`
}

type s3Detail struct {
	Bucket s3Bucket `json:"bucket"`
	Object s3Object `json:"object"`
}

type s3Bucket struct {
	Name string `json:"name"`
}

type s3Object struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

// SubscriptionConfirmation is sent by VK Cloud S3 when configuring a webhook.
// The endpoint must respond with a computed HMAC-SHA256 signature.
type subscriptionConfirmation struct {
	Timestamp        string `json:"Timestamp"`
	Type             string `json:"Type"`
	TopicArn         string `json:"TopicArn"`
	SignatureVersion int    `json:"SignatureVersion"`
	Token            string `json:"Token"`
}

// computeSignature calculates the HMAC-SHA256 chain for VK Cloud S3 webhook validation.
// Formula: signature = hmac_sha256_hex(url, hmac_sha256(TopicArn, hmac_sha256(Timestamp, Token)))
// where hmac_sha256(msg, key) means HMAC-SHA256 with key as HMAC key and msg as message.
func computeSignature(token, timestamp, topicArn, url string) string {
	h1 := hmac.New(sha256.New, []byte(token))
	h1.Write([]byte(timestamp))
	step1 := h1.Sum(nil)

	h2 := hmac.New(sha256.New, step1)
	h2.Write([]byte(topicArn))
	step2 := h2.Sum(nil)

	h3 := hmac.New(sha256.New, step2)
	h3.Write([]byte(url))
	return hex.EncodeToString(h3.Sum(nil))
}
