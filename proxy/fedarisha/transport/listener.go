package transport

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

// Listener watches for new client sessions and returns net.Conn for each.
//
// InboundTag is the user-visible identifier of the inbound this listener
// represents. It is stamped onto every accepted Conn so the routing and
// stats layers can attribute the stream to its source. Empty in standalone
// (non-runtime-config) mode.
type Listener struct {
	Store         storage.Storage
	SessionsDir   string // e.g. "sessions"
	InboundTag    string // matches RuntimeConfig.Inbounds[].Tag
	MultiUser     bool   // scan */sessions/ for per-user prefixes
	PollInterval  time.Duration
	WriteInterval time.Duration
	IdleTimeout   time.Duration
	MaxFileSize   int
	WebhookHub    *WebhookHub // optional — enables event-driven session detection

	// IsUserAllowed is the layer-2 entitlement gate. Even when a stale or
	// out-of-band PAK lets a client write to the bucket, we refuse to handshake
	// for prefixes that aren't registered as active users on this inbound.
	// nil means "no gate" — accept every prefix (single-user / standalone mode).
	IsUserAllowed func(userPrefix string) bool

	ctx    context.Context
	cancel context.CancelFunc

	incoming chan *Conn
	known    map[string]bool // key: "sessionsDir/sessID"
	knownMu  sync.Mutex

	closeOnce sync.Once
	addr      net.Addr
}

// ListenOpts holds optional parameters for Listen/ListenMultiUser.
type ListenOpts struct {
	WebhookHub *WebhookHub
	InboundTag string // stamped onto every accepted Conn
}

// Listen starts watching the sessions directory for new client connections.
func Listen(ctx context.Context, store storage.Storage, sessionsDir string, opts ...ListenOpts) (*Listener, error) {
	if err := store.EnsureDir(ctx, sessionsDir); err != nil {
		return nil, fmt.Errorf("fedarisha listen: ensure sessions dir: %w", err)
	}

	lCtx, cancel := context.WithCancel(ctx)
	l := &Listener{
		Store:       store,
		SessionsDir: sessionsDir,
		ctx:         lCtx,
		cancel:      cancel,
		incoming:    make(chan *Conn, 16),
		known:       make(map[string]bool),
		addr:        fedarishaAddr{tag: "fedarisha-listener:" + sessionsDir},
	}
	if len(opts) > 0 {
		l.WebhookHub = opts[0].WebhookHub
		l.InboundTag = opts[0].InboundTag
	}

	go l.watchLoop()
	return l, nil
}

// ListenMultiUser starts watching for sessions across all user prefixes.
// It scans */sessionsDir/ for new sessions, where each user has their own
// subdirectory under the storage root.
func ListenMultiUser(ctx context.Context, store storage.Storage, sessionsDir string, opts ...ListenOpts) (*Listener, error) {
	lCtx, cancel := context.WithCancel(ctx)
	l := &Listener{
		Store:       store,
		SessionsDir: sessionsDir,
		MultiUser:   true,
		ctx:         lCtx,
		cancel:      cancel,
		incoming:    make(chan *Conn, 16),
		known:       make(map[string]bool),
		addr:        fedarishaAddr{tag: "fedarisha-listener:*/" + sessionsDir},
	}
	if len(opts) > 0 {
		l.WebhookHub = opts[0].WebhookHub
		l.InboundTag = opts[0].InboundTag
	}

	go l.watchLoop()
	return l, nil
}

// Accept blocks until a new client session is detected or the listener is closed.
func (l *Listener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.incoming:
		return conn, nil
	case <-l.ctx.Done():
		return nil, l.ctx.Err()
	}
}

func (l *Listener) Close() error {
	l.closeOnce.Do(func() {
		l.cancel()
	})
	return nil
}

func (l *Listener) Addr() net.Addr { return l.addr }

// ---------- internal ----------

func (l *Listener) watchLoop() {
	poll := l.PollInterval
	if poll == 0 {
		poll = 500 * time.Millisecond // Listener can poll slower — sessions are rare events
	}

	// With webhooks, use a much longer fallback poll interval.
	if l.WebhookHub != nil {
		poll = 10 * time.Second
	}

	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	var webhookCh <-chan string
	if l.WebhookHub != nil {
		webhookCh = l.WebhookHub.NewSessions()
	}

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			l.scanForNewSessions()
		case sessDir := <-webhookCh:
			l.acceptSessionFromWebhook(sessDir)
		}
	}
}

func (l *Listener) scanForNewSessions() {
	if !l.MultiUser {
		l.scanSessionsIn(l.SessionsDir)
		return
	}

	// Multi-user: list user directories, then scan {user}/sessions/ for each.
	users, err := l.Store.List(l.ctx, "", "")
	if err != nil {
		log.Printf("[fedarisha-server] list users error: %v", err)
		return
	}
	for _, u := range users {
		if !u.IsDir {
			continue
		}
		l.scanSessionsIn(u.Name + "/" + l.SessionsDir)
	}
}

func (l *Listener) scanSessionsIn(sessionsDir string) {
	dirs, err := l.Store.List(l.ctx, sessionsDir, "")
	if err != nil {
		return // Directory may not exist yet.
	}

	for _, d := range dirs {
		if !d.IsDir {
			continue
		}
		sessDir := sessionsDir + "/" + d.Name
		l.acceptSession(sessDir)
	}
}

// acceptSessionFromWebhook handles a webhook notification about a new hello file.
// sessDir is the relative path like "sessions/abc123" or "user1/sessions/abc123".
func (l *Listener) acceptSessionFromWebhook(sessDir string) {
	log.Printf("[fedarisha-server] webhook: new session in %s", sessDir)
	l.acceptSession(sessDir)
}

// acceptSession performs the key exchange handshake for a single session directory
// and enqueues the resulting connection.
func (l *Listener) acceptSession(sessDir string) {
	parts := strings.Split(sessDir, "/")
	sessID := parts[len(parts)-1]

	l.knownMu.Lock()
	if l.known[sessDir] {
		l.knownMu.Unlock()
		return
	}
	l.knownMu.Unlock()

	// In multi-user mode the prefix is the first path segment.
	// sessDir format: "user1/sessions/abc123" → userPrefix = "user1"
	var userPrefix string
	if l.MultiUser {
		if idx := strings.Index(sessDir, "/"); idx > 0 {
			userPrefix = sessDir[:idx]
		}
	}

	// Layer-2 gate: refuse handshakes for prefixes that aren't currently
	// entitled. PAK revocation (layer 1) prevents most rogue writes, but a
	// race window between AddUser/RemoveUser and PAK provisioning leaves a
	// gap that the gate closes deterministically. We mark the session known
	// and delete the hello so we don't keep poking it on every poll tick.
	if l.IsUserAllowed != nil && userPrefix != "" && !l.IsUserAllowed(userPrefix) {
		l.knownMu.Lock()
		l.known[sessDir] = true
		l.knownMu.Unlock()
		_ = l.Store.Delete(l.ctx, sessDir+"/"+HelloFile)
		log.Printf("[fedarisha-server] session %s rejected: user %q not allowed", sessID[:8], userPrefix)
		return
	}

	// Check for hello file (contains sessID + client public key).
	helloPath := sessDir + "/" + HelloFile
	data, err := l.Store.Download(l.ctx, helloPath)
	if err != nil || len(data) == 0 {
		return // Not ready yet.
	}

	// Extract client public key from hello (after sessID).
	if len(data) < len(sessID)+32 {
		log.Printf("[fedarisha-server] session %s: hello too short for key exchange", sessID[:8])
		return
	}
	clientPub := data[len(sessID):][:32]

	log.Printf("[fedarisha-server] new session %s in %s", sessID[:8], sessDir)

	l.knownMu.Lock()
	l.known[sessDir] = true
	l.knownMu.Unlock()

	_ = l.Store.Delete(l.ctx, helloPath)

	// Generate server X25519 key pair and derive shared secret.
	privKey, pubKey, err := GenerateX25519()
	if err != nil {
		log.Printf("[fedarisha-server] session %s: keygen failed: %v", sessID[:8], err)
		return
	}

	aead, err := DeriveAEAD(privKey, clientPub, sessID)
	if err != nil {
		log.Printf("[fedarisha-server] session %s: key derivation failed: %v", sessID[:8], err)
		return
	}

	// Write ACK with server's public key.
	ackPath := sessDir + "/" + AckFile
	if err := l.Store.Upload(l.ctx, ackPath, pubKey); err != nil {
		log.Printf("[fedarisha-server] failed to ACK session %s: %v", sessID[:8], err)
		return
	}

	conn := NewConn(ConnConfig{
		Store:         l.Store,
		SessionID:     sessID,
		SessionDir:    sessDir,
		UserPrefix:    userPrefix,
		InboundTag:    l.InboundTag,
		IsClient:      false,
		PollInterval:  l.PollInterval,
		WriteInterval: l.WriteInterval,
		IdleTimeout:   l.IdleTimeout,
		MaxFileSize:   l.MaxFileSize,
		Cipher:        aead,
		WebhookHub:    l.WebhookHub,
	})

	select {
	case l.incoming <- conn:
	default:
		log.Printf("[fedarisha-server] incoming channel full, dropping session %s", sessID[:8])
		conn.Close()
	}
}
