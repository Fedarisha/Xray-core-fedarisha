package transport

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/cipher"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

// Conn implements net.Conn over a cloud-storage session.
type Conn struct {
	store     storage.Storage
	sessionID string
	sessDir   string      // e.g. "sessions/abc123"
	cipher    cipher.AEAD // AES-256-GCM for E2E encryption (nil = plaintext)

	writePrefix string
	readPrefix  string

	writeSeq uint64
	readSeq  uint64

	// Read buffer: pollLoop deposits data here, Read() consumes it.
	readBuf  bytes.Buffer
	readMu   sync.Mutex
	readCond *sync.Cond

	// Write: Write() deposits data here, flushLoop sends it via upload pipeline.
	writeBuf bytes.Buffer
	writeMu  sync.Mutex
	flushNow chan struct{}

	// Upload pipeline: multiple uploads in flight concurrently.
	uploadQueue chan uploadJob
	uploadWg    sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	pollInterval  time.Duration
	writeInterval time.Duration
	idleTimeout   time.Duration
	maxFileSize   int

	lastRecv   time.Time
	lastRecvMu sync.Mutex

	// Active transfer tracking — suppress backoff when data is flowing.
	lastRecvActive atomic.Int64 // unix nano of last successful fetch

	lastFlush time.Time // time of last actual flush

	// holeSince marks when we first noticed a "hole": later files present but
	// the file at readSeq still missing. Only touched by the pollLoop goroutine
	// (via fetchNext), so it needs no lock. A persistent hole means the peer is
	// flow-control wedged and the missing file will never land — we tear the
	// session down for a fast re-dial instead of waiting out keepalive.
	holeSince time.Time

	// Prefetch cache — stores files fetched past a hole (uploads land out of
	// order) but not yet consumed. Avoids re-GETting them on the next poll.
	prefetchMu    sync.Mutex
	prefetchCache map[uint64][]byte // seq -> raw data (before decode)

	closeOnce sync.Once
	closed    chan struct{}

	// Webhook notification channel — if set, pollLoop waits on this
	// instead of blind polling. Nil means no webhook (use adaptive polling).
	notify     <-chan struct{}
	webhookHub *WebhookHub // for unregistering on close

	// Metrics counters for baseline measurement.
	s3Puts      atomic.Int64
	s3Gets      atomic.Int64
	s3PutErrors atomic.Int64
	s3GetErrors atomic.Int64

	localAddr  net.Addr
	remoteAddr net.Addr

	// UserPrefix identifies the user in multi-user mode (e.g. "user1").
	userPrefix string

	// inboundTag is the runtime-config tag of the listener that produced
	// this conn; routing and stats key off it. Empty in standalone mode.
	inboundTag string
}

type uploadJob struct {
	path string
	data []byte
}

// ConnConfig holds per-connection tunables.
type ConnConfig struct {
	Store         storage.Storage
	SessionID     string
	SessionDir    string
	UserPrefix    string // e.g. "user1" — set in multi-user mode
	InboundTag    string // runtime-config tag of the originating listener
	IsClient      bool
	PollInterval  time.Duration
	WriteInterval time.Duration
	IdleTimeout   time.Duration
	MaxFileSize   int
	Cipher        cipher.AEAD // AES-256-GCM cipher for E2E encryption (nil = plaintext)
	WebhookHub    *WebhookHub // optional — enables event-driven polling via S3 webhooks
}

const uploadWorkers = 32

// Hedged PUTs. A single slow PUT (gpucloud throttling a connection under load)
// stalls one file; because the reader consumes strictly in order, a stuck file
// at the read frontier blocks the whole stream and trips the hole watchdog into
// a disruptive re-dial. So we hedge: if a PUT hasn't completed in time, fire a
// duplicate on a fresh connection and take whichever finishes first (PutObject
// is idempotent for a fixed key+body). uploadTimeout is the hard cap across
// attempts; uploadAttempts bounds the hedges.
//
// The hedge clock is SIZE-AWARE, and this is the key to upload throughput. A
// small file (a control frame or yamux window-update, tens of bytes) should
// land in ~60ms; if it hasn't within uploadHedgeSmall it's genuinely stuck on a
// throttled connection, and because a stuck window-update file stalls the peer's
// in-order read it freezes the RETURN path — which is exactly what zeroes the
// upload direction — so we overcome it fast (a 29-byte duplicate costs nothing).
// A large data file, by contrast, legitimately takes ~1.5s for 2MB: hedging it
// on the same short clock blindly re-uploads 2MB and burns real uplink bandwidth
// (on a marginal uplink every PUT then trips the hedge, doubling traffic and
// spiralling goodput toward zero). So a large file only gets a duplicate once
// it's clearly stuck, past uploadHedgeLarge.
const (
	uploadHedgeSmall   = 600 * time.Millisecond
	uploadHedgeLarge   = 3 * time.Second
	uploadHedgeSizeCut = 256 * 1024 // bytes; files at/above use the large delay
	uploadTimeout      = 8 * time.Second
	uploadAttempts     = 3
)

// Read batching bounds. Each poll Lists the session dir (cheap, strongly
// consistent on Ceph) and fetches up to maxReadBatch present files starting at
// readSeq, with at most maxReadConcurrency GETs in flight. maxReadBatch caps
// per-round work and prefetch-cache memory; maxReadConcurrency bounds the share
// of the read S3 pool used so it can't starve unrelated work.
const (
	maxReadBatch = 64
	// Read GETs in flight cap. With List-confirmed fetches there are no wasted
	// 404s, so this can be generous to keep many multiplexed streams fed under a
	// parallel download flood without one stream starving the others. Bounded
	// below the read pool's 96 connections, leaving headroom for List calls.
	maxReadConcurrency = 48
)

// holeTimeout bounds how long a missing readSeq file (with later files already
// present) is tolerated before the session is treated as wedged and re-dialed.
// Comfortably above a normal out-of-order gap (a slow 2MB PUT vs fast control
// files, well under 1s) so it only fires on a genuine flow-control deadlock,
// giving ~holeTimeout recovery instead of the keepalive's tens of seconds.
// Set above the hedged-PUT budget (uploadHedgeDelay × a couple attempts) so a
// merely-slow file gets overcome by a hedge before the watchdog re-dials.
const holeTimeout = 7 * time.Second

// Hedged GETs. A healthy GET returns in a few hundred ms; a tail (a transient
// error pushed the AWS SDK into a multi-second backoff-retry, or a connection
// hung) would otherwise freeze the whole in-order batch. Instead of waiting one
// slow request out, if it hasn't answered within readHedgeDelay we fire a
// duplicate on a fresh request and take whichever returns first — so a tail
// costs ~readHedgeDelay + a normal GET, not seconds. readGetTimeout is the hard
// cap across all attempts; readGetAttempts bounds how many duplicates we fire.
// Because List already confirmed the file exists, a GET should not 404 — so a
// retry here only covers genuine transient transport errors.
const (
	readHedgeDelay  = 400 * time.Millisecond
	readGetTimeout  = 1200 * time.Millisecond
	readGetAttempts = 2
)

func NewConn(cfg ConnConfig) *Conn {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	if cfg.WriteInterval == 0 {
		cfg.WriteInterval = DefaultWriteInterval
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = DefaultIdleTimeout
	}
	if cfg.MaxFileSize == 0 {
		cfg.MaxFileSize = DefaultMaxFileSize
	}

	wp, rp := PrefixServer, PrefixClient
	if cfg.IsClient {
		wp, rp = PrefixClient, PrefixServer
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Conn{
		store:         cfg.Store,
		sessionID:     cfg.SessionID,
		sessDir:       cfg.SessionDir,
		cipher:        cfg.Cipher,
		writePrefix:   wp,
		readPrefix:    rp,
		ctx:           ctx,
		cancel:        cancel,
		pollInterval:  cfg.PollInterval,
		writeInterval: cfg.WriteInterval,
		idleTimeout:   cfg.IdleTimeout,
		maxFileSize:   cfg.MaxFileSize,
		lastRecv:      time.Now(),
		closed:        make(chan struct{}),
		flushNow:      make(chan struct{}, 1),
		uploadQueue:   make(chan uploadJob, 64),
		localAddr:     fedarishaAddr{tag: "fedarisha-local"},
		remoteAddr:    fedarishaAddr{tag: "fedarisha:" + cfg.SessionID[:8]},
		prefetchCache: make(map[uint64][]byte),
		userPrefix:    cfg.UserPrefix,
		inboundTag:    cfg.InboundTag,
	}
	c.readCond = sync.NewCond(&c.readMu)

	// Register with webhook hub if available.
	if cfg.WebhookHub != nil {
		c.webhookHub = cfg.WebhookHub
		c.notify = cfg.WebhookHub.Register(cfg.SessionID)
	}

	// Start upload workers.
	for i := 0; i < uploadWorkers; i++ {
		c.uploadWg.Add(1)
		go c.uploadWorker()
	}

	go c.pollLoop()
	go c.flushLoop()
	go c.idleWatcher()

	return c
}

// ---------- net.Conn ----------

func (c *Conn) Read(b []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for c.readBuf.Len() == 0 {
		select {
		case <-c.closed:
			return 0, net.ErrClosed
		case <-c.ctx.Done():
			return 0, c.ctx.Err()
		default:
		}
		waitDone := make(chan struct{})
		go func() {
			select {
			case <-c.closed:
			case <-c.ctx.Done():
			case <-waitDone:
			}
			c.readCond.Broadcast()
		}()
		c.readCond.Wait()
		close(waitDone)
	}

	return c.readBuf.Read(b)
}

func (c *Conn) Write(b []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}

	c.writeMu.Lock()
	c.writeBuf.Write(b)
	size := c.writeBuf.Len()
	c.writeMu.Unlock()

	if size >= c.maxFileSize {
		// Buffer full — force flush immediately (bypasses accumulation delay).
		c.forceFlush()
	} else if size == len(b) && len(b) < 256 {
		// First small write (yamux control frame) — flush after brief coalescing.
		go func() {
			time.Sleep(5 * time.Millisecond)
			select {
			case c.flushNow <- struct{}{}:
			default:
			}
		}()
	}
	// For medium/large writes: rely on the ticker + flush() accumulation logic.
	// This lets bulk data accumulate to large chunks before uploading to S3.

	return len(b), nil
}

func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		log.Printf("[fedarisha] session %s Close() called (S3 puts: %d, gets: %d, put_errs: %d, get_errs: %d, write_seq: %d, read_seq: %d)",
			c.sessionID[:8], c.s3Puts.Load(), c.s3Gets.Load(), c.s3PutErrors.Load(), c.s3GetErrors.Load(), c.writeSeq, c.readSeq)
		if c.webhookHub != nil {
			c.webhookHub.Unregister(c.sessionID)
		}
		c.flush()
		close(c.closed)
		c.cancel()
		close(c.uploadQueue)
		c.uploadWg.Wait()
		c.readCond.Broadcast()
		go func() {
			// Bigger sessions accumulate hundreds-to-thousands of c_*/s_*
			// files. With per-file Delete on the slow path, 5s wasn't enough
			// for the cleanup to finish, leaving stragglers in the bucket.
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			c.cleanupSession(ctx)
		}()
	})
	return nil
}

func (c *Conn) LocalAddr() net.Addr  { return c.localAddr }
func (c *Conn) RemoteAddr() net.Addr { return c.remoteAddr }

// UserPrefix returns the user identifier (e.g. "user1") for multi-user sessions.
func (c *Conn) UserPrefix() string { return c.userPrefix }

// InboundTag returns the runtime-config tag of the listener that produced
// this connection. Empty in standalone (non-runtime-config) mode.
func (c *Conn) InboundTag() string { return c.inboundTag }

func (c *Conn) SetDeadline(t time.Time) error      { return nil }
func (c *Conn) SetReadDeadline(t time.Time) error  { return nil }
func (c *Conn) SetWriteDeadline(t time.Time) error { return nil }

// ---------- wire format ----------

const (
	headerRaw        = 0x00
	headerCompressed = 0x01
)

func encodePayload(data []byte) []byte {
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, flate.BestSpeed)
	w.Write(data)
	w.Close()
	compressed := buf.Bytes()

	if len(compressed) < len(data) {
		out := make([]byte, 1+len(compressed))
		out[0] = headerCompressed
		copy(out[1:], compressed)
		return out
	}

	out := make([]byte, 1+len(data))
	out[0] = headerRaw
	copy(out[1:], data)
	return out
}

func decodePayload(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	switch data[0] {
	case headerCompressed:
		r := flate.NewReader(bytes.NewReader(data[1:]))
		defer r.Close()
		return io.ReadAll(r)
	default:
		return data[1:], nil
	}
}

// encrypt applies AES-256-GCM encryption if a cipher is configured.
func (c *Conn) encrypt(data []byte, seq uint64) []byte {
	if c.cipher == nil {
		return data
	}
	nonce := MakeNonce(c.writePrefix, seq)
	return c.cipher.Seal(nil, nonce, data, []byte(c.sessionID))
}

// decrypt applies AES-256-GCM decryption if a cipher is configured.
func (c *Conn) decrypt(data []byte, seq uint64) ([]byte, error) {
	if c.cipher == nil {
		return data, nil
	}
	nonce := MakeNonce(c.readPrefix, seq)
	plain, err := c.cipher.Open(nil, nonce, data, []byte(c.sessionID))
	if err != nil {
		return nil, fmt.Errorf("decrypt seq %d: %w", seq, err)
	}
	return plain, nil
}

// ---------- upload pipeline ----------

func (c *Conn) uploadWorker() {
	defer c.uploadWg.Done()
	for job := range c.uploadQueue {
		t0 := time.Now()
		err := c.uploadWithTimeout(job.path, job.data)
		dt := time.Since(t0)
		c.s3Puts.Add(1)
		if err != nil {
			c.s3PutErrors.Add(1)
			log.Printf("[fedarisha:%s] upload ERR %s (%d B, %v): %v", c.sessionID[:8], job.path, len(job.data), dt, err)
		} else {
			log.Printf("[fedarisha:%s] upload %s (%d B, %v)", c.sessionID[:8], job.path, len(job.data), dt)
		}
	}
}

// uploadWithTimeout PUTs with hedging: it launches one PUT, and if that hasn't
// finished within the (size-aware) hedge delay it fires a duplicate on a fresh
// request (up to uploadAttempts), returning the first success. gpucloud tail
// latency is usually per-connection, so a duplicate on a new connection beats
// waiting out a stuck one — keeping a single slow file from blocking the
// in-order reader and tripping the hole watchdog. PutObject is idempotent for a
// fixed key+body. Small files hedge fast (a stuck control/window-update frame
// freezes the return path); large files hedge lazily (a 2MB PUT is slow because
// it's big, not stuck — re-uploading it would just burn uplink bandwidth).
func (c *Conn) uploadWithTimeout(path string, data []byte) error {
	ctx, cancel := context.WithTimeout(c.ctx, uploadTimeout)
	defer cancel()

	hedgeDelay := uploadHedgeSmall
	if len(data) >= uploadHedgeSizeCut {
		hedgeDelay = uploadHedgeLarge
	}

	errCh := make(chan error, uploadAttempts)
	put := func() {
		select {
		case errCh <- c.store.Upload(ctx, path, data):
		case <-ctx.Done():
		}
	}

	go put()
	inflight := 1
	hedge := time.NewTimer(hedgeDelay)
	defer hedge.Stop()

	var lastErr error
	for {
		select {
		case err := <-errCh:
			inflight--
			if err == nil {
				return nil // first success wins; defer cancel() aborts the rest
			}
			lastErr = err
			if c.ctx.Err() != nil {
				return err // session closing
			}
			if inflight == 0 { // all attempts failed — fire another if budget left
				go put()
				inflight++
			}
		case <-hedge.C:
			if inflight < uploadAttempts {
				go put()
				inflight++
			}
			hedge.Reset(hedgeDelay)
		case <-ctx.Done():
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			return lastErr
		}
	}
}

// ---------- flush ----------

func (c *Conn) flushLoop() {
	ticker := time.NewTicker(c.writeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closed:
			return
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.flush()
		case <-c.flushNow:
			c.flush()
		}
	}
}

// pendingChunk is a slice of drained write-buffer data with its assigned
// sequence number. Seq is assigned under writeMu (see sliceChunksLocked) so the
// order of sequence numbers always matches the order bytes were drained.
type pendingChunk struct {
	seq  uint64
	data []byte
}

// sliceChunksLocked splits drained data into maxFileSize chunks and assigns each
// the next writeSeq. It MUST be called with writeMu held.
//
// This is the ONLY place writeSeq is advanced, and holding writeMu across the
// read+increment is load-bearing: flush() (flushLoop ticker) and forceFlush()
// (the yamux send goroutine via Write) run on different goroutines, so the old
// code — which read `seq := c.writeSeq` and did `c.writeSeq++` OUTSIDE the lock
// — was a data race. A lost increment skipped a sequence number: the server
// would write ...17f, 181... with no 180, and the client's in-order reader then
// waited forever on a file that was never produced → hole watchdog → re-dial →
// "Download Test Error" (with no upload ERR, because the PUT was never even
// attempted). Assigning seq under the same lock that drains the buffer also
// guarantees seq order == byte order, so the stream can't be reordered.
func (c *Conn) sliceChunksLocked(data []byte) []pendingChunk {
	var chunks []pendingChunk
	for len(data) > 0 {
		chunk := data
		if len(chunk) > c.maxFileSize {
			chunk = data[:c.maxFileSize]
		}
		data = data[len(chunk):]
		chunks = append(chunks, pendingChunk{seq: c.writeSeq, data: chunk})
		c.writeSeq++
	}
	return chunks
}

// sendChunks encodes, encrypts and enqueues chunks. Called WITHOUT writeMu held:
// encryption is CPU work and the enqueue can block on a full uploadQueue, neither
// of which should stall Write(). Concurrent sendChunks calls are safe — each owns
// a disjoint set of unique, ordered seqs, and the reader reorders by seq, so the
// order PUTs actually land in does not matter.
func (c *Conn) sendChunks(chunks []pendingChunk) {
	for _, ch := range chunks {
		encoded := encodePayload(ch.data)
		encrypted := c.encrypt(encoded, ch.seq)
		path := c.sessDir + "/" + SeqFileName(c.writePrefix, ch.seq)
		select {
		case c.uploadQueue <- uploadJob{path: path, data: encrypted}:
		case <-c.closed:
			return
		case <-c.ctx.Done():
			return
		}
	}
}

// flush takes buffered data, splits into chunks, and enqueues them for
// concurrent upload. Does NOT block on the upload completing — that's
// the job of the upload workers.
//
// During bulk transfers, we delay small flushes to accumulate larger files.
// Each S3 upload costs ~100ms overhead regardless of size, so bigger = better throughput.
func (c *Conn) flush() {
	c.writeMu.Lock()
	if c.writeBuf.Len() == 0 {
		c.writeMu.Unlock()
		return
	}
	size := c.writeBuf.Len()
	now := time.Now()

	// If buffer is small and we flushed recently, let data accumulate — but only
	// briefly. This window is a direct add to tunnel latency: a small interactive
	// write (an Ookla upload chunk, an HTTP request, a yamux frame) waits here
	// before it even becomes a c_ file, and the round-trip already costs ~1s of
	// poll+List+GET on each hop. Bulk transfers are unaffected — they fill past
	// maxFileSize/2 (or hit maxFileSize → forceFlush) and skip this entirely — so
	// shrinking 100ms→25ms only sharpens the latency-sensitive small-write path
	// (which is exactly what breaks Ookla's upload measurement) without hurting
	// throughput batching.
	if size < c.maxFileSize/2 && now.Sub(c.lastFlush) < 25*time.Millisecond {
		c.writeMu.Unlock()
		return
	}

	c.lastFlush = now
	data := make([]byte, size)
	copy(data, c.writeBuf.Bytes())
	c.writeBuf.Reset()
	chunks := c.sliceChunksLocked(data)
	c.writeMu.Unlock()

	c.sendChunks(chunks)
}

// forceFlush bypasses the accumulation delay and flushes immediately.
// Used when the buffer reaches maxFileSize.
func (c *Conn) forceFlush() {
	c.writeMu.Lock()
	if c.writeBuf.Len() == 0 {
		c.writeMu.Unlock()
		return
	}
	c.lastFlush = time.Now()
	data := make([]byte, c.writeBuf.Len())
	copy(data, c.writeBuf.Bytes())
	c.writeBuf.Reset()
	chunks := c.sliceChunksLocked(data)
	c.writeMu.Unlock()

	c.sendChunks(chunks)
}

// ---------- poll ----------

func (c *Conn) pollLoop() {
	var emptyPolls int
	for {
		select {
		case <-c.closed:
			return
		case <-c.ctx.Done():
			return
		default:
		}

		n := c.fetchNext()

		if n > 0 {
			if emptyPolls > 0 {
				log.Printf("[fedarisha:%s] poll: %d empty polls before data arrived", c.sessionID[:8], emptyPolls)
				emptyPolls = 0
			}
			c.lastRecvActive.Store(time.Now().UnixNano())
			continue
		}

		emptyPolls++

		// Webhook mode: wait for notification with a safety fallback poll.
		// This eliminates almost all empty S3 GETs — we only fetch when we
		// know a file was just created.
		if c.notify != nil {
			select {
			case <-c.closed:
				return
			case <-c.ctx.Done():
				return
			case <-c.notify:
				continue
			case <-time.After(30 * time.Second):
				// Safety fallback: poll in case a webhook notification was lost.
				continue
			}
		}

		// No webhook — adaptive delay: three tiers based on how recently we got data.
		// Page loads have bursty patterns — 2-5s gaps between resource groups.
		sinceActive := time.Since(time.Unix(0, c.lastRecvActive.Load()))

		// Each poll is a List (tens to ~100ms), not a cheap speculative GET. The
		// floor only applies AFTER an empty poll (a successful fetch loops back
		// immediately), so it governs the discovery latency of interactive,
		// ping-pong traffic — an Ookla upload chunk, the request half of an HTTP
		// round trip — where data arrives between empty polls. 90ms keeps the
		// floor just above the ~80ms List time so polls don't overlap and flood
		// the read pool (the old 20ms floor caused a List-storm that starved the
		// missing-file landing and wedged the session), while shaving ~60ms per
		// hop off the round trip vs the previous 150ms. Above the floor, back off
		// the longer we've gone without data.
		var delay time.Duration
		if sinceActive < 5*time.Second {
			delay = 90 * time.Millisecond
		} else if sinceActive < 30*time.Second {
			delay = 300 * time.Millisecond
		} else {
			delay = 700 * time.Millisecond
		}

		select {
		case <-c.closed:
			return
		case <-c.ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// fetchNext discovers the read-direction files present in the session dir with
// a single List, then downloads and consumes the contiguous run starting at
// readSeq.
//
// Listing first is the key to avoiding a pathology of the old speculative
// scheme: when the writer was flow-control-limited and trickling data, GETting
// files by predicted sequence number produced a storm of 404s (one bursty
// download logged 157ك wasted GETs) that saturated the read path and was the
// dominant cause of the session wedging. Ceph RGW's bucket index is strongly
// consistent and List is ~50ms regardless of count, so one List reveals exactly
// which files exist and we GET only those — zero 404s.
//
// Uploads land out of order (many upload workers), so List may show a hole at
// readSeq with later files already present. We GET every present file in the
// window in parallel, consume contiguously from readSeq, and stash the
// past-the-hole arrivals in prefetchCache so the next poll doesn't re-fetch
// them — combining List's no-404 property with out-of-order tolerance.
func (c *Conn) fetchNext() int {
	fetchStart := time.Now()

	infos, err := c.store.List(c.ctx, c.sessDir, c.readPrefix)
	if err != nil {
		return 0 // transient list error — pollLoop backs off and retries
	}
	present := make(map[uint64]struct{}, len(infos))
	for _, fi := range infos {
		if fi.IsDir {
			continue
		}
		if _, seq, ok := ParseSeqFileName(fi.Name); ok && seq >= c.readSeq {
			present[seq] = struct{}{}
		}
	}

	// laterFiles: List shows at least one file past readSeq. Captured before the
	// speculative probe below so the hole watchdog can tell "data exists beyond a
	// stuck head" from "nothing to read yet".
	laterFiles := false
	for seq := range present {
		if seq > c.readSeq {
			laterFiles = true
			break
		}
	}

	// Speculative head probe: if List shows later files but NOT readSeq itself,
	// it may be a real hole (writer hasn't produced it) OR just List index lag —
	// on Ceph under heavy load the bucket index can trail a freshly-PUT object
	// that a direct GET would already return (object read-after-write is strong,
	// the List index is not). So when there's data past readSeq, also try to GET
	// readSeq by name. This is ONE speculative GET (not the old 404 storm) and it
	// turns a List-lag stall — which otherwise persists into a hole-watchdog
	// re-dial mid-speedtest — into an immediate fetch.
	if _, ok := present[c.readSeq]; !ok && laterFiles {
		present[c.readSeq] = struct{}{}
	}

	// Sequence numbers to fetch this round: present-and-not-cached, within a
	// bounded window from readSeq (caps memory and per-round work).
	type fetched struct {
		data []byte
		err  error
	}
	results := make(map[uint64]fetched, maxReadBatch)
	sem := make(chan struct{}, maxReadConcurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup
	// Fetch at most maxReadBatch present files this round. We scan a bit past
	// readSeq so out-of-order arrivals (a hole at readSeq with later files
	// present) still pipeline, but we do NOT chase hundreds of files into the
	// cache while blocked on a hole — that just burns the read pool and keeps
	// the missing file from landing.
	fetching := 0
	for seq := c.readSeq; seq < c.readSeq+maxReadBatch && fetching < maxReadBatch; seq++ {
		if _, ok := present[seq]; !ok {
			continue // not written yet (a real hole) — skip, no 404
		}
		c.prefetchMu.Lock()
		if _, cached := c.prefetchCache[seq]; cached {
			c.prefetchMu.Unlock()
			continue // already have it from a previous round
		}
		c.prefetchMu.Unlock()

		fetching++
		wg.Add(1)
		sem <- struct{}{}
		go func(seq uint64) {
			defer wg.Done()
			defer func() { <-sem }()
			path := c.sessDir + "/" + SeqFileName(c.readPrefix, seq)
			data, derr := c.downloadWithTimeout(path)
			mu.Lock()
			results[seq] = fetched{data, derr}
			mu.Unlock()
		}(seq)
	}
	wg.Wait()

	// Consume strictly in order from readSeq, drawing each file from this
	// round's results or the prefetch cache. Stop at the first seq we don't have
	// (a genuine hole — the writer hasn't produced it yet) and resume there next
	// poll. Files fetched past a hole stay cached.
	consumed := 0
	for {
		seq := c.readSeq
		var data []byte
		var derr error
		c.prefetchMu.Lock()
		if cd, ok := c.prefetchCache[seq]; ok {
			data = cd
			delete(c.prefetchCache, seq)
			c.prefetchMu.Unlock()
		} else {
			c.prefetchMu.Unlock()
			r, ok := results[seq]
			if !ok {
				break // not available this round
			}
			delete(results, seq)
			data, derr = r.data, r.err
		}
		if derr != nil || len(data) == 0 {
			break // GET failed/empty — treat as a hole, retry next poll
		}

		decrypted, e := c.decrypt(data, seq)
		if e != nil {
			log.Printf("[fedarisha:%s] decrypt error seq %d: %v", c.sessionID[:8], seq, e)
			break
		}
		payload, e := decodePayload(decrypted)
		if e != nil {
			log.Printf("[fedarisha:%s] decode error: %v", c.sessionID[:8], e)
			break
		}

		c.readMu.Lock()
		c.readBuf.Write(payload)
		c.readMu.Unlock()
		c.readCond.Broadcast()

		c.lastRecvMu.Lock()
		c.lastRecv = time.Now()
		c.lastRecvMu.Unlock()

		c.readSeq++
		consumed++

		// Detached delete (robust against a Close race cancelling c.ctx).
		path := c.sessDir + "/" + SeqFileName(c.readPrefix, seq)
		go func(p string) {
			dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = c.store.Delete(dctx, p)
		}(path)
	}

	// Anything fetched this round but past the consume point: cache it so the
	// next poll consumes it without re-GETting.
	for seq, r := range results {
		if r.err == nil && len(r.data) > 0 {
			c.prefetchMu.Lock()
			c.prefetchCache[seq] = r.data
			c.prefetchMu.Unlock()
		}
	}

	if consumed > 0 {
		log.Printf("[fedarisha:%s] fetchNext: got %d files (%d present), total %v", c.sessionID[:8], consumed, len(present), time.Since(fetchStart))
	}

	// Hole watchdog: later files exist but we consumed nothing — readSeq is still
	// missing even after the speculative head GET, so it's a genuine hole (not
	// List lag). Normally a brief out-of-order gap (a large data file's PUT
	// finishing after small control files with higher seqs) that fills in well
	// under a second. If it persists past holeTimeout the peer is wedged on flow
	// control and the file will never arrive — tear the session down so the
	// outbound re-dials promptly instead of waiting for keepalive.
	hole := laterFiles && consumed == 0
	switch {
	case !hole:
		c.holeSince = time.Time{}
	case c.holeSince.IsZero():
		c.holeSince = time.Now()
	case time.Since(c.holeSince) > holeTimeout:
		log.Printf("[fedarisha:%s] hole at seq %d persisted %v (%d present), closing for re-dial",
			c.sessionID[:8], c.readSeq, time.Since(c.holeSince).Round(time.Millisecond), len(present))
		c.Close()
	}

	return consumed
}

// downloadWithTimeout GETs a file with hedging: it launches one request, and if
// that hasn't answered within readHedgeDelay it fires a duplicate on a fresh
// request (up to readGetAttempts), returning the first success. This caps the
// damage of an S3 tail-latency outlier — which, because reads are delivered in
// order, would otherwise stall the whole batch — at roughly readHedgeDelay plus
// one healthy GET instead of seconds. A fast error (NoSuchKey for a not-yet-
// written file) returns immediately: it arrives before the hedge fires, so it's
// passed straight back as the writer-frontier signal without spawning dupes.
func (c *Conn) downloadWithTimeout(path string) ([]byte, error) {
	type res struct {
		data []byte
		err  error
	}

	ctx, cancel := context.WithTimeout(c.ctx, readGetTimeout)
	defer cancel()

	resCh := make(chan res, readGetAttempts)
	get := func() {
		data, err := c.store.Download(ctx, path)
		c.s3Gets.Add(1)
		if err != nil {
			c.s3GetErrors.Add(1)
		}
		select {
		case resCh <- res{data, err}:
		case <-ctx.Done():
		}
	}

	go get()
	inflight := 1

	hedge := time.NewTimer(readHedgeDelay)
	defer hedge.Stop()

	var lastErr error
	for {
		select {
		case r := <-resCh:
			inflight--
			if r.err == nil {
				return r.data, nil // first success wins; defer cancel() stops the rest
			}
			lastErr = r.err
			// Nothing else in flight (a fast frontier error, or every dupe
			// failed): hand the error back so the caller treats it as the gap.
			if inflight == 0 {
				return r.data, r.err
			}
		case <-hedge.C:
			if inflight < readGetAttempts {
				go get()
				inflight++
			}
			hedge.Reset(readHedgeDelay)
		case <-ctx.Done():
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			return nil, lastErr
		}
	}
}

// adaptReadAhead adjusts the read-ahead window. writerAhead is true when this
// poll proved the writer is still producing — either a full-window drain with
// no hole, or a hole with data already landed beyond it (out-of-order uploads).
// In that case we grow, so a high-throughput flood ramps the window up to fetch
// many files (and reorder gap-fillers) in parallel. When writerAhead is false
// (the whole window past readSeq was empty) we've genuinely caught up and
// shrink. elapsed gates growth only when caught-up-ish: during a real flood the
// batch is slow simply because it moved tens of MB, which must NOT block growth,
// so a flood (consumed a meaningful batch) grows regardless of elapsed.
// ---------- idle ----------

func (c *Conn) idleWatcher() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.lastRecvMu.Lock()
			idle := time.Since(c.lastRecv)
			c.lastRecvMu.Unlock()
			if idle > c.idleTimeout {
				log.Printf("[fedarisha] session %s idle timeout (%v > %v), closing", c.sessionID[:8], idle, c.idleTimeout)
				c.Close()
				return
			}
		}
	}
}

// batchDeleter is implemented by storage backends (currently S3) that can
// remove many objects in a single API round-trip. Falling back to per-file
// Delete when this isn't available is correct but slow enough that long
// sessions can blow past cleanupSession's deadline and leak files.
type batchDeleter interface {
	BatchDelete(ctx context.Context, paths []string) error
}

func (c *Conn) cleanupSession(ctx context.Context) {
	files, err := c.store.List(ctx, c.sessDir, "")
	if err != nil {
		return
	}
	if len(files) == 0 {
		_ = c.store.Delete(ctx, c.sessDir)
		return
	}
	paths := make([]string, 0, len(files))
	for _, f := range files {
		paths = append(paths, c.sessDir+"/"+f.Name)
	}
	if bd, ok := c.store.(batchDeleter); ok {
		// S3 DeleteObjects accepts up to 1000 keys per call.
		const chunkSize = 1000
		for i := 0; i < len(paths); i += chunkSize {
			end := i + chunkSize
			if end > len(paths) {
				end = len(paths)
			}
			_ = bd.BatchDelete(ctx, paths[i:end])
		}
	} else {
		for _, p := range paths {
			_ = c.store.Delete(ctx, p)
		}
	}
	_ = c.store.Delete(ctx, c.sessDir)
}

// ---------- net.Addr ----------

type fedarishaAddr struct{ tag string }

func (a fedarishaAddr) Network() string { return "fedarisha" }
func (a fedarishaAddr) String() string  { return a.tag }
