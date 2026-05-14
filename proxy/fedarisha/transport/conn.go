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
	sessDir   string // e.g. "sessions/abc123"
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
	writeBuf  bytes.Buffer
	writeMu   sync.Mutex
	flushNow  chan struct{}

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

	// Prefetch cache — stores data fetched ahead but not yet consumed
	// because earlier sequence numbers hadn't arrived yet.
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
	path    string
	data    []byte
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

const uploadWorkers = 8

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
		uploadQueue:   make(chan uploadJob, 16),
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
func (c *Conn) SetReadDeadline(t time.Time) error   { return nil }
func (c *Conn) SetWriteDeadline(t time.Time) error  { return nil }

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
		err := c.store.Upload(c.ctx, job.path, job.data)
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

	// If buffer is small and we flushed recently, let data accumulate.
	// After 100ms always flush (safety net for control messages).
	if size < c.maxFileSize/2 && now.Sub(c.lastFlush) < 100*time.Millisecond {
		c.writeMu.Unlock()
		return
	}

	c.lastFlush = now
	data := make([]byte, size)
	copy(data, c.writeBuf.Bytes())
	c.writeBuf.Reset()
	c.writeMu.Unlock()

	for len(data) > 0 {
		chunk := data
		if len(chunk) > c.maxFileSize {
			chunk = data[:c.maxFileSize]
		}
		data = data[len(chunk):]

		encoded := encodePayload(chunk)
		seq := c.writeSeq
		encrypted := c.encrypt(encoded, seq)
		name := SeqFileName(c.writePrefix, seq)
		path := c.sessDir + "/" + name
		c.writeSeq++

		select {
		case c.uploadQueue <- uploadJob{path: path, data: encrypted}:
		case <-c.closed:
			return
		case <-c.ctx.Done():
			return
		}
	}
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
	c.writeMu.Unlock()

	for len(data) > 0 {
		chunk := data
		if len(chunk) > c.maxFileSize {
			chunk = data[:c.maxFileSize]
		}
		data = data[len(chunk):]

		encoded := encodePayload(chunk)
		seq := c.writeSeq
		encrypted := c.encrypt(encoded, seq)
		name := SeqFileName(c.writePrefix, seq)
		path := c.sessDir + "/" + name
		c.writeSeq++

		select {
		case c.uploadQueue <- uploadJob{path: path, data: encrypted}:
		case <-c.closed:
			return
		case <-c.ctx.Done():
			return
		}
	}
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

		var delay time.Duration
		if sinceActive < 5*time.Second {
			// Active transfer or recent burst — poll fast but within S3 rate limits.
			delay = 20 * time.Millisecond
		} else if sinceActive < 30*time.Second {
			// Page might still be loading — moderate polling.
			delay = 100 * time.Millisecond
		} else {
			// Truly idle — slow polling to save S3 requests.
			delay = 500 * time.Millisecond
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

// fetchNext downloads the next expected file(s) from storage.
// Uses a prefetch cache to avoid re-downloading files that were fetched
// ahead but couldn't be consumed due to gaps in the sequence.
func (c *Conn) fetchNext() int {
	sinceActive := time.Since(time.Unix(0, c.lastRecvActive.Load()))
	ahead := 2
	if sinceActive < 5*time.Second {
		ahead = 4
	}

	type result struct {
		seq  uint64
		idx  int
		data []byte
		err  error
		path string
	}

	fetchStart := time.Now()
	ch := make(chan result, ahead)
	inflight := 0

	for i := 0; i < ahead; i++ {
		seq := c.readSeq + uint64(i)

		// Check prefetch cache first.
		c.prefetchMu.Lock()
		if data, ok := c.prefetchCache[seq]; ok {
			delete(c.prefetchCache, seq)
			c.prefetchMu.Unlock()
			name := SeqFileName(c.readPrefix, seq)
			path := c.sessDir + "/" + name
			ch <- result{seq: seq, idx: i, data: data, path: path}
			inflight++
			continue
		}
		c.prefetchMu.Unlock()

		// Not cached — download from S3.
		inflight++
		go func(idx int, seq uint64) {
			name := SeqFileName(c.readPrefix, seq)
			path := c.sessDir + "/" + name
			data, err := c.store.Download(c.ctx, path)
			c.s3Gets.Add(1)
			if err != nil {
				c.s3GetErrors.Add(1)
			}
			ch <- result{seq: seq, idx: idx, data: data, err: err, path: path}
		}(i, seq)
	}

	slots := make([]*result, ahead)
	received := 0
	consumed := 0
	nextSlot := 0

	for received < inflight {
		r := <-ch
		received++
		rCopy := r
		slots[r.idx] = &rCopy

		for nextSlot < ahead && slots[nextSlot] != nil {
			s := slots[nextSlot]
			if s.err != nil || len(s.data) == 0 {
				// Cache any successful results beyond the gap.
				for j := nextSlot + 1; j < ahead; j++ {
					if slots[j] != nil && slots[j].err == nil && len(slots[j].data) > 0 {
						c.prefetchMu.Lock()
						c.prefetchCache[slots[j].seq] = slots[j].data
						c.prefetchMu.Unlock()
					}
				}
				for received < inflight {
					extra := <-ch
					received++
					if extra.err == nil && len(extra.data) > 0 {
						c.prefetchMu.Lock()
						c.prefetchCache[extra.seq] = extra.data
						c.prefetchMu.Unlock()
					}
				}
				if consumed > 0 {
					log.Printf("[fedarisha:%s] fetchNext: got %d files, total %v", c.sessionID[:8], consumed, time.Since(fetchStart))
				}
				return consumed
			}

			decrypted, err := c.decrypt(s.data, s.seq)
			if err != nil {
				log.Printf("[fedarisha:%s] decrypt error seq %d: %v", c.sessionID[:8], s.seq, err)
				for received < inflight {
					<-ch
					received++
				}
				return consumed
			}
			payload, err := decodePayload(decrypted)
			if err != nil {
				log.Printf("[fedarisha:%s] decode error: %v", c.sessionID[:8], err)
				for received < inflight {
					<-ch
					received++
				}
				return consumed
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

			// Detached context: c.ctx may be cancelled by Close before this
			// goroutine runs, which would silently abort the delete and leak
			// the file. cleanupSession would normally catch it, but only if
			// List sees it within the cleanup deadline — easier to just make
			// the per-file delete robust against close races.
			go func(p string) {
				dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = c.store.Delete(dctx, p)
			}(s.path)

			nextSlot++
		}

		if slots[0] != nil && (slots[0].err != nil || len(slots[0].data) == 0) {
			// Cache successful results from later slots.
			for j := 1; j < ahead; j++ {
				if slots[j] != nil && slots[j].err == nil && len(slots[j].data) > 0 {
					c.prefetchMu.Lock()
					c.prefetchCache[slots[j].seq] = slots[j].data
					c.prefetchMu.Unlock()
				}
			}
			for received < inflight {
				extra := <-ch
				received++
				if extra.err == nil && len(extra.data) > 0 {
					c.prefetchMu.Lock()
					c.prefetchCache[extra.seq] = extra.data
					c.prefetchMu.Unlock()
				}
			}
			if consumed > 0 {
				log.Printf("[fedarisha:%s] fetchNext: got %d files, total %v", c.sessionID[:8], consumed, time.Since(fetchStart))
			}
			return consumed
		}
	}

	if consumed > 0 {
		log.Printf("[fedarisha:%s] fetchNext: got %d/%d files, total %v", c.sessionID[:8], consumed, ahead, time.Since(fetchStart))
	}
	return consumed
}

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
