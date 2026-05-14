package transport

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

const x25519KeySize = 32

// Dialer creates outbound FEDARISHA connections (client side).
type Dialer struct {
	Store         storage.Storage
	SessionsDir   string        // e.g. "sessions"
	PollInterval  time.Duration
	WriteInterval time.Duration
	IdleTimeout   time.Duration
	MaxFileSize   int
}

// Dial creates a new session and returns a net.Conn backed by cloud storage.
// Performs X25519 key exchange with the server to establish E2E encryption.
func (d *Dialer) Dial(ctx context.Context) (*Conn, error) {
	sessID := GenerateSessionID()
	sessDir := d.SessionsDir + "/" + sessID

	log.Printf("[fedarisha-client] dialing new session %s", sessID[:8])

	// Generate client X25519 key pair.
	privKey, pubKey, err := GenerateX25519()
	if err != nil {
		return nil, fmt.Errorf("fedarisha dial: %w", err)
	}

	// Create session directory.
	if err := d.Store.EnsureDir(ctx, sessDir); err != nil {
		return nil, fmt.Errorf("fedarisha dial: create session dir: %w", err)
	}

	// Write hello file: sessID + client public key.
	helloData := make([]byte, len(sessID)+x25519KeySize)
	copy(helloData, sessID)
	copy(helloData[len(sessID):], pubKey)

	helloPath := sessDir + "/" + HelloFile
	if err := d.Store.Upload(ctx, helloPath, helloData); err != nil {
		return nil, fmt.Errorf("fedarisha dial: write hello: %w", err)
	}

	// Wait for server ACK containing server's public key.
	ackPath := sessDir + "/" + AckFile
	var serverPub []byte
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		data, err := d.Store.Download(ctx, ackPath)
		if err == nil && len(data) >= x25519KeySize {
			serverPub = data[:x25519KeySize]
			log.Printf("[fedarisha-client] session %s accepted by server (encrypted)", sessID[:8])
			_ = d.Store.Delete(ctx, ackPath)
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	if serverPub == nil {
		_ = d.Store.Delete(ctx, helloPath)
		_ = d.Store.Delete(ctx, sessDir)
		return nil, fmt.Errorf("fedarisha dial: server did not ACK within 60s")
	}

	// Derive shared AES-256-GCM cipher.
	aead, err := DeriveAEAD(privKey, serverPub, sessID)
	if err != nil {
		return nil, fmt.Errorf("fedarisha dial: derive key: %w", err)
	}

	return NewConn(ConnConfig{
		Store:         d.Store,
		SessionID:     sessID,
		SessionDir:    sessDir,
		IsClient:      true,
		PollInterval:  d.PollInterval,
		WriteInterval: d.WriteInterval,
		IdleTimeout:   d.IdleTimeout,
		MaxFileSize:   d.MaxFileSize,
		Cipher:        aead,
	}), nil
}
