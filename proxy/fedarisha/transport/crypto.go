package transport

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"

	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
)

// GenerateX25519 creates a new X25519 key pair.
func GenerateX25519() (priv, pub []byte, err error) {
	priv = make([]byte, curve25519.ScalarSize)
	if _, err := rand.Read(priv); err != nil {
		return nil, nil, fmt.Errorf("generate x25519: %w", err)
	}
	pub, err = curve25519.X25519(priv, curve25519.Basepoint)
	if err != nil {
		return nil, nil, fmt.Errorf("generate x25519 public: %w", err)
	}
	return priv, pub, nil
}

// DeriveAEAD performs X25519 key exchange followed by HKDF-SHA256 key derivation,
// returning an AES-256-GCM AEAD cipher.
func DeriveAEAD(privateKey, peerPublicKey []byte, sessionID string) (cipher.AEAD, error) {
	shared, err := curve25519.X25519(privateKey, peerPublicKey)
	if err != nil {
		return nil, fmt.Errorf("x25519 key exchange: %w", err)
	}

	hkdfR := hkdf.New(sha256.New, shared, []byte(sessionID), []byte("fedarisha-e2e-v1"))
	key := make([]byte, 32)
	if _, err := io.ReadFull(hkdfR, key); err != nil {
		return nil, fmt.Errorf("hkdf derive: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes cipher: %w", err)
	}
	return cipher.NewGCM(block)
}

// MakeNonce builds a 12-byte GCM nonce from direction prefix and sequence number.
// Bytes 0-3: prefix ("c_" or "s_"), bytes 4-11: big-endian sequence number.
// Each (direction, seq) pair is unique, guaranteeing nonce uniqueness.
func MakeNonce(prefix string, seq uint64) []byte {
	nonce := make([]byte, 12)
	copy(nonce, prefix)
	binary.BigEndian.PutUint64(nonce[4:], seq)
	return nonce
}
