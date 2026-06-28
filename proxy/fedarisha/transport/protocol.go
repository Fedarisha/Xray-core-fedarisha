// Package transport implements the FEDARISHA transport layer.
//
// Protocol overview:
//
//  1. Client generates a random session ID and creates a session directory
//     on the remote storage: {base}/{sessionID}/
//
//  2. Both sides exchange data by writing small files into that directory.
//     File naming: {direction}_{seqNo}  (e.g. "c_00000042", "s_00000001")
//     - "c_" prefix = client → server
//     - "s_" prefix = server → client
//     Compressed files use "z" infix: "cz00000042", "sz00000001"
//
//  3. Each side tries to GET the next expected file directly (no List).
//     Adaptive backoff: fast polling when active, slower when idle.
//
//  4. An idle timeout closes the session if no data received.
//     With yamux keepalives this is effectively a connectivity check.
//
//  5. A special file "c_hello" is created by the client to signal a new session.
//     The server watches for new session directories.
package transport

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// PrefixClient is the file prefix for client-to-server data.
	PrefixClient = "c_"
	// PrefixServer is the file prefix for server-to-client data.
	PrefixServer = "s_"
	// HelloFile signals a new session to the server.
	HelloFile = "c_hello"
	// AckFile signals session acceptance by server.
	AckFile = "s_ack"

	// DefaultPollInterval is the base interval for polling new files.
	DefaultPollInterval = 100 * time.Millisecond
	// DefaultWriteInterval is the write buffer flush interval.
	DefaultWriteInterval = 20 * time.Millisecond
	// DefaultIdleTimeout closes a session after this long with no data.
	DefaultIdleTimeout = 300 * time.Second
	// DefaultMaxFileSize limits data per file (bytes).
	DefaultMaxFileSize = 2 * 1024 * 1024 // 2 MB
	// CleanupAge — files older than this are deleted during garbage collection.
	CleanupAge = 30 * time.Second
)

// GenerateSessionID returns a random 16-byte hex string.
func GenerateSessionID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// SeqFileName returns the file name for a given direction and sequence number.
func SeqFileName(prefix string, seq uint64) string {
	return fmt.Sprintf("%s%08x", prefix, seq)
}

// ParseSeqFileName extracts the prefix and sequence number from a file name.
func ParseSeqFileName(name string) (prefix string, seq uint64, ok bool) {
	for _, p := range []string{PrefixClient, PrefixServer} {
		if strings.HasPrefix(name, p) {
			rest := strings.TrimPrefix(name, p)
			n, err := strconv.ParseUint(rest, 16, 64)
			if err != nil {
				return "", 0, false
			}
			return p, n, true
		}
	}
	return "", 0, false
}

// SortBySeq sorts file names by their sequence number.
func SortBySeq(names []string) {
	sort.Slice(names, func(i, j int) bool {
		_, si, _ := ParseSeqFileName(names[i])
		_, sj, _ := ParseSeqFileName(names[j])
		return si < sj
	})
}
