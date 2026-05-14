// Package storage defines the interface for remote storage backends used by FEDARISHA.
package storage

import (
	"context"
	"io"
	"time"
)

// FileInfo represents metadata about a file in remote storage.
type FileInfo struct {
	Name      string
	Path      string
	Size      int64
	Created   time.Time
	Modified  time.Time
	IsDir     bool
	MediaType string
}

// Storage defines operations that any FEDARISHA-compatible storage backend must implement.
// Implementations include Yandex Disk, Google Drive, S3, local filesystem, etc.
type Storage interface {
	// Init authenticates and prepares the storage backend.
	Init(ctx context.Context) error

	// EnsureDir creates a directory (and parents) if it doesn't exist.
	EnsureDir(ctx context.Context, path string) error

	// Upload writes data to a file at the given path, creating or overwriting it.
	Upload(ctx context.Context, path string, data []byte) error

	// Download reads the entire contents of a file.
	Download(ctx context.Context, path string) ([]byte, error)

	// List returns files in a directory, optionally filtered by prefix.
	List(ctx context.Context, dir string, prefix string) ([]FileInfo, error)

	// Delete removes a file or empty directory.
	Delete(ctx context.Context, path string) error

	// Watch is an optional optimisation: block until new files appear or timeout.
	// Backends that don't support push notifications simply return immediately.
	Watch(ctx context.Context, dir string, since time.Time, timeout time.Duration) ([]FileInfo, error)
}

// ReadCloserFunc adapts a function into an io.ReadCloser.
type ReadCloserFunc struct {
	io.Reader
	CloseFunc func() error
}

func (r *ReadCloserFunc) Close() error {
	if r.CloseFunc != nil {
		return r.CloseFunc()
	}
	return nil
}
