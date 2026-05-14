// Package local implements the Storage interface using the local filesystem.
// Use it for testing or with cloud-sync clients (Yandex Disk desktop app, etc.).
package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

type Config struct {
	RootDir string `json:"root_dir"`
}

type Local struct {
	root string
}

func New(cfg Config) *Local {
	return &Local{root: cfg.RootDir}
}

func (l *Local) Init(_ context.Context) error {
	return os.MkdirAll(l.root, 0o755)
}

func (l *Local) EnsureDir(_ context.Context, path string) error {
	return os.MkdirAll(l.abs(path), 0o755)
}

func (l *Local) Upload(_ context.Context, path string, data []byte) error {
	fp := l.abs(path)
	if err := os.MkdirAll(filepath.Dir(fp), 0o755); err != nil {
		return err
	}
	return os.WriteFile(fp, data, 0o644)
}

func (l *Local) Download(_ context.Context, path string) ([]byte, error) {
	return os.ReadFile(l.abs(path))
}

func (l *Local) List(_ context.Context, dir string, prefix string) ([]storage.FileInfo, error) {
	entries, err := os.ReadDir(l.abs(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var result []storage.FileInfo
	for _, e := range entries {
		if prefix != "" && !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		result = append(result, storage.FileInfo{
			Name:     e.Name(),
			Path:     filepath.Join(dir, e.Name()),
			Size:     info.Size(),
			Created:  info.ModTime(), // POSIX has no birth time — use mtime.
			Modified: info.ModTime(),
			IsDir:    e.IsDir(),
		})
	}
	return result, nil
}

func (l *Local) Delete(_ context.Context, path string) error {
	err := os.Remove(l.abs(path))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (l *Local) Watch(ctx context.Context, dir string, since time.Time, timeout time.Duration) ([]storage.FileInfo, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		files, err := l.List(ctx, dir, "")
		if err != nil {
			return nil, err
		}
		var newer []storage.FileInfo
		for _, f := range files {
			if f.Modified.After(since) {
				newer = append(newer, f)
			}
		}
		if len(newer) > 0 {
			return newer, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil, nil
}

func (l *Local) abs(rel string) string {
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" {
		return l.root
	}
	fp := filepath.Join(l.root, filepath.FromSlash(rel))
	// Safety: prevent path traversal.
	abs, err := filepath.Abs(fp)
	if err != nil {
		return fp
	}
	rootAbs, _ := filepath.Abs(l.root)
	if !strings.HasPrefix(abs, rootAbs) {
		panic(fmt.Sprintf("path traversal attempt: %s", rel))
	}
	return abs
}
