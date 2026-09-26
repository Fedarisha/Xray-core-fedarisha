package transport

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
	"github.com/xtls/xray-core/proxy/fedarisha/storage/local"
)

type recordingStorage struct {
	storage.Storage
	listed chan string
}

func (s *recordingStorage) List(ctx context.Context, dir, prefix string) ([]storage.FileInfo, error) {
	select {
	case s.listed <- dir:
	default:
	}
	return s.Storage.List(ctx, dir, prefix)
}

func TestMultiUserListenerDefaultsAndPollTuning(t *testing.T) {
	for _, withWebhook := range []bool{false, true} {
		name := "polling"
		if withWebhook {
			name = "webhook-fallback"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.MkdirAll(filepath.Join(root, "alice", DefaultSessionsDir), 0o755); err != nil {
				t.Fatal(err)
			}
			store := &recordingStorage{
				Storage: local.New(local.Config{RootDir: root}),
				listed:  make(chan string, 20),
			}
			opts := ListenOpts{PollInterval: 15 * time.Millisecond}
			if withWebhook {
				opts.WebhookHub = NewWebhookHub("", "")
			}
			listener, err := ListenMultiUser(context.Background(), store, "", opts)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if listener.SessionsDir != DefaultSessionsDir {
				t.Fatalf("sessions dir = %q, want %q", listener.SessionsDir, DefaultSessionsDir)
			}

			deadline := time.After(300 * time.Millisecond)
			for {
				select {
				case dir := <-store.listed:
					if dir == "alice/sessions" {
						return
					}
				case <-deadline:
					t.Fatal("listener did not scan alice/sessions at configured poll interval")
				}
			}
		})
	}
}

func TestEffectiveSessionsDir(t *testing.T) {
	for input, want := range map[string]string{
		"":         DefaultSessionsDir,
		"   ":      DefaultSessionsDir,
		"sessions": "sessions",
		"custom":   "custom",
	} {
		if got := effectiveSessionsDir(input); got != want {
			t.Errorf("effectiveSessionsDir(%q) = %q, want %q", input, got, want)
		}
	}
}
