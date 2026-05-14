package fedarisha

import (
	"context"
	"log"

	fedstorage "github.com/xtls/xray-core/proxy/fedarisha/storage"
)

type lifecycleSetupper interface {
	SetupLifecycle(ctx context.Context, prefix string) error
}

// registerLifecycle installs (or refreshes) the bucket-level expiration rule
// that sweeps orphaned session writes for this inbound's prefix. Best-effort:
// the inbound starts even if the rule cannot be set, since transport works
// without lifecycle and the only consequence of failure is bucket bloat.
//
// Storage backends that do not implement lifecycleSetupper (local, yadisk)
// are skipped silently — there is nothing to configure.
func registerLifecycle(ctx context.Context, tag string, storageConfig *StorageConfig, store fedstorage.Storage) {
	if !isS3Storage(storageConfig) {
		return
	}
	setupper, ok := store.(lifecycleSetupper)
	if !ok {
		return
	}
	prefix := normalizeS3Prefix(storageConfig.GetPrefix())
	if prefix == "" {
		log.Printf("[fedarisha] inbound %q: lifecycle setup skipped (empty prefix)", tag)
		return
	}
	log.Printf("[fedarisha] inbound %q: configuring S3 bucket lifecycle (prefix: %s, expire: 1d)", tag, prefix)
	if err := setupper.SetupLifecycle(ctx, prefix); err != nil {
		log.Printf("[fedarisha] inbound %q: WARNING lifecycle setup failed: %v", tag, err)
	}
}
