package s3

import (
	"context"
	"testing"
)

func TestRestrictedCredentialsSkipBucketHead(t *testing.T) {
	store := New(Config{
		Bucket:          "bucket",
		Endpoint:        "http://127.0.0.1:1",
		AccessKey:       "prefix-scoped",
		SecretKey:       "secret",
		SkipBucketCheck: true,
	})
	if err := store.Init(context.Background()); err != nil {
		t.Fatalf("restricted credentials should not require HeadBucket: %v", err)
	}
}
