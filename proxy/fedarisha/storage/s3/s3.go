// Package s3 implements the Storage interface for S3-compatible object stores.
// Works with AWS S3, MinIO, Backblaze B2, Cloudflare R2, etc.
package s3

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

// Config holds S3 connection parameters.
type Config struct {
	Bucket    string // S3 bucket name
	Prefix    string // Key prefix (e.g. "fedarisha/")
	Region    string // AWS region (default "us-east-1")
	Endpoint  string // Custom endpoint for S3-compatible services (MinIO, R2, etc.)
	AccessKey string
	SecretKey string
}

// S3Store implements storage.Storage for S3-compatible backends.
type S3Store struct {
	cfg    Config
	client *s3.Client
}

// New creates a new S3 storage backend.
func New(cfg Config) *S3Store {
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.Prefix != "" && !strings.HasSuffix(cfg.Prefix, "/") {
		cfg.Prefix += "/"
	}

	// Tuned HTTP transport for high-frequency small requests.
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:          50,
			MaxIdleConnsPerHost:   30,
			IdleConnTimeout:       120 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}

	opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.Region = cfg.Region
			o.Credentials = credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")
			o.HTTPClient = httpClient
		},
	}
	if cfg.Endpoint != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		})
	}

	client := s3.New(s3.Options{}, opts...)

	return &S3Store{cfg: cfg, client: client}
}

func (s *S3Store) key(path string) string {
	path = strings.TrimPrefix(path, "/")
	return s.cfg.Prefix + path
}

// ---------- storage.Storage ----------

func (s *S3Store) Init(ctx context.Context) error {
	// Verify access by doing a HeadBucket.
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.cfg.Bucket),
	})
	return err
}

func (s *S3Store) EnsureDir(_ context.Context, _ string) error {
	// S3 has no directories — they're implicit from key prefixes.
	return nil
}

func (s *S3Store) Upload(ctx context.Context, path string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(s.key(path)),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s *S3Store) Download(ctx context.Context, path string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(s.key(path)),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s *S3Store) List(ctx context.Context, dir string, prefix string) ([]storage.FileInfo, error) {
	dirKey := s.key(dir)
	if !strings.HasSuffix(dirKey, "/") {
		dirKey += "/"
	}

	searchPrefix := dirKey
	if prefix != "" {
		searchPrefix = dirKey + prefix
	}

	var result []storage.FileInfo
	var token *string
	for {
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.cfg.Bucket),
			Prefix:            aws.String(searchPrefix),
			Delimiter:         aws.String("/"),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range out.Contents {
			name := strings.TrimPrefix(aws.ToString(obj.Key), dirKey)
			if name == "" {
				continue
			}
			var modified time.Time
			if obj.LastModified != nil {
				modified = *obj.LastModified
			}
			result = append(result, storage.FileInfo{
				Name:     name,
				Path:     aws.ToString(obj.Key),
				Size:     aws.ToInt64(obj.Size),
				Modified: modified,
				Created:  modified,
				IsDir:    false,
			})
		}

		for _, cp := range out.CommonPrefixes {
			name := strings.TrimPrefix(aws.ToString(cp.Prefix), dirKey)
			name = strings.TrimSuffix(name, "/")
			if name == "" {
				continue
			}
			result = append(result, storage.FileInfo{
				Name:  name,
				Path:  aws.ToString(cp.Prefix),
				IsDir: true,
			})
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}

	return result, nil
}

func (s *S3Store) Delete(ctx context.Context, path string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(s.key(path)),
	})
	return err
}

func (s *S3Store) Watch(ctx context.Context, dir string, since time.Time, timeout time.Duration) ([]storage.FileInfo, error) {
	// S3 has no push notifications — fall back to polling.
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		files, err := s.List(ctx, dir, "")
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
		case <-time.After(200 * time.Millisecond):
		}
	}
	return nil, nil
}

// BatchDelete removes multiple objects in one API call (up to 1000).
func (s *S3Store) BatchDelete(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	objects := make([]s3types.ObjectIdentifier, len(paths))
	for i, p := range paths {
		objects[i] = s3types.ObjectIdentifier{Key: aws.String(s.key(p))}
	}
	_, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(s.cfg.Bucket),
		Delete: &s3types.Delete{Objects: objects, Quiet: aws.Bool(true)},
	})
	return err
}
