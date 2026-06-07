package s3

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	lifecycleRuleIDPrefix = "fedarisha-expire-"
	lifecycleExpireDays   = int32(1)
)

// SetupLifecycle ensures the bucket has a lifecycle rule that expires objects
// under prefix one day after their last modification. Idempotent: re-running
// rewrites only the rule keyed by this prefix and preserves rules belonging
// to other prefixes (so multiple inbounds can share a bucket).
//
// Days=1 is the AWS S3 minimum; active fedarisha sessions live seconds-to-
// minutes (server consumes and deletes), so they never reach the threshold —
// the rule only sweeps orphans (crashed clients, unconsumed multi-user dirs).
func (s *S3Store) SetupLifecycle(ctx context.Context, prefix string) error {
	if prefix == "" {
		return fmt.Errorf("lifecycle: prefix is required")
	}

	ruleID := lifecycleRuleIDForPrefix(prefix)

	existing, err := s.fetchLifecycleRules(ctx)
	if err != nil {
		return err
	}

	rules := make([]s3types.LifecycleRule, 0, len(existing)+1)
	// VK Cloud rejects AbortIncompleteMultipartUpload with "InvalidArgument:
	// This argument is unsupported at the time" — keep the rule to just the
	// fields VK Cloud accepts (Filter.Prefix + Expiration.Days).
	rules = append(rules, s3types.LifecycleRule{
		ID:     aws.String(ruleID),
		Status: s3types.ExpirationStatusEnabled,
		Filter: &s3types.LifecycleRuleFilter{
			Prefix: aws.String(prefix),
		},
		Expiration: &s3types.LifecycleExpiration{
			Days: aws.Int32(lifecycleExpireDays),
		},
	})
	for _, r := range existing {
		if aws.ToString(r.ID) == ruleID {
			continue
		}
		rules = append(rules, r)
	}

	_, err = s.readClient.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(s.cfg.Bucket),
		LifecycleConfiguration: &s3types.BucketLifecycleConfiguration{
			Rules: rules,
		},
	})
	if err != nil {
		return fmt.Errorf("put bucket lifecycle: %w", err)
	}
	return nil
}

func (s *S3Store) fetchLifecycleRules(ctx context.Context) ([]s3types.LifecycleRule, error) {
	out, err := s.readClient.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(s.cfg.Bucket),
	})
	if err == nil {
		return out.Rules, nil
	}
	// "no rules yet" is the success case for the first run on a fresh bucket.
	// Check via the smithy-go APIError interface so we don't pin the import.
	var apiErr interface{ ErrorCode() string }
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchLifecycleConfiguration" {
		return nil, nil
	}
	return nil, fmt.Errorf("get bucket lifecycle: %w", err)
}

// lifecycleRuleIDForPrefix derives a rule ID from the inbound's S3 prefix so
// that multiple inbounds sharing a bucket get separate, addressable rules.
// VK Cloud accepts alphanumerics plus -_. — we replace path separators with
// dashes and collapse the trailing slash.
func lifecycleRuleIDForPrefix(prefix string) string {
	cleaned := strings.Trim(prefix, "/")
	if cleaned == "" {
		return lifecycleRuleIDPrefix + "all"
	}
	return lifecycleRuleIDPrefix + strings.ReplaceAll(cleaned, "/", "-")
}
