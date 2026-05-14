package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// SetupWebhook configures S3 bucket notification to send ObjectCreated events
// to the given webhook URL. This uses the VK Cloud S3 SimpleTopicConfiguration
// extension (not standard AWS SNS/SQS).
//
// prefix filters events to keys starting with the given prefix (e.g. "projects/sessions/").
// webhookURL is the public HTTP endpoint that will receive POST notifications.
func (s *S3Store) SetupWebhook(ctx context.Context, webhookURL, prefix string) error {
	xmlBody := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <SimpleTopicConfiguration>
      <Id>fedarisha</Id>
      <Event>s3:ObjectCreated:Put</Event>
      <Url>%s</Url>
      <Filter>
         <S3Key>
            <FilterRule>
               <Name>Prefix</Name>
               <Value>%s</Value>
            </FilterRule>
         </S3Key>
      </Filter>
   </SimpleTopicConfiguration>
</NotificationConfiguration>`, webhookURL, prefix)

	// VK Cloud S3 uses virtual-hosted style: {bucket}.{host}/?notification
	// Standard AWS also uses this by default.
	endpoint := s.cfg.Endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", s.cfg.Region)
	}
	// Extract host from endpoint to build virtual-hosted URL.
	host := strings.TrimPrefix(endpoint, "https://")
	host = strings.TrimPrefix(host, "http://")
	scheme := "https://"
	if strings.HasPrefix(endpoint, "http://") {
		scheme = "http://"
	}
	url := fmt.Sprintf("%s%s.%s/?notification", scheme, s.cfg.Bucket, host)

	body := []byte(xmlBody)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/xml")

	// Sign the request with AWS Signature V4.
	// VK Cloud S3 requires x-amz-content-sha256 as an explicit header.
	hash := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(hash[:])
	req.Header.Set("x-amz-content-sha256", payloadHash)

	creds, err := credentials.NewStaticCredentialsProvider(
		s.cfg.AccessKey, s.cfg.SecretKey, "",
	).Retrieve(ctx)
	if err != nil {
		return fmt.Errorf("retrieve credentials: %w", err)
	}

	signer := v4.NewSigner()
	if err := signer.SignHTTP(ctx, creds, req, payloadHash, "s3", s.cfg.Region, time.Now()); err != nil {
		return fmt.Errorf("sign request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("S3 notification config failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}
