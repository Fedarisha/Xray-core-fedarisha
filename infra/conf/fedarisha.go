package conf

import (
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/proxy/fedarisha"
	"google.golang.org/protobuf/proto"
)

type FedarishaStorageConfig struct {
	Type        string `json:"type"`
	Bucket      string `json:"bucket"`
	Endpoint    string `json:"endpoint"`
	Region      string `json:"region"`
	Prefix      string `json:"prefix"`
	AccessKey   string `json:"accessKey"`
	SecretKey   string `json:"secretKey"`
	LocalDir    string `json:"localDir"`
	SessionsDir string `json:"sessionsDir"`
}

func (c *FedarishaStorageConfig) Build() *fedarisha.StorageConfig {
	if c == nil {
		return nil
	}
	return &fedarisha.StorageConfig{
		Type:        c.Type,
		Bucket:      c.Bucket,
		Endpoint:    c.Endpoint,
		Region:      c.Region,
		Prefix:      c.Prefix,
		AccessKey:   c.AccessKey,
		SecretKey:   c.SecretKey,
		LocalDir:    c.LocalDir,
		SessionsDir: c.SessionsDir,
	}
}

type FedarishaTuningConfig struct {
	PollIntervalMs   uint32 `json:"pollIntervalMs"`
	WriteIntervalMs  uint32 `json:"writeIntervalMs"`
	IdleTimeoutSec   uint32 `json:"idleTimeoutSec"`
	MaxFileSizeBytes uint32 `json:"maxFileSizeBytes"`
}

func (c *FedarishaTuningConfig) Build() *fedarisha.TuningConfig {
	if c == nil {
		return nil
	}
	return &fedarisha.TuningConfig{
		PollIntervalMs:   c.PollIntervalMs,
		WriteIntervalMs:  c.WriteIntervalMs,
		IdleTimeoutSec:   c.IdleTimeoutSec,
		MaxFileSizeBytes: c.MaxFileSizeBytes,
	}
}

type FedarishaWebhookConfig struct {
	Enabled   bool   `json:"enabled"`
	Listen    string `json:"listen"`
	PublicURL string `json:"publicUrl"`
	AutoSetup *bool  `json:"autoSetup"`
	TLSCert   string `json:"tlsCert"`
	TLSKey    string `json:"tlsKey"`
}

func (c *FedarishaWebhookConfig) Build() *fedarisha.WebhookConfig {
	if c == nil {
		return nil
	}
	autoSetup := true
	if c.AutoSetup != nil {
		autoSetup = *c.AutoSetup
	}
	return &fedarisha.WebhookConfig{
		Enabled:   c.Enabled,
		Listen:    c.Listen,
		PublicUrl: c.PublicURL,
		AutoSetup: autoSetup,
		TlsCert:   c.TLSCert,
		TlsKey:    c.TLSKey,
	}
}

type FedarishaClientConfig struct {
	Storage   *FedarishaStorageConfig `json:"storage"`
	Tuning    *FedarishaTuningConfig  `json:"tuning"`
	UserLevel uint32                  `json:"userLevel"`
}

func (c *FedarishaClientConfig) Build() (proto.Message, error) {
	config := &fedarisha.ClientConfig{
		Storage:   c.Storage.Build(),
		Tuning:    c.Tuning.Build(),
		UserLevel: c.UserLevel,
	}

	if config.Storage == nil {
		return nil, errors.New("fedarisha settings must contain storage")
	}
	return config, nil
}

type FedarishaUserConfig struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Level uint32 `json:"level"`
}

func (c *FedarishaUserConfig) Build() *fedarisha.User {
	if c == nil {
		return nil
	}
	return &fedarisha.User{
		Id:    c.ID,
		Email: c.Email,
		Level: c.Level,
	}
}

type FedarishaServerConfig struct {
	Storage   *FedarishaStorageConfig `json:"storage"`
	Tuning    *FedarishaTuningConfig  `json:"tuning"`
	Clients   []*FedarishaUserConfig  `json:"clients"`
	UserLevel uint32                  `json:"userLevel"`
	Webhook   *FedarishaWebhookConfig `json:"webhook"`
}

func (c *FedarishaServerConfig) Build() (proto.Message, error) {
	config := &fedarisha.ServerConfig{
		Storage:   c.Storage.Build(),
		Tuning:    c.Tuning.Build(),
		UserLevel: c.UserLevel,
		Webhook:   c.Webhook.Build(),
	}
	for _, client := range c.Clients {
		if built := client.Build(); built != nil {
			config.Clients = append(config.Clients, built)
		}
	}

	if config.Storage == nil {
		return nil, errors.New("fedarisha server settings must contain storage")
	}
	return config, nil
}
