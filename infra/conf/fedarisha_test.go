package conf_test

import (
	"testing"

	. "github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy/fedarisha"
)

func TestFedarishaClientConfig(t *testing.T) {
	creator := func() Buildable {
		return new(FedarishaClientConfig)
	}

	runMultiTestCase(t, []TestCase{
		{
			Input: `{
				"storage": {
					"type": "s3",
					"bucket": "bucket-a",
					"endpoint": "https://s3.example.com",
					"region": "ru-1",
					"prefix": "projects/user-a/",
					"accessKey": "key",
					"secretKey": "secret"
				},
				"tuning": {
					"pollIntervalMs": 250,
					"writeIntervalMs": 100,
					"idleTimeoutSec": 30,
					"maxFileSizeBytes": 65536
				},
				"userLevel": 1
			}`,
			Parser: loadJSON(creator),
			Output: &fedarisha.ClientConfig{
				Storage: &fedarisha.StorageConfig{
					Type:      "s3",
					Bucket:    "bucket-a",
					Endpoint:  "https://s3.example.com",
					Region:    "ru-1",
					Prefix:    "projects/user-a/",
					AccessKey: "key",
					SecretKey: "secret",
				},
				Tuning: &fedarisha.TuningConfig{
					PollIntervalMs:   250,
					WriteIntervalMs:  100,
					IdleTimeoutSec:   30,
					MaxFileSizeBytes: 65536,
				},
				UserLevel: 1,
			},
		},
	})
}

func TestFedarishaServerConfig(t *testing.T) {
	creator := func() Buildable {
		return new(FedarishaServerConfig)
	}

	runMultiTestCase(t, []TestCase{
		{
			Input: `{
				"storage": {
					"type": "local",
					"localDir": "/tmp/fedarisha",
					"sessionsDir": "sessions"
				},
				"tuning": {
					"pollIntervalMs": 250,
					"writeIntervalMs": 100,
					"idleTimeoutSec": 30,
					"maxFileSizeBytes": 65536
				},
				"webhook": {
					"enabled": true,
					"listen": ":8080",
					"publicUrl": "https://node.example.com/webhook",
					"autoSetup": true,
					"tlsCert": "/etc/certs/webhook.crt",
					"tlsKey": "/etc/certs/webhook.key"
				},
				"userLevel": 1,
				"clients": [
					{
						"id": "user-a",
						"email": "user-a@example.com",
						"level": 2
					}
				]
			}`,
			Parser: loadJSON(creator),
			Output: &fedarisha.ServerConfig{
				Storage: &fedarisha.StorageConfig{
					Type:        "local",
					LocalDir:    "/tmp/fedarisha",
					SessionsDir: "sessions",
				},
				Tuning: &fedarisha.TuningConfig{
					PollIntervalMs:   250,
					WriteIntervalMs:  100,
					IdleTimeoutSec:   30,
					MaxFileSizeBytes: 65536,
				},
				Webhook: &fedarisha.WebhookConfig{
					Enabled:   true,
					Listen:    ":8080",
					PublicUrl: "https://node.example.com/webhook",
					AutoSetup: true,
					TlsCert:   "/etc/certs/webhook.crt",
					TlsKey:    "/etc/certs/webhook.key",
				},
				UserLevel: 1,
				Clients: []*fedarisha.User{
					{
						Id:    "user-a",
						Email: "user-a@example.com",
						Level: 2,
					},
				},
			},
		},
	})
}
