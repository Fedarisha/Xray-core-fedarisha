package fedarisha

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	fedstorage "github.com/xtls/xray-core/proxy/fedarisha/storage"
	fedtransport "github.com/xtls/xray-core/proxy/fedarisha/transport"
)

var sharedWebhooks = struct {
	sync.Mutex
	endpoints map[string]*webhookEndpoint
}{
	endpoints: make(map[string]*webhookEndpoint),
}

type webhookEndpoint struct {
	listen  string
	tlsCert string
	tlsKey  string
	server  *http.Server
	mux     *http.ServeMux
	groups  map[string]*webhookGroup
	refs    int
}

type webhookGroup struct {
	publicURL string
	hubs      map[*fedtransport.WebhookHub]registeredWebhook
}

type registeredWebhook struct {
	bucket string
	prefix string
}

type webhookMessageType struct {
	Type string `json:"Type"`
}

type webhookSetupper interface {
	SetupWebhook(context.Context, string, string) error
}

func registerWebhook(ctx context.Context, tag string, storageConfig *StorageConfig, store fedstorage.Storage, cfg *WebhookConfig) (*fedtransport.WebhookHub, func(), error) {
	if cfg == nil || !cfg.GetEnabled() {
		return nil, nil, nil
	}

	if cfg.GetListen() == "" {
		return nil, nil, fmt.Errorf("webhook listen is required")
	}
	if (cfg.GetTlsCert() == "") != (cfg.GetTlsKey() == "") {
		return nil, nil, fmt.Errorf("webhook tlsCert and tlsKey must be set together")
	}
	if cfg.GetPublicUrl() == "" {
		return nil, nil, fmt.Errorf("webhook publicUrl is required")
	}
	if !isS3Storage(storageConfig) {
		return nil, nil, fmt.Errorf("webhook requires s3 storage")
	}

	path, err := webhookPath(cfg.GetPublicUrl())
	if err != nil {
		return nil, nil, err
	}

	prefix := normalizeS3Prefix(storageConfig.GetPrefix())
	hub := fedtransport.NewWebhookHub(prefix, cfg.GetPublicUrl())

	sharedWebhooks.Lock()
	endpoint := sharedWebhooks.endpoints[cfg.GetListen()]
	if endpoint == nil {
		endpoint, err = newWebhookEndpoint(cfg.GetListen(), cfg.GetTlsCert(), cfg.GetTlsKey())
		if err != nil {
			sharedWebhooks.Unlock()
			return nil, nil, err
		}
		sharedWebhooks.endpoints[cfg.GetListen()] = endpoint
	} else if endpoint.tlsCert != cfg.GetTlsCert() || endpoint.tlsKey != cfg.GetTlsKey() {
		sharedWebhooks.Unlock()
		return nil, nil, fmt.Errorf("webhook listen %s already uses different TLS settings", cfg.GetListen())
	}

	group := endpoint.groups[path]
	if group == nil {
		group = &webhookGroup{
			publicURL: cfg.GetPublicUrl(),
			hubs:      make(map[*fedtransport.WebhookHub]registeredWebhook),
		}
		endpoint.groups[path] = group
		endpoint.mux.Handle(path, group)
	} else if group.publicURL != cfg.GetPublicUrl() {
		sharedWebhooks.Unlock()
		return nil, nil, fmt.Errorf("webhook path %q on %s already uses publicUrl %q", path, cfg.GetListen(), group.publicURL)
	}

	if err := group.validatePrefix(prefix); err != nil {
		sharedWebhooks.Unlock()
		return nil, nil, err
	}

	group.hubs[hub] = registeredWebhook{
		bucket: storageConfig.GetBucket(),
		prefix: prefix,
	}
	endpoint.refs++
	setupPrefix := group.setupPrefixForBucket(storageConfig.GetBucket())
	sharedWebhooks.Unlock()

	if cfg.GetAutoSetup() {
		setupper, ok := store.(webhookSetupper)
		if !ok {
			return nil, nil, fmt.Errorf("webhook autoSetup requires s3 storage")
		}
		log.Printf("[fedarisha] inbound %q: configuring S3 webhook %s (prefix: %s)", tag, cfg.GetPublicUrl(), setupPrefix)
		if err := setupper.SetupWebhook(ctx, cfg.GetPublicUrl(), setupPrefix); err != nil {
			log.Printf("[fedarisha] inbound %q: WARNING webhook setup failed: %v", tag, err)
		}
	}

	log.Printf("[fedarisha] inbound %q: webhook enabled on %s%s", tag, cfg.GetListen(), path)
	return hub, func() {
		unregisterWebhook(cfg.GetListen(), path, hub)
	}, nil
}

func newWebhookEndpoint(listen, tlsCert, tlsKey string) (*webhookEndpoint, error) {
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:              listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	endpoint := &webhookEndpoint{
		listen:  listen,
		tlsCert: tlsCert,
		tlsKey:  tlsKey,
		server:  server,
		mux:     mux,
		groups:  make(map[string]*webhookGroup),
	}
	go func() {
		var err error
		if tlsCert != "" {
			err = server.ServeTLS(ln, tlsCert, tlsKey)
		} else {
			err = server.Serve(ln)
		}
		if err != nil && err != http.ErrServerClosed {
			log.Printf("[fedarisha] webhook server on %s failed: %v", listen, err)
		}
	}()
	return endpoint, nil
}

func isS3Storage(storageConfig *StorageConfig) bool {
	if storageConfig == nil {
		return false
	}
	storageType := strings.ToLower(storageConfig.GetType())
	switch storageType {
	case "s3":
		return true
	case "":
		return storageConfig.GetBucket() != ""
	}
	return false
}

func normalizeS3Prefix(prefix string) string {
	prefix = strings.TrimPrefix(prefix, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix
}

func unregisterWebhook(listen, path string, hub *fedtransport.WebhookHub) {
	sharedWebhooks.Lock()
	defer sharedWebhooks.Unlock()

	endpoint := sharedWebhooks.endpoints[listen]
	if endpoint == nil {
		return
	}
	if group := endpoint.groups[path]; group != nil {
		delete(group.hubs, hub)
	}

	endpoint.refs--
	if endpoint.refs > 0 {
		return
	}

	delete(sharedWebhooks.endpoints, listen)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := endpoint.server.Shutdown(ctx); err != nil {
		log.Printf("[fedarisha] webhook shutdown on %s failed: %v", listen, err)
	}
}

func webhookPath(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("webhook publicUrl: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("webhook publicUrl must be absolute")
	}
	if parsed.Path == "" {
		return "/webhook", nil
	}
	if !strings.HasPrefix(parsed.Path, "/") {
		return "/" + parsed.Path, nil
	}
	return parsed.Path, nil
}

func (g *webhookGroup) validatePrefix(prefix string) error {
	for _, existing := range g.hubs {
		if prefix == existing.prefix || strings.HasPrefix(prefix, existing.prefix) || strings.HasPrefix(existing.prefix, prefix) {
			return fmt.Errorf("webhook storage prefix %q overlaps with existing prefix %q on %s", prefix, existing.prefix, g.publicURL)
		}
	}
	return nil
}

func (g *webhookGroup) setupPrefixForBucket(bucket string) string {
	var prefixes []string
	for _, registered := range g.hubs {
		if registered.bucket == bucket {
			prefixes = append(prefixes, registered.prefix)
		}
	}
	return commonS3Prefix(prefixes)
}

func commonS3Prefix(prefixes []string) string {
	if len(prefixes) == 0 {
		return ""
	}
	prefix := prefixes[0]
	for _, next := range prefixes[1:] {
		for !strings.HasPrefix(next, prefix) {
			if prefix == "" {
				return ""
			}
			prefix = prefix[:len(prefix)-1]
		}
	}
	if prefix == "" || strings.HasSuffix(prefix, "/") {
		return prefix
	}
	idx := strings.LastIndex(prefix, "/")
	if idx < 0 {
		return ""
	}
	return prefix[:idx+1]
}

func (g *webhookGroup) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	var msgType webhookMessageType
	_ = json.Unmarshal(body, &msgType)

	hubs := g.snapshotHubs()
	if len(hubs) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}

	if msgType.Type == "SubscriptionConfirmation" {
		hubs[0].ServeHTTP(w, cloneWebhookRequest(r, body))
		return
	}

	for _, hub := range hubs {
		hub.ServeHTTP(discardResponseWriter{}, cloneWebhookRequest(r, body))
	}
	w.WriteHeader(http.StatusOK)
}

func (g *webhookGroup) snapshotHubs() []*fedtransport.WebhookHub {
	sharedWebhooks.Lock()
	defer sharedWebhooks.Unlock()

	hubs := make([]*fedtransport.WebhookHub, 0, len(g.hubs))
	for hub := range g.hubs {
		hubs = append(hubs, hub)
	}
	return hubs
}

func cloneWebhookRequest(r *http.Request, body []byte) *http.Request {
	clone := r.Clone(r.Context())
	clone.Body = io.NopCloser(bytes.NewReader(body))
	clone.URL.Path = "/webhook"
	return clone
}

type discardResponseWriter struct{}

func (discardResponseWriter) Header() http.Header {
	return http.Header{}
}

func (discardResponseWriter) Write(data []byte) (int, error) {
	return len(data), nil
}

func (discardResponseWriter) WriteHeader(int) {}
