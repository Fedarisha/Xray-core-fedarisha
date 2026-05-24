package fedarisha

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	stdnet "net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
	fedstorage "github.com/xtls/xray-core/proxy/fedarisha/storage"
	fedlocal "github.com/xtls/xray-core/proxy/fedarisha/storage/local"
	feds3 "github.com/xtls/xray-core/proxy/fedarisha/storage/s3"
	fedtransport "github.com/xtls/xray-core/proxy/fedarisha/transport"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	xraynet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal"
	"github.com/xtls/xray-core/common/task"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
)

func init() {
	common.Must(common.RegisterConfig((*ClientConfig)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return NewClient(ctx, config.(*ClientConfig))
	}))
}

type Client struct {
	ctx           context.Context
	policyManager policy.Manager
	dialer        *fedtransport.Dialer
	userLevel     uint32

	mu      sync.Mutex
	session *yamux.Session
}

func NewClient(ctx context.Context, config *ClientConfig) (*Client, error) {
	v := core.MustFromContext(ctx)

	storageConfig := config.GetStorage()
	if storageConfig == nil {
		return nil, errors.New("fedarisha: storage backend is not configured")
	}

	dialer, err := buildDialer(ctx, storageConfig, config.GetTuning())
	if err != nil {
		return nil, errors.New("fedarisha: failed to configure storage backend").Base(err)
	}

	return &Client{
		ctx:           ctx,
		policyManager: v.GetFeature(policy.ManagerType()).(policy.Manager),
		dialer:        dialer,
		userLevel:     config.GetUserLevel(),
	}, nil
}

func buildDialer(ctx context.Context, config *StorageConfig, tuning *TuningConfig) (*fedtransport.Dialer, error) {
	store, err := buildStorage(ctx, config)
	if err != nil {
		return nil, err
	}

	dialer := &fedtransport.Dialer{
		Store:       store,
		SessionsDir: config.GetSessionsDir(),
	}
	applyTuning(dialer, tuning)
	return dialer, nil
}

func buildStorage(ctx context.Context, config *StorageConfig) (fedstorage.Storage, error) {
	if config == nil {
		return nil, fmt.Errorf("storage config is empty")
	}

	storageType := strings.ToLower(config.GetType())
	if storageType == "" {
		switch {
		case config.GetBucket() != "":
			storageType = "s3"
		case config.GetLocalDir() != "":
			storageType = "local"
		}
	}

	// vkcloud-pak / selectel-iam / static name node-side PAK providers
	// — distinct credential factories, but on the wire they all speak
	// the same S3 protocol that this transport implements. Collapse them
	// to "s3" so the inbound that node provisioned this way boots.
	switch storageType {
	case "vkcloud-pak", "selectel-iam", "static":
		storageType = "s3"
	}

	switch storageType {
	case "s3":
		if config.GetBucket() == "" {
			return nil, fmt.Errorf("s3 bucket is empty")
		}
		store := feds3.New(feds3.Config{
			Bucket:    config.GetBucket(),
			Prefix:    config.GetPrefix(),
			Region:    config.GetRegion(),
			Endpoint:  config.GetEndpoint(),
			AccessKey: config.GetAccessKey(),
			SecretKey: config.GetSecretKey(),
		})
		if err := store.Init(ctx); err != nil {
			return nil, err
		}
		return store, nil
	case "local":
		if config.GetLocalDir() == "" {
			return nil, fmt.Errorf("localDir is empty")
		}
		store := fedlocal.New(fedlocal.Config{RootDir: config.GetLocalDir()})
		if err := store.Init(ctx); err != nil {
			return nil, err
		}
		return store, nil
	default:
		return nil, fmt.Errorf("unsupported storage type %q", storageType)
	}
}

func yamuxSessionConfig() *yamux.Config {
	muxCfg := yamux.DefaultConfig()
	muxCfg.LogOutput = io.Discard
	muxCfg.MaxStreamWindowSize = 16 * 1024 * 1024
	muxCfg.ConnectionWriteTimeout = 60 * time.Second
	muxCfg.KeepAliveInterval = 60 * time.Second
	muxCfg.StreamOpenTimeout = 60 * time.Second
	return muxCfg
}

func applyTuning(dialer *fedtransport.Dialer, tuning *TuningConfig) {
	if tuning == nil {
		return
	}
	if v := tuning.GetPollIntervalMs(); v > 0 {
		dialer.PollInterval = time.Duration(v) * time.Millisecond
	}
	if v := tuning.GetWriteIntervalMs(); v > 0 {
		dialer.WriteInterval = time.Duration(v) * time.Millisecond
	}
	if v := tuning.GetIdleTimeoutSec(); v > 0 {
		dialer.IdleTimeout = time.Duration(v) * time.Second
	}
	if v := tuning.GetMaxFileSizeBytes(); v > 0 {
		dialer.MaxFileSize = int(v)
	}
}

func (c *Client) getSession() (*yamux.Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.session != nil && !c.session.IsClosed() {
		return c.session, nil
	}

	xconn, err := c.dialer.Dial(c.ctx)
	if err != nil {
		return nil, fmt.Errorf("fedarisha dial: %w", err)
	}

	session, err := yamux.Client(xconn, yamuxSessionConfig())
	if err != nil {
		xconn.Close()
		return nil, fmt.Errorf("yamux client: %w", err)
	}

	c.session = session
	return session, nil
}

func (c *Client) openStream() (stdnet.Conn, error) {
	session, err := c.getSession()
	if err != nil {
		return nil, err
	}

	stream, err := session.Open()
	if err != nil {
		c.mu.Lock()
		if c.session == session {
			c.session.Close()
			c.session = nil
		}
		c.mu.Unlock()

		session, err = c.getSession()
		if err != nil {
			return nil, err
		}
		return session.Open()
	}
	return stream, nil
}

func (c *Client) Process(ctx context.Context, link *transport.Link, _ internet.Dialer) error {
	outbounds := session.OutboundsFromContext(ctx)
	ob := outbounds[len(outbounds)-1]
	if !ob.Target.IsValid() {
		return errors.New("target not specified")
	}
	ob.Name = "fedarisha"
	ob.CanSpliceCopy = 1

	destination := ob.Target
	if destination.Network != xraynet.Network_TCP && destination.Network != xraynet.Network_UDP {
		return errors.New("fedarisha supports TCP and UDP destinations only")
	}

	stream, err := c.openStream()
	if err != nil {
		return errors.New("fedarisha: failed to open stream").Base(err)
	}
	defer stream.Close()

	if err := writeTargetHeader(stream, destination); err != nil {
		return errors.New("fedarisha: failed to write target header").Base(err)
	}

	p := c.policyManager.ForLevel(c.userLevel)
	var newCtx context.Context
	var newCancel context.CancelFunc
	if session.TimeoutOnlyFromContext(ctx) {
		newCtx, newCancel = context.WithCancel(context.Background())
	}

	ctx, cancel := context.WithCancel(ctx)
	timer := signal.CancelAfterInactivity(ctx, func() {
		cancel()
		if newCancel != nil {
			newCancel()
		}
	}, p.Timeouts.ConnectionIdle)

	requestDone := func() error {
		defer timer.SetTimeout(p.Timeouts.DownlinkOnly)
		writer := buf.NewWriter(stream)
		if destination.Network == xraynet.Network_UDP {
			writer = newFedarishaPacketWriter(writer, destination)
		}
		return buf.Copy(link.Reader, writer, buf.UpdateActivity(timer))
	}
	responseDone := func() error {
		defer timer.SetTimeout(p.Timeouts.UplinkOnly)
		reader := buf.NewReader(stream)
		if destination.Network == xraynet.Network_UDP {
			reader = newFedarishaPacketReader(stream, destination)
		}
		return buf.Copy(reader, link.Writer, buf.UpdateActivity(timer))
	}

	if newCtx != nil {
		ctx = newCtx
	}

	if err := task.Run(ctx, requestDone, task.OnSuccess(responseDone, task.Close(link.Writer))); err != nil {
		return errors.New("fedarisha: connection ends").Base(err)
	}
	return nil
}

func writeTargetHeader(w io.Writer, destination xraynet.Destination) error {
	host := destination.Address.String()
	if host == "" {
		return fmt.Errorf("empty target host")
	}
	if len(host) > 32767 {
		return fmt.Errorf("target host is too long")
	}

	hostLen := uint16(len(host))
	if destination.Network == xraynet.Network_UDP {
		hostLen |= targetHeaderUDPFlag
	}

	var header bytes.Buffer
	if err := binary.Write(&header, binary.BigEndian, hostLen); err != nil {
		return err
	}
	if _, err := header.WriteString(host); err != nil {
		return err
	}
	if err := binary.Write(&header, binary.BigEndian, destination.Port.Value()); err != nil {
		return err
	}
	_, err := w.Write(header.Bytes())
	return err
}
