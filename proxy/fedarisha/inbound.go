package fedarisha

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	stdnet "net"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
	fedtransport "github.com/xtls/xray-core/proxy/fedarisha/transport"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	ctxpkg "github.com/xtls/xray-core/common/ctx"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	xraynet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func init() {
	common.Must(common.RegisterConfig((*ServerConfig)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		server := &Server{config: config.(*ServerConfig)}
		err := core.RequireFeatures(ctx, func(pm policy.Manager, dispatcher routing.Dispatcher) error {
			return server.Init(ctx, pm, dispatcher)
		})
		return server, err
	}))
}

type Server struct {
	ctx             context.Context
	cancel          context.CancelFunc
	config          *ServerConfig
	dispatcher      routing.Dispatcher
	listener        *fedtransport.Listener
	closeWebhook    func()
	tag             string
	sniffingRequest session.SniffingRequest

	usersMu sync.RWMutex
	users   map[string]*protocol.MemoryUser

	// activeSessions tracks currently-open yamux sessions keyed by user prefix
	// so RemoveUser can tear them down immediately. Without this, a revoked
	// user keeps proxying traffic over their existing yamux session until it
	// idle-times-out — the listener gate only blocks NEW handshakes.
	sessionsMu     sync.Mutex
	activeSessions map[string]map[*yamux.Session]struct{}
}

func (s *Server) Init(ctx context.Context, _ policy.Manager, dispatcher routing.Dispatcher) error {
	s.dispatcher = dispatcher
	s.users = buildInboundUsers(s.config.GetClients(), s.config.GetUserLevel())
	s.activeSessions = make(map[string]map[*yamux.Session]struct{})

	if inbound := session.InboundFromContext(ctx); inbound != nil {
		s.tag = inbound.Tag
	}
	if content := session.ContentFromContext(ctx); content != nil {
		s.sniffingRequest = content.SniffingRequest
	}

	baseCtx := core.ToBackgroundDetachedContext(ctx)
	s.ctx, s.cancel = context.WithCancel(baseCtx)

	store, err := buildStorage(s.ctx, s.config.GetStorage())
	if err != nil {
		return errors.New("fedarisha: failed to configure listener storage").Base(err)
	}

	opts := fedtransport.ListenOpts{InboundTag: s.tag}
	hub, closeWebhook, err := registerWebhook(s.ctx, s.tag, s.config.GetStorage(), store, s.config.GetWebhook())
	if err != nil {
		return errors.New("fedarisha: failed to configure webhook").Base(err)
	}
	if hub != nil {
		opts.WebhookHub = hub
		s.closeWebhook = closeWebhook
	}

	registerLifecycle(s.ctx, s.tag, s.config.GetStorage(), store)

	s.listener, err = fedtransport.ListenMultiUser(s.ctx, store, s.config.GetStorage().GetSessionsDir(), opts)
	if err != nil {
		if s.closeWebhook != nil {
			s.closeWebhook()
			s.closeWebhook = nil
		}
		return errors.New("fedarisha: failed to start listener").Base(err)
	}
	s.listener.IsUserAllowed = s.isUserAllowed
	applyListenerTuning(s.listener, s.config.GetTuning())

	go s.acceptLoop()
	return nil
}

func buildInboundUsers(clients []*User, defaultLevel uint32) map[string]*protocol.MemoryUser {
	users := make(map[string]*protocol.MemoryUser, len(clients))
	for _, client := range clients {
		id := client.GetId()
		if id == "" {
			continue
		}
		email := client.GetEmail()
		if email == "" {
			email = id
		}
		level := client.GetLevel()
		if level == 0 {
			level = defaultLevel
		}
		users[id] = &protocol.MemoryUser{
			Email: email,
			Level: level,
		}
	}
	return users
}

func applyListenerTuning(listener *fedtransport.Listener, tuning *TuningConfig) {
	if listener == nil || tuning == nil {
		return
	}
	if v := tuning.GetPollIntervalMs(); v > 0 {
		listener.PollInterval = time.Duration(v) * time.Millisecond
	}
	if v := tuning.GetWriteIntervalMs(); v > 0 {
		listener.WriteInterval = time.Duration(v) * time.Millisecond
	}
	if v := tuning.GetIdleTimeoutSec(); v > 0 {
		listener.IdleTimeout = time.Duration(v) * time.Second
	}
	if v := tuning.GetMaxFileSizeBytes(); v > 0 {
		listener.MaxFileSize = int(v)
	}
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				errors.LogWarningInner(s.ctx, err, "fedarisha: accept failed")
				continue
			}
		}
		go s.handleMuxSession(conn)
	}
}

func (s *Server) handleMuxSession(conn stdnet.Conn) {
	defer conn.Close()

	userID := ""
	if fedConn, ok := conn.(*fedtransport.Conn); ok {
		userID = fedConn.UserPrefix()
	}

	muxSession, err := yamux.Server(conn, yamuxSessionConfig())
	if err != nil {
		errors.LogWarningInner(s.ctx, err, "fedarisha: failed to open yamux server")
		return
	}
	defer muxSession.Close()

	s.registerSession(userID, muxSession)
	defer s.unregisterSession(userID, muxSession)

	for {
		stream, err := muxSession.AcceptStream()
		if err != nil {
			return
		}
		go s.handleStream(stream, userID)
	}
}

func (s *Server) registerSession(userID string, sess *yamux.Session) {
	if userID == "" || sess == nil {
		return
	}
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	bucket, ok := s.activeSessions[userID]
	if !ok {
		bucket = make(map[*yamux.Session]struct{})
		s.activeSessions[userID] = bucket
	}
	bucket[sess] = struct{}{}
}

func (s *Server) unregisterSession(userID string, sess *yamux.Session) {
	if userID == "" || sess == nil {
		return
	}
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	bucket, ok := s.activeSessions[userID]
	if !ok {
		return
	}
	delete(bucket, sess)
	if len(bucket) == 0 {
		delete(s.activeSessions, userID)
	}
}

func (s *Server) closeSessionsFor(userID string) []*yamux.Session {
	s.sessionsMu.Lock()
	bucket, ok := s.activeSessions[userID]
	if !ok {
		s.sessionsMu.Unlock()
		return nil
	}
	delete(s.activeSessions, userID)
	out := make([]*yamux.Session, 0, len(bucket))
	for sess := range bucket {
		out = append(out, sess)
	}
	s.sessionsMu.Unlock()
	for _, sess := range out {
		_ = sess.Close()
	}
	return out
}

// isUserAllowed is the layer-2 gate handed to the listener. It mirrors the
// xray UserManager state — anything in s.users is entitled, anything else
// gets refused before key exchange even runs.
func (s *Server) isUserAllowed(userPrefix string) bool {
	if userPrefix == "" {
		return false
	}
	s.usersMu.RLock()
	defer s.usersMu.RUnlock()
	_, ok := s.users[userPrefix]
	return ok
}

func (s *Server) handleStream(stream *yamux.Stream, userID string) {
	defer stream.Close()

	destination, err := readTargetHeader(stream)
	if err != nil {
		errors.LogWarningInner(s.ctx, err, "fedarisha: failed to read target header")
		return
	}

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	ctx = ctxpkg.ContextWithID(ctx, session.NewID())

	sourceLabel := "fedarisha"
	if userID != "" {
		sourceLabel = "fedarisha." + userID
	}
	source := xraynet.TCPDestination(xraynet.DomainAddress(sourceLabel), 0)
	inbound := session.Inbound{
		Name:          "fedarisha",
		Tag:           s.tag,
		CanSpliceCopy: 1,
		Source:        source,
		User:          s.userFor(userID),
	}
	if inbound.User == nil {
		inbound.User = &protocol.MemoryUser{
			Email: userID,
			Level: s.config.GetUserLevel(),
		}
	}

	ctx = session.ContextWithInbound(ctx, &inbound)
	ctx = session.ContextWithContent(ctx, &session.Content{
		SniffingRequest: s.sniffingRequest,
	})
	ctx = session.SubContextFromMuxInbound(ctx)
	ctx = log.ContextWithAccessMessage(ctx, &log.AccessMessage{
		From:   inbound.Source,
		To:     destination,
		Status: log.AccessAccepted,
		Reason: "",
	})

	errors.LogInfo(ctx, "processing from ", source, " to ", destination)

	reader := buf.NewReader(stream)
	writer := buf.NewWriter(stream)
	if destination.Network == xraynet.Network_UDP {
		reader = newFedarishaPacketReader(stream, destination)
		writer = newFedarishaPacketWriter(writer, destination)
	}

	link := &transport.Link{
		Reader: reader,
		Writer: writer,
	}
	if err := s.dispatcher.DispatchLink(ctx, destination, link); err != nil {
		errors.LogInfoInner(ctx, err, "fedarisha: connection closed")
	}
}

func (s *Server) userFor(id string) *protocol.MemoryUser {
	if id == "" {
		return nil
	}
	s.usersMu.RLock()
	defer s.usersMu.RUnlock()
	return s.users[id]
}

// AddUser implements proxy.UserManager.AddUser. The panel calls this via the
// xray gRPC handler when a user becomes (re)entitled. For Fedarisha id and
// email are both the user's tId, so we key by email.
func (s *Server) AddUser(_ context.Context, u *protocol.MemoryUser) error {
	if u == nil || u.Email == "" {
		return errors.New("fedarisha: user requires non-empty email")
	}
	s.usersMu.Lock()
	defer s.usersMu.Unlock()
	if s.users == nil {
		s.users = make(map[string]*protocol.MemoryUser)
	}
	s.users[u.Email] = u
	return nil
}

// RemoveUser implements proxy.UserManager.RemoveUser. After unregistering the
// user from the entitlement map we close every yamux session they currently
// hold so an active client can't keep proxying through a still-valid mux —
// the listener gate would only stop their NEXT handshake. Sessions are closed
// outside the users lock to avoid blocking subsequent UserManager calls
// while net.Conn.Close drains.
func (s *Server) RemoveUser(_ context.Context, email string) error {
	if email == "" {
		return errors.New("fedarisha: empty email")
	}
	s.usersMu.Lock()
	if _, ok := s.users[email]; !ok {
		s.usersMu.Unlock()
		return errors.New("fedarisha: user not found: " + email)
	}
	delete(s.users, email)
	s.usersMu.Unlock()

	s.closeSessionsFor(email)
	return nil
}

// GetUser implements proxy.UserManager.GetUser.
func (s *Server) GetUser(_ context.Context, email string) *protocol.MemoryUser {
	return s.userFor(email)
}

// GetUsers implements proxy.UserManager.GetUsers.
func (s *Server) GetUsers(_ context.Context) []*protocol.MemoryUser {
	s.usersMu.RLock()
	defer s.usersMu.RUnlock()
	users := make([]*protocol.MemoryUser, 0, len(s.users))
	for _, u := range s.users {
		users = append(users, u)
	}
	return users
}

// GetUsersCount implements proxy.UserManager.GetUsersCount.
func (s *Server) GetUsersCount(_ context.Context) int64 {
	s.usersMu.RLock()
	defer s.usersMu.RUnlock()
	return int64(len(s.users))
}

func readTargetHeader(r io.Reader) (xraynet.Destination, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return xraynet.Destination{}, err
	}
	rawHostLen := binary.BigEndian.Uint16(lenBuf[:])
	network := xraynet.Network_TCP
	if rawHostLen&targetHeaderUDPFlag != 0 {
		network = xraynet.Network_UDP
		rawHostLen &^= targetHeaderUDPFlag
	}
	hostLen := rawHostLen
	if hostLen == 0 || hostLen > 512 {
		return xraynet.Destination{}, fmt.Errorf("invalid target host length %d", hostLen)
	}

	host := make([]byte, hostLen)
	if _, err := io.ReadFull(r, host); err != nil {
		return xraynet.Destination{}, err
	}

	var portBuf [2]byte
	if _, err := io.ReadFull(r, portBuf[:]); err != nil {
		return xraynet.Destination{}, err
	}
	address := xraynet.ParseAddress(string(host))
	port := xraynet.Port(binary.BigEndian.Uint16(portBuf[:]))
	if network == xraynet.Network_UDP {
		return xraynet.UDPDestination(address, port), nil
	}
	return xraynet.TCPDestination(address, port), nil
}

func (s *Server) Network() []xraynet.Network {
	return []xraynet.Network{}
}

func (s *Server) Process(context.Context, xraynet.Network, stat.Connection, routing.Dispatcher) error {
	return nil
}

func (s *Server) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.closeWebhook != nil {
		s.closeWebhook()
		s.closeWebhook = nil
	}
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}
