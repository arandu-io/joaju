package joaju

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/joaju/ws"
)

// ErrSocketClosed is what [Sink.Send] answers once the socket is gone.
//
// A client that cannot keep up gets it too, wrapped: the socket is closed
// first, because the alternative is a broadcast that blocks on the slowest
// subscriber and takes the channel down with it.
var ErrSocketClosed = errors.New("joaju: the socket is closed")

// The defaults [NewServer] fills in for a zero [ServerConfig] field.
const (
	// DefaultMaxMessageSize is the largest frame a client may send, and is the
	// Pusher protocol's own limit of 10 KiB. It is a limit on what a client
	// sends, not on what the server sends: a presence channel's member list is
	// larger than this and goes out fine.
	DefaultMaxMessageSize int64 = 10 << 10
	// DefaultMaxBodySize is the largest body an API route reads. A batch
	// publish is the big one.
	DefaultMaxBodySize int64 = 1 << 20
	// DefaultOutboundQueue is how many frames may be waiting on one socket
	// before the client is judged to have fallen behind.
	DefaultOutboundQueue = 64
	// DefaultMaxConnections is how many sockets one tenant may hold open.
	//
	// Zero in [ServerConfig] means this, and this is not "no limit": a server
	// that accepts sockets until the file descriptors run out stops answering
	// for EVERY tenant, and the one that exhausted them is not necessarily the
	// one that notices. The number here is a default that a deployment raises
	// knowingly.
	DefaultMaxConnections = 10_000
	// DefaultWriteTimeout is how long one frame may take to reach the client.
	DefaultWriteTimeout = 10 * time.Second
	// DefaultPingInterval is how often the server sends a WebSocket ping.
	DefaultPingInterval = 30 * time.Second
	// DefaultPongTimeout is how long the server waits to hear anything from a
	// client before it hangs up. It has to be longer than
	// [DefaultPingInterval], or the read deadline expires before the pong the
	// ping asked for can arrive.
	DefaultPongTimeout = 70 * time.Second
)

// Refusal is one of the three things a [Server] decides on its own, without any
// frame having been understood.
//
// Each of them is the transport's: how many sockets a tenant may hold, how fast
// one socket may send, and which opcode carries a message. A [Protocol] is
// never handed the frame that caused one and could not answer for it -- but the
// client still has to be told, and in something it can read. [Protocol.Refuse]
// is where these three become bytes, so that the wire format stays in one place
// and the server writes what it is given.
type Refusal uint8

const (
	// RefusalOverQuota is the tenant already holding as many sockets as
	// [ServerConfig.MaxConnections] allows. It is decided after the upgrade, so
	// the socket is writable when the answer goes out, and it closes after.
	RefusalOverQuota Refusal = iota
	// RefusalRateLimited is a frame dropped because the socket is sending
	// faster than [ServerConfig.MaxMessagesPerSecond] allows. The socket stays.
	RefusalRateLimited
	// RefusalUnreadable is a frame the transport delivered and this server
	// cannot act on, which is one that was not a text frame. The socket stays.
	RefusalUnreadable
)

// Protocol is the frame layer: what the [Server] hands a socket's traffic to.
//
// The server owns the socket -- the upgrade, the two goroutines, the deadlines,
// the registry -- and owns no part of the Pusher protocol. It builds no frame
// of its own, not even [EventConnectionEstablished], because a second place
// that builds a frame is a second answer to what a frame looks like. The three
// refusals that are the transport's own go out through [Protocol.Refuse], which
// is the implementation's bytes and not the server's.
//
// The HTTP routes are the protocol's too, and arrive through [Protocol.Routes].
// A wire protocol is what a client speaks over a socket AND what it calls over
// HTTP, and the server that owns neither owns neither of them: it holds the
// sockets, answers [Protocol.Routes] with an [API], and mounts what comes back.
//
// There is no method for reporting an error: the error comes back from the call
// that failed.
//
// The four socket methods are called from the one goroutine that reads a given
// socket, so calls concerning one [Connection] are ordered and never concurrent
// with each other. Calls concerning different connections are concurrent, and a
// route runs on the goroutine net/http gave it.
type Protocol interface {
	// Open is called once, after the handshake was authorized and the socket is
	// writable, and before any client frame is read. It is where
	// [EventConnectionEstablished] goes out.
	//
	// An error closes the socket.
	Open(ctx context.Context, conn *Connection) error

	// Message is called for each frame the client sends.
	//
	// An error does not close the socket: a client that asks for a channel it
	// may not have is told so with [EventError] and goes on using the ones it
	// has. The server logs it and reads the next frame.
	Message(ctx context.Context, conn *Connection, message []byte) error

	// Close is called once, after the socket is gone and the connection has
	// been dropped from the server's registry. It is where a channel's
	// subscribers are cleaned up and [EventMemberRemoved] goes out.
	//
	// It gets a context that is already cancelled if the process is shutting
	// down, so an implementation that has to reach Redis derives one with its
	// own deadline.
	Close(ctx context.Context, conn *Connection)

	// Refuse answers with the bytes that tell a client about one [Refusal],
	// ready to be written to its socket. An empty answer is written as nothing,
	// and the refusal takes its course in silence.
	//
	// It takes no [Connection] and no context because it decides nothing and
	// reaches nobody: the answer depends on the refusal alone, and the caller is
	// the one holding the socket it belongs to. An implementation is free to
	// encode the three once for the process and hand back the same slice every
	// time -- the caller writes it, and neither keeps nor modifies it.
	Refuse(r Refusal) []byte

	// Routes are the HTTP routes this protocol answers, mounted by the server
	// beside the socket route it keeps. A nil answer is a protocol that answers
	// none, and then the socket route is the whole of what the server serves.
	//
	// It is called once, from [NewServer], with everything a route may reach.
	// The socket route is not among them: an upgrade is the transport's, and
	// what comes back here is handed a request that will stay a request.
	Routes(api API) http.Handler
}

// Registry is the sockets a [Server] knows about, as an HTTP route may read
// them: what this process holds, what the other instances hold, and the one way
// to close what this process holds.
//
// Every question takes a Grant a [ConnectPolicy] or a [SubscriptionPolicy]
// issued, and the tenant on it is the only filter -- because it is the only one
// that did not arrive with the request. There is nothing here that opens a
// socket, nothing that closes one outside the Grant's tenant, and nothing that
// counts across tenants: a count that spans them tells one customer how many of
// another's people are online.
//
// [Server] is what implements it.
type Registry interface {
	// Connections is how many sockets this process holds for the Grant's
	// tenant.
	Connections(g auth.Grant) (int, error)

	// Terminate closes every socket the Grant's tenant holds for one subject,
	// and answers how many it closed.
	Terminate(ctx context.Context, g auth.Grant, subject string) (int, error)

	// Fleet is what the OTHER instances hold for the Grant's tenant. channel is
	// [ChannelName.Requested] of the one channel being asked about, and is
	// empty to ask about the whole tenant.
	Fleet(ctx context.Context, g auth.Grant, channel string) FleetTally
}

var _ Registry = (*Server)(nil)

// API is what a [Protocol] builds its HTTP routes out of, and is the whole of
// what the server lets one of those routes reach.
//
// Six of the seven fields are the [ServerConfig] the server was built from, so
// a route answers for the application the server answers for, runs the policies
// the server runs, and reads channels through the Broker the sockets read them
// through. There is no second place to say any of it. The seventh is
// [API.Registry].
//
// What is NOT here is the shape of it: nothing writes to the connection
// registry, and nothing reads one without a Grant. A [Protocol] that wanted to
// count another tenant's sockets would have to be handed a Grant of that
// tenant, and a [ConnectPolicy] is what issues one.
//
// A [Protocol] is handed one and does not build one.
type API struct {
	// AppID is the {appId} the routes carry. A request naming another app is
	// answered 404, and one server is one application.
	AppID string

	// Broker holds the channels, and there is no method on it that reaches one
	// without a Grant.
	Broker Broker

	// Connect decides whether a subject may act on this server at all. It is
	// the policy the socket route runs, asked the same question with no socket
	// on it -- an API caller wants no socket opened.
	Connect ConnectPolicy

	// Subscribe decides whether a subject may reach one channel. It runs once
	// per channel a route touches, listing and counting included: reading who
	// is talking to whom is a read, and there is no exception for reads.
	Subscribe SubscriptionPolicy

	// Registry is the sockets this process holds and what the other instances
	// answered about theirs. See [Registry].
	Registry Registry

	// MaxBodySize is the largest body a route may read.
	MaxBodySize int64

	// Log is where a refused request is recorded. It is never nil.
	Log *slog.Logger
}

// ServerConfig is what a [Server] is built from.
//
// Everything without a default is required, and [NewServer] refuses a config
// missing one rather than filling in something safe-looking: a server with no
// [ConnectPolicy] would accept every socket, and a nil policy is exactly the
// mistake this shape exists to make impossible.
type ServerConfig struct {
	// AppID is the {appId} the API routes carry, and AppKey is the {appKey} the
	// socket route carries. A request naming another app is answered 404.
	//
	// One server is one application: a Go binary is a process, and running one
	// per application costs nothing that would justify multiplexing them.
	// These are here so the names in a client's configuration mean what they
	// mean everywhere else.
	AppID  string
	AppKey string

	// Broker holds the channels. Every route that reads or writes channel state
	// goes through it, and there is no method on it that reaches a channel
	// without a Grant.
	Broker Broker

	// Connect decides whether a subject may be on this server at all. It runs
	// on the socket route and on every API route, before anything else.
	Connect ConnectPolicy

	// Subscribe decides whether a subject may reach one channel. It runs once
	// per channel touched, on every route that touches one.
	Subscribe SubscriptionPolicy

	// Protocol is the frame layer. See [Protocol].
	Protocol Protocol

	// Relay is the other instances, and a deployment of more than one needs it.
	// With one, an event published here reaches the sockets the other instances
	// hold, what they publish reaches the sockets held here, and the four
	// metrics routes answer for the whole fleet. Without one, each of those
	// three is answered by this process alone -- which is the true answer for a
	// deployment of one and the wrong answer for any other: two instances
	// serving one application each hold half the sockets, and neither can see
	// the other half.
	//
	// It is optional because a relay is a Redis, and a server that refused to
	// start without one would make the single-instance deployment the harder of
	// the two. See [Relay].
	Relay *Relay

	// MetricsTimeout is how long a metrics route waits for the other instances
	// before it answers with what has arrived. Zero means
	// [DefaultMetricsTimeout].
	//
	// It is a bound and not a promise: an instance that answers after it is
	// left out of that reply, because a dashboard served late by one instance
	// being replaced is a dashboard that is down. See [Relay.ask].
	MetricsTimeout time.Duration

	// Log is where a refusal and a dropped socket are recorded. nil means
	// slog.Default.
	Log *slog.Logger

	// MaxMessageSize, MaxBodySize, OutboundQueue, WriteTimeout, PingInterval
	// and PongTimeout are the socket's limits. Zero means the Default of the
	// same name.
	MaxMessageSize int64
	MaxBodySize    int64

	// MaxConnections is how many sockets ONE TENANT may hold. Zero means
	// [DefaultMaxConnections]; a negative number means no limit, and saying so
	// takes writing -1 rather than leaving a field out.
	//
	// Per tenant and not per server: a global limit lets one customer's
	// traffic refuse another customer's connections,
	// which is a denial of service one of them did not cause and cannot see.
	MaxConnections int

	// MaxMessagesPerSecond is how many frames ONE SOCKET may send in a second.
	// Zero, which is the default, is no limit.
	//
	// Per socket and not per tenant, which is the opposite of what
	// MaxConnections does and is the same reasoning taken one layer in: a socket
	// is the smallest thing a noisy client owns, so metering it spends nothing
	// of what the tenant's other sockets have. An allowance shared across a
	// tenant would let one runaway browser tab refuse the frames of every other
	// tab that customer has open, which is the denial of service the connection
	// limit exists to keep out.
	//
	// Zero is no limit rather than a Default of its own, because there is no
	// rate that is right for traffic this server has not seen: a chat client and
	// one that reports a cursor position differ by two orders of magnitude, and
	// a limit that refuses the frames of a correct client is worse than no limit
	// at all. It is turned on by a deployment that measured its own traffic.
	//
	// A frame past the limit is answered with [RefusalRateLimited] and dropped,
	// and the socket stays open. There is no second setting that closes it:
	// two ways to answer one refusal is two behaviours to explain, and
	// the client that a limit is aimed at is the one worth keeping addressable.
	MaxMessagesPerSecond int

	// Observer is told what happened, after it happened. Nil means
	// [NopObserver]. See [Observer] for why nothing there can refuse anything.
	Observer      Observer
	OutboundQueue int
	WriteTimeout  time.Duration
	PingInterval  time.Duration
	PongTimeout   time.Duration
}

// Server answers one route -- the socket -- and mounts whatever the [Protocol]
// answers over HTTP beside it.
//
// One and not nine, because the other eight are a wire format's and this owns
// no wire format. What it owns is the socket: the upgrade, the two goroutines,
// the deadlines, the registry and the limits. See [Protocol] for the other side
// of that line and [API] for what a route is given to cross it.
//
// It is an http.Handler and not a net/http.Server, so that it is mounted the
// way any other handler is -- behind hesape/auth's Authenticate middleware,
// which is where the auth.Subject on the request context comes from. That
// middleware is the only thing in the ecosystem that calls auth.WithSubject,
// and this server does not authenticate anybody: it reads the subject the
// middleware put there and asks a Policy about it.
//
// # The authorization shape, which the socket route follows
//
//	[ConnectPolicy]  once, before the upgrade  may this subject be here at all
//
// It runs before the upgrade, so a refusal is an HTTP status a browser reports
// rather than a socket that opens and shuts. What a subject may then reach is
// the [SubscriptionPolicy]'s answer, asked once per channel by whoever reaches
// one -- the [Protocol] on a frame, and its routes on a request.
type Server struct {
	appID  string
	appKey string

	broker    Broker
	connect   ConnectPolicy
	subscribe SubscriptionPolicy
	protocol  Protocol
	log       *slog.Logger
	// relay is the other instances, or nil for a deployment of one. It is what
	// [Server.Fleet] asks; the other direction arrives through [relayedBroker],
	// which is what broker is whenever this is not nil.
	relay *Relay

	upgrader ws.Upgrader
	mux      *http.ServeMux

	maxMessageSize int64
	maxBodySize    int64
	outboundQueue  int
	writeTimeout   time.Duration
	pingInterval   time.Duration
	pongTimeout    time.Duration
	maxConnections int
	// maxMessagesPerSecond is zero unless a deployment asked for a limit. It is
	// read once per socket, into the bucket the reading goroutine keeps.
	maxMessagesPerSecond int

	// observer is never nil. See [NopObserver] for why.
	observer Observer

	mu    sync.RWMutex
	conns map[SocketID]*Connection
	// perTenant is how many sockets each tenant holds, and it is what the
	// limit is checked against. Kept beside conns rather than counted from it
	// because counting a map of ten thousand on every upgrade is work that
	// grows with the thing it is protecting.
	perTenant map[string]int
}

// NewServer builds the server, or says which part of the config was missing.
func NewServer(cfg ServerConfig) (*Server, error) {
	switch {
	case cfg.AppID == "":
		return nil, errors.New("joaju: a server needs an app id, which is the {appId} of its API routes")
	case cfg.AppKey == "":
		return nil, errors.New("joaju: a server needs an app key, which is the {appKey} of its socket route")
	case cfg.Broker == nil:
		return nil, errors.New("joaju: a server needs a Broker, which is the only way to a channel")
	case cfg.Connect == nil:
		return nil, errors.New("joaju: a server needs a ConnectPolicy, and a server that accepts every socket is not the default")
	case cfg.Subscribe == nil:
		return nil, errors.New("joaju: a server needs a SubscriptionPolicy, because subscribing to a channel is a read")
	case cfg.Protocol == nil:
		return nil, errors.New("joaju: a server needs a Protocol to hand a socket's frames to")
	}

	// A Relay whose Broker does not carry it is half a fleet: this server would
	// relay what its own API publishes and never subscribe to what the others
	// publish, so a socket here would miss every event raised anywhere else.
	//
	// It cannot be fixed by wrapping here. The Protocol was built before the
	// server and holds the Broker it was handed; wrapping at this point would
	// leave that one raw, and the failure would be silent -- the worst of the
	// three outcomes. So it is refused, and the message carries the line that
	// fixes it. See [RelayedBroker].
	if cfg.Relay != nil {
		if _, ok := cfg.Broker.(relayedBroker); !ok {
			return nil, errors.New("joaju: a server with a Relay needs a Broker from RelayedBroker, and the same value has to reach the Protocol: broker := joaju.RelayedBroker(base, relay)")
		}
	}

	s := &Server{
		appID:          cfg.AppID,
		appKey:         cfg.AppKey,
		broker:         cfg.Broker,
		connect:        cfg.Connect,
		subscribe:      cfg.Subscribe,
		protocol:       cfg.Protocol,
		log:            cfg.Log,
		maxMessageSize: cfg.MaxMessageSize,
		maxBodySize:    cfg.MaxBodySize,
		outboundQueue:  cfg.OutboundQueue,
		writeTimeout:   cfg.WriteTimeout,
		pingInterval:   cfg.PingInterval,
		pongTimeout:    cfg.PongTimeout,
		maxConnections: cfg.MaxConnections,
		observer:       cfg.Observer,
		conns:          make(map[SocketID]*Connection),
		perTenant:      make(map[string]int),
	}
	if s.observer == nil {
		s.observer = NopObserver{}
	}
	// Zero is the whole answer for the rate limit -- it means off -- so unlike
	// every field below it there is no Default to fall back to.
	s.maxMessagesPerSecond = cfg.MaxMessagesPerSecond
	if s.maxConnections == 0 {
		s.maxConnections = DefaultMaxConnections
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	if s.maxMessageSize == 0 {
		s.maxMessageSize = DefaultMaxMessageSize
	}
	if s.maxBodySize == 0 {
		s.maxBodySize = DefaultMaxBodySize
	}
	if s.outboundQueue == 0 {
		s.outboundQueue = DefaultOutboundQueue
	}
	if s.writeTimeout == 0 {
		s.writeTimeout = DefaultWriteTimeout
	}
	if s.pingInterval == 0 {
		s.pingInterval = DefaultPingInterval
	}
	if s.pongTimeout == 0 {
		s.pongTimeout = DefaultPongTimeout
	}
	if s.pongTimeout <= s.pingInterval {
		return nil, fmt.Errorf("joaju: PongTimeout (%s) has to be longer than PingInterval (%s), or the read deadline expires before the pong the ping asked for can arrive", s.pongTimeout, s.pingInterval)
	}

	// CheckOrigin is left nil, and that is the decision. Nil is the transport's
	// same-origin check: an Origin header naming another host is refused with
	// 403 before the socket exists. A WebSocket that accepts any origin is CSRF
	// over a socket -- the browser attaches the cookies either way, and there is
	// no preflight to stop it.
	//
	// Nothing this server exposes sets that field, so there is no configuration
	// that widens it. A [ConnectPolicy] sees the Origin verbatim on the
	// [Handshake] and may refuse it; it cannot allow one the check refuses, so
	// the two can only narrow, never disagree. Cross-origin sockets are not
	// available in the first version, and when they are it will be this
	// assignment that changes, not a list in a config file.
	s.upgrader = ws.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	// One route is this server's, and it is the socket. Everything else a
	// deployment answers is the protocol's and is mounted under the pattern
	// that matches whatever the socket route did not.
	//
	// The socket route is registered first and is the more specific of the two,
	// so it is reached whatever a protocol mounts: an upgrade is the
	// transport's, and no wire format may take it over.
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("GET /app/{appKey}", s.handleSocket)
	if routes := cfg.Protocol.Routes(s.api()); routes != nil {
		s.mux.Handle("/", routes)
	}

	// Last, because it starts goroutines: the relay subscribes to the fleet's
	// questions from here on, and a server that failed to build after that
	// would leave them answering for a process nobody can reach.
	if cfg.Relay != nil {
		if err := cfg.Relay.serve(s, cfg.MetricsTimeout); err != nil {
			return nil, fmt.Errorf("joaju: this server cannot answer the fleet through its relay: %w", err)
		}
		s.relay = cfg.Relay
	}

	return s, nil
}

// api is what this server offers a [Protocol] to answer HTTP requests with, and
// is built once, after the defaults have been filled in -- so a route reads the
// body limit the sockets were given and not the zero somebody left in the
// config.
func (s *Server) api() API {
	return API{
		AppID:       s.appID,
		Broker:      s.broker,
		Connect:     s.connect,
		Subscribe:   s.subscribe,
		Registry:    s,
		MaxBodySize: s.maxBodySize,
		Log:         s.log,
	}
}

// ServeHTTP routes the socket route and whatever the [Protocol] brought.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Connections is how many sockets this process holds for the Grant's tenant.
//
// It takes a Grant for the reason [Broker.All] does: a count of who is
// connected is a read, and a count that spans tenants tells one customer how
// many of another's people are online. The Grant has to be one a
// [ConnectPolicy] issued -- auth.Grant.Check is what says so -- and the tenant
// it carries is the only filter, because it is the only one that did not come
// in with the request.
func (s *Server) Connections(g auth.Grant) (int, error) {
	tenant, err := registryTenant(g)
	if err != nil {
		return 0, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	n := 0
	for _, c := range s.conns {
		if c.Tenant() == tenant {
			n++
		}
	}

	return n, nil
}

// Terminate closes every socket the Grant's tenant holds for one subject, and
// answers how many it closed.
//
// It answers POST /apps/{appId}/users/{userId}/terminate_connections, and it
// is what a sign-out or a revoked membership calls: the socket was authorized
// once, at the handshake, and nothing about it expires on its own.
func (s *Server) Terminate(ctx context.Context, g auth.Grant, subject string) (int, error) {
	tenant, err := registryTenant(g)
	if err != nil {
		return 0, err
	}
	if subject == "" {
		return 0, errors.New("joaju: terminating connections needs a subject to terminate them for")
	}

	s.mu.RLock()
	doomed := make([]*Connection, 0, len(s.conns))
	for _, c := range s.conns {
		if c.Tenant() == tenant && c.Subject().ID == subject {
			doomed = append(doomed, c)
		}
	}
	s.mu.RUnlock()

	for _, c := range doomed {
		if err := c.Terminate(ctx); err != nil {
			s.log.WarnContext(ctx, "joaju: terminating a socket failed",
				slog.String("socket", string(c.ID())), slog.Any("error", err))
		}
	}

	return len(doomed), nil
}

// registryTenant is the one check the two registry reads share: a Grant a
// [ConnectPolicy] issued, carrying a tenant.
func registryTenant(g auth.Grant) (string, error) {
	if err := g.Check(Connect); err != nil {
		return "", fmt.Errorf("joaju: %w", err)
	}
	tenant := auth.Tenant(g)
	if tenant == "" {
		return "", fmt.Errorf("%w: the grant carries no tenant", ErrNoGrant)
	}

	return tenant, nil
}

// Close terminates every socket this server holds. It is what a shutdown calls,
// and it takes no Grant because it crosses every tenant on purpose.
func (s *Server) Close(ctx context.Context) {
	s.mu.RLock()
	doomed := make([]*Connection, 0, len(s.conns))
	for _, c := range s.conns {
		doomed = append(doomed, c)
	}
	s.mu.RUnlock()

	for _, c := range doomed {
		_ = c.Terminate(ctx)
	}
}

// handleSocket is GET /app/{appKey}: the socket itself.
func (s *Server) handleSocket(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("appKey") != s.appKey {
		s.refuse(w, r, http.StatusNotFound, "Unknown app.", nil)
		return
	}

	subject, ok := auth.SubjectFrom(r.Context())
	if !ok {
		s.refuse(w, r, http.StatusUnauthorized, "Unauthenticated.", nil)
		return
	}

	id, err := newSocketID()
	if err != nil {
		s.refuse(w, r, http.StatusInternalServerError, "Could not mint a socket id.", err)
		return
	}

	// The handshake is decided before the upgrade, so a refusal is an HTTP
	// status a browser reports rather than a socket that opens and shuts.
	grant, err := auth.Authorize(r.Context(), s.connect, subject, Connect, Handshake{
		Socket: id,
		Origin: r.Header.Get("Origin"),
	})
	if err != nil {
		status, message := http.StatusBadRequest, "The handshake could not be served."
		if errors.Is(err, auth.ErrForbidden) {
			status, message = http.StatusForbidden, "Forbidden."
		}
		s.refuse(w, r, status, message, err)

		return
	}

	// The same-origin check runs inside Upgrade, which writes its own 403.
	socket, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.InfoContext(r.Context(), "joaju: the upgrade was refused", slog.Any("error", err))
		return
	}

	sink := s.newSink(socket)
	conn, err := NewConnection(grant, id, sink)
	if err != nil {
		// A Grant that got this far and is refused here is a wiring mistake --
		// a ConnectPolicy issuing for another action, or a subject with no
		// tenant -- so the socket is closed without a protocol frame. Closing
		// the sink and not the Conn is what stops the writing goroutine: it
		// owns the Conn from the moment it starts.
		s.log.ErrorContext(r.Context(), "joaju: the authorized handshake did not yield a connection", slog.Any("error", err))
		_ = sink.Terminate(r.Context())
		return
	}

	s.read(r, conn, socket)
}

// refuse answers a socket route request that will not become a socket, and
// records why.
//
// Plain text and a status, which is what a refused upgrade already answers
// with: ws.Upgrader.Upgrade writes its own 403 that way when the Origin names
// another host, and answering the four decisions above it in some other shape
// would mean a client having to read two. What a [Protocol] answers its own
// routes with is its own, and this is not it.
//
// The refusal says only that it was refused. The sentence a Policy wrote names
// the subject and often the resource, and it goes to the log rather than to
// whoever was refused.
func (s *Server) refuse(w http.ResponseWriter, r *http.Request, status int, message string, err error) {
	if err != nil {
		s.log.InfoContext(r.Context(), "joaju: a socket was refused",
			slog.String("route", r.Method+" "+r.URL.Path),
			slog.Int("status", status),
			slog.Any("error", err))
	}

	http.Error(w, message, status)
}

// read runs the connection: it registers it, hands it to the [Protocol], and
// then reads it until the socket ends.
//
// This is the reading goroutine of the two -- it is the one net/http already
// gave us, so serving a socket costs one extra goroutine and not two. The other
// is the writer started by [Server.newSink], and it is the only thing that ever
// writes to this socket. Two writers on one Conn is a corrupted frame, which is
// why [Sink.Send] is a queue and not a write.
func (s *Server) read(r *http.Request, conn *Connection, socket *ws.Conn) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	if err := s.register(conn); err != nil {
		s.log.WarnContext(ctx, "joaju: the connection was refused by the tenant's limit",
			slog.String("socket", string(conn.ID())), slog.String("tenant", conn.Tenant()))
		// The refusal is written before the socket goes. A socket that only
		// closes says nothing a dropped network does not also say, and what a
		// client does about a dropped network is dial again -- so a tenant at
		// its limit would be dialled in a loop for as long as it stayed there.
		// 4004 is the code the Pusher clients already branch on.
		//
		// Saying so is best effort: a write that fails is logged and the close
		// goes ahead, because a socket that will not take the frame is a socket
		// there is nothing left to say to. context.WithoutCancel for the reason
		// Terminate uses it -- the request's context is often already cancelled
		// by the time an upgraded socket is refused, and a cancelled context
		// would drop the very frame that stops the loop.
		closing := context.WithoutCancel(ctx)
		if sendErr := s.tell(closing, conn, RefusalOverQuota); sendErr != nil {
			s.log.InfoContext(ctx, "joaju: the refused socket was closed without being told why",
				slog.String("socket", string(conn.ID())), slog.Any("error", sendErr))
		}
		s.observer.ConnectionClosed(ctx, conn.ID(), conn.Tenant(), ReasonLimit)
		_ = conn.Terminate(closing)
		return
	}
	s.observer.ConnectionOpened(ctx, conn.ID(), conn.Tenant())
	defer func() {
		s.unregister(conn)
		s.observer.ConnectionClosed(ctx, conn.ID(), conn.Tenant(), ReasonClient)
		s.protocol.Close(ctx, conn)
		_ = conn.Terminate(context.WithoutCancel(ctx))
	}()

	if err := s.protocol.Open(ctx, conn); err != nil {
		s.log.WarnContext(ctx, "joaju: the connection was dropped before it was established",
			slog.String("socket", string(conn.ID())), slog.Any("error", err))
		return
	}

	socket.SetReadLimit(s.maxMessageSize)
	// A client that says nothing for PongTimeout is gone, whatever its TCP
	// connection still claims. The server's ping and the client's pong reset
	// the deadline; so does any frame the client sends, because reading one is
	// proof enough that it is there.
	_ = socket.SetReadDeadline(time.Now().Add(s.pongTimeout))
	socket.SetPongHandler(func(string) error {
		return socket.SetReadDeadline(time.Now().Add(s.pongTimeout))
	})

	// The rate limit is one socket's, so it lives on this goroutine's stack: no
	// map that grows with the connection count, no lock, and nothing to evict
	// when the socket ends.
	limiter := newRateLimiter(s.maxMessagesPerSecond, time.Now())

	for {
		kind, message, err := socket.ReadMessage()
		if err != nil {
			if ws.IsUnexpectedClose(err, ws.CloseNormalClosure, ws.CloseGoingAway) {
				s.log.InfoContext(ctx, "joaju: the socket ended",
					slog.String("socket", string(conn.ID())), slog.Any("error", err))
			}
			return
		}
		_ = socket.SetReadDeadline(time.Now().Add(s.pongTimeout))

		// Counted here, where a frame the client sent has arrived and nothing
		// has acted on it yet. The WebSocket ping and pong never reach this
		// point -- ReadMessage answers a ping and hands a pong to the handler,
		// and returns neither -- and they should not: they are the transport
		// keeping itself alive, not the client asking for anything.
		if !limiter.allow(time.Now()) {
			s.log.InfoContext(ctx, "joaju: a frame was dropped by the socket's rate limit",
				slog.String("socket", string(conn.ID())),
				slog.Int("limit", s.maxMessagesPerSecond))
			_ = s.tell(ctx, conn, RefusalRateLimited)
			continue
		}

		// The opcode is read here and nowhere else, because it is the last
		// thing about a frame that is the transport's and this is where the
		// transport ends. [Protocol.Message] is handed bytes and cannot see
		// what framed them.
		//
		// The protocol is JSON over text frames. A binary frame carrying the
		// same JSON would be a second way to send every message there is: two
		// framings to read, two to test, and the one nobody thought about is
		// the one a client reaches for. It is dropped and the socket stays,
		// which is what happens to every other frame this server cannot act on.
		if kind != ws.TextMessage {
			s.log.InfoContext(ctx, "joaju: a frame was dropped because it was not text",
				slog.String("socket", string(conn.ID())), slog.Int("opcode", kind))
			_ = s.tell(ctx, conn, RefusalUnreadable)
			continue
		}

		// A refused frame is not a refused socket. A client that asks for a
		// channel it may not have is told so and goes on using the ones it has.
		if err := s.protocol.Message(ctx, conn, message); err != nil {
			s.log.InfoContext(ctx, "joaju: a frame was refused",
				slog.String("socket", string(conn.ID())), slog.Any("error", err))
		}
	}
}

// register admits the connection, or refuses it because the tenant is full.
//
// The check and the insert are one critical section on purpose. Checking under
// a read lock and inserting under a write lock lets two upgrades that both saw
// room take the last slot, and a limit that admits N+1 under load is a limit
// that fails exactly when it is needed.
func (s *Server) register(conn *Connection) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tenant := conn.Tenant()
	if s.maxConnections > 0 && s.perTenant[tenant] >= s.maxConnections {
		return ErrConnectionLimit
	}

	s.conns[conn.ID()] = conn
	s.perTenant[tenant]++
	return nil
}

func (s *Server) unregister(conn *Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, held := s.conns[conn.ID()]; !held {
		// Never registered -- refused by the limit, and its slot was never
		// taken. Decrementing here would let a refused connection lower the
		// count for the ones that were admitted.
		return
	}
	delete(s.conns, conn.ID())

	tenant := conn.Tenant()
	if s.perTenant[tenant] <= 1 {
		// The map entry goes when the last one does, so a tenant that connected
		// once and left costs nothing for the life of the process.
		delete(s.perTenant, tenant)
		return
	}
	s.perTenant[tenant]--
}

// newSocketID mints a socket id in the shape Pusher's clients print,
// "<digits>.<digits>".
//
// It is random and not a counter, because the id is what excludes the sender
// from its own broadcast: a client that can guess another's socket id can
// publish an event that everybody but that client receives.
func newSocketID() (SocketID, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("joaju: could not mint a socket id: %w", err)
	}

	return SocketID(fmt.Sprintf("%d.%d",
		binary.BigEndian.Uint32(b[0:4]),
		binary.BigEndian.Uint32(b[4:8]),
	)), nil
}

// sink is the writing half of one socket, and the [Sink] a [Connection] holds.
//
// It exists because a Conn may have one writer and a channel broadcast has as
// many callers as it has subscribers. Every one of them queues; one goroutine
// writes.
type sink struct {
	conn         *ws.Conn
	out          chan []byte
	done         chan struct{}
	once         sync.Once
	writeTimeout time.Duration
	pingInterval time.Duration
}

// newSink wraps the socket and starts its writer.
func (s *Server) newSink(conn *ws.Conn) *sink {
	k := &sink{
		conn:         conn,
		out:          make(chan []byte, s.outboundQueue),
		done:         make(chan struct{}),
		writeTimeout: s.writeTimeout,
		pingInterval: s.pingInterval,
	}
	go k.write()

	return k
}

// Send queues one frame.
//
// A client whose queue is full is closed rather than waited for. A broadcast
// runs through every subscriber of a channel, so blocking on the slowest one
// stops the channel for everybody -- and the slowest one is often a client that
// is already gone and whose TCP window has not noticed yet.
func (k *sink) Send(ctx context.Context, message []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case k.out <- message:
		return nil
	case <-k.done:
		return ErrSocketClosed
	default:
	}

	k.close()

	return fmt.Errorf("%w: it fell behind by more than %d frames", ErrSocketClosed, cap(k.out))
}

// Terminate closes the socket. It is safe to call more than once and from more
// than one goroutine, which matters: a shutdown and a read loop that ended race
// to call it.
func (k *sink) Terminate(context.Context) error {
	k.close()

	return nil
}

func (k *sink) close() {
	k.once.Do(func() { close(k.done) })
}

// write is the one goroutine that writes to this socket.
//
// It also sends the WebSocket ping that keeps the read deadline in
// [Server.read] from expiring. That ping is the transport's, and it is not the
// protocol's [EventPing]: a browser cannot send a WebSocket control frame from
// JavaScript, which is why the Pusher protocol has a ping of its own for the
// client to use. Different layers, each with one keepalive.
func (k *sink) write() {
	ticker := time.NewTicker(k.pingInterval)
	defer ticker.Stop()
	defer func() { _ = k.conn.Close() }()

	for {
		select {
		case message := <-k.out:
			_ = k.conn.SetWriteDeadline(time.Now().Add(k.writeTimeout))
			if err := k.conn.WriteMessage(ws.TextMessage, message); err != nil {
				k.close()
				return
			}
		case <-ticker.C:
			_ = k.conn.SetWriteDeadline(time.Now().Add(k.writeTimeout))
			if err := k.conn.WriteMessage(ws.PingMessage, nil); err != nil {
				k.close()
				return
			}
		case <-k.done:
			// What was queued before the close goes out first. A refusal is
			// queued and terminated in the same breath -- the connection limit
			// is the one that does it -- and a select whose two cases are both
			// ready picks between them at random, so without this the frame
			// that explains the close is a coin toss.
			k.flush()
			// A close frame first, so the client is told rather than left to
			// find out from a reset. Whether it arrives is not worth waiting
			// on: the socket is closed either way by the deferred Close.
			_ = k.conn.SetWriteDeadline(time.Now().Add(k.writeTimeout))
			_ = k.conn.WriteMessage(ws.CloseMessage,
				ws.FormatClose(ws.CloseNormalClosure, ""))
			return
		}
	}
}

// flush writes the frames already queued when the socket closed.
//
// It stops at the first write that fails, because the socket is going away
// either way, and it never writes more than the queue holds: a broadcast still
// reaching this socket while it closes must not keep the writer here.
func (k *sink) flush() {
	for i := 0; i < cap(k.out); i++ {
		select {
		case message := <-k.out:
			_ = k.conn.SetWriteDeadline(time.Now().Add(k.writeTimeout))
			if err := k.conn.WriteMessage(ws.TextMessage, message); err != nil {
				return
			}
		default:
			return
		}
	}
}

// tell writes what the [Protocol] says a [Refusal] looks like, and answers
// whether the write got through.
//
// A protocol with nothing to say for this refusal writes nothing, and this
// answers nil: the refusal itself has already been decided, and the frame is
// only how the client hears about it.
func (s *Server) tell(ctx context.Context, conn *Connection, r Refusal) error {
	message := s.protocol.Refuse(r)
	if len(message) == 0 {
		return nil
	}

	return conn.Send(ctx, message)
}

// rateLimiter meters one socket's inbound frames, and is the whole of
// [ServerConfig.MaxMessagesPerSecond].
//
// It is a token bucket and not a count reset every second, because a counted
// window has an edge: a client that spends its whole allowance at 0.99s and
// again at 1.01s puts twice the limit through in two milliseconds and trips
// nothing. A bucket has no edge -- it is refilled by the time that actually
// passed, so what a client spends early it does not have later.
//
// It is three words held by the goroutine that reads the socket, and that is
// the design: [Protocol] says calls concerning one connection are never
// concurrent, so there is nothing to lock, and a bucket kept on the stack of a
// loop is freed when the loop ends. A map of limiters would need a mutex on the
// per-frame path and an eviction for every socket that ever connected; a
// goroutine per connection would double what a connection costs.
type rateLimiter struct {
	// limit is the bucket's size and the frames one second buys. Zero is no
	// limit, and [rateLimiter.allow] answers that before anything else.
	limit int
	// cost is what one frame spends: a second divided by limit. It is computed
	// once because it is a division on the per-frame path, and because a limit
	// finer than a nanosecond rounds it to zero -- which is the divisor below,
	// and a zero there is a panic on a live socket.
	cost time.Duration
	// tokens is what is left to spend, and never exceeds limit.
	tokens int
	// filled is the instant the tokens on hand were earned at. It advances by
	// the time a refill actually consumed rather than straight to now, so the
	// fraction of a token that had not been earned yet is not thrown away.
	filled time.Time
}

// newRateLimiter starts the bucket full, so that the first frames of a
// connection are not the ones refused: a client subscribing to six channels the
// moment it opens is doing what a client does.
//
// A limit of zero or less, or one so fine that a frame costs less than a
// nanosecond, is no limit at all and produces the zero value.
func newRateLimiter(limit int, now time.Time) rateLimiter {
	if limit <= 0 || time.Second/time.Duration(limit) == 0 {
		return rateLimiter{}
	}

	return rateLimiter{
		limit:  limit,
		cost:   time.Second / time.Duration(limit),
		tokens: limit,
		filled: now,
	}
}

// allow reports whether one frame may be spent at now, and spends it if it may.
//
// now is a parameter and not a call to time.Now inside, so that a test can say
// what a second is instead of sleeping through one.
func (l *rateLimiter) allow(now time.Time) bool {
	if l.limit <= 0 {
		return true
	}

	if elapsed := now.Sub(l.filled); elapsed >= l.cost {
		room := int64(l.limit - l.tokens)
		if earned := int64(elapsed / l.cost); earned < room {
			l.tokens += int(earned)
			l.filled = l.filled.Add(time.Duration(earned) * l.cost)
		} else {
			// The bucket filled somewhere inside that gap and the time after
			// that earned nothing. Starting again from now is what stops a
			// socket that idled an hour from holding an hour of credit.
			l.tokens = l.limit
			l.filled = now
		}
	}
	if l.tokens == 0 {
		return false
	}
	l.tokens--

	return true
}
