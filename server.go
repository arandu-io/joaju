package joaju

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
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
	// one that notices. Reverb calls the refusal ConnectionLimitExceeded and
	// leaves the number to configuration; the number here is a default that a
	// deployment raises knowingly.
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

// Protocol is the frame layer: what the [Server] hands a socket's traffic to.
//
// The server owns the socket -- the upgrade, the two goroutines, the deadlines,
// the registry -- and owns no part of the Pusher protocol. It sends no frame of
// its own, not even [EventConnectionEstablished], because a second place that
// builds a frame is a second answer to what a frame looks like (RULE 9).
//
// The three methods are Reverb's Protocols\Pusher\Server: open(), message() and
// close(). Its fourth, error(), is not here -- in Go the error comes back from
// the call that failed.
//
// An implementation is called from the one goroutine that reads a given socket,
// so calls concerning one [Connection] are ordered and never concurrent with
// each other. Calls concerning different connections are concurrent.
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
}

// ServerConfig is what a [Server] is built from.
//
// Everything without a default is required, and [NewServer] refuses a config
// missing one rather than filling in something safe-looking: a server with no
// [ConnectPolicy] would accept every socket, and a nil policy is exactly the
// mistake RULE 17 exists to make impossible.
type ServerConfig struct {
	// AppID is the {appId} the API routes carry, and AppKey is the {appKey} the
	// socket route carries. A request naming another app is answered 404.
	//
	// One server is one application (ADR 0052): Reverb serves several from one
	// instance because hosting PHP is expensive, and a Go binary is a process.
	// These are here so the route table is Reverb's and the names in a client's
	// configuration mean what they mean everywhere else.
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
	// Per tenant and not per server, for the reason RULE 14 gives: a global
	// limit lets one customer's traffic refuse another customer's connections,
	// which is a denial of service one of them did not cause and cannot see.
	MaxConnections int

	// Observer is told what happened, after it happened. Nil means
	// [NopObserver]. See [Observer] for why nothing there can refuse anything.
	Observer      Observer
	OutboundQueue int
	WriteTimeout  time.Duration
	PingInterval  time.Duration
	PongTimeout   time.Duration
}

// Server answers the nine routes: the socket and the eight of the HTTP API.
//
// It is an http.Handler and not a net/http.Server, so that it is mounted the
// way any other handler is -- behind hesape/auth's Authenticate middleware,
// which is where the auth.Subject on the request context comes from. That
// middleware is the only thing in the ecosystem that calls auth.WithSubject,
// and this server does not authenticate anybody: it reads the subject the
// middleware put there and asks a Policy about it.
//
// That is the one real difference from Reverb's API routes, which verify an
// app_secret HMAC on every call. Reverb has to, because it is a separate
// process with no session to read. Here the request has already been through
// the framework's front door, and a second credential of its own would be a
// second way to prove who is calling (RULE 9).
//
// # The authorization shape, which every route follows
//
//	[ConnectPolicy]       once per request  may this subject be here at all
//	[SubscriptionPolicy]  once per channel  may this subject reach this channel
//
// Both run on the API routes and not only on the socket, because listing
// channels, counting subscribers and reading a presence channel's members are
// reads of who is talking to whom, and RULE 17 opens no exception for reads. A
// dashboard that lists channels without a policy is a tenant boundary that
// holds everywhere except the dashboard.
//
// A [Handshake] is what the [ConnectPolicy] is asked about on an API route as
// well, with Socket empty -- an API caller is asking the same question a
// browser asks, minus the socket it wants opened. Inventing a third auth.Action
// for it would mean a third policy an application has to remember to write, and
// the question it would answer is the one [Connect] already answers.
//
// The Connect Grant is also what a [ChannelName] on an API route is built from,
// because a name needs a tenant and RULE 14 says a tenant comes off a Grant.
// The channel a caller named in the path is never trusted for that: it supplies
// the name after the tenant, and nothing else.
type Server struct {
	appID  string
	appKey string

	broker    Broker
	connect   ConnectPolicy
	subscribe SubscriptionPolicy
	protocol  Protocol
	log       *slog.Logger

	upgrader ws.Upgrader
	mux      *http.ServeMux

	maxMessageSize int64
	maxBodySize    int64
	outboundQueue  int
	writeTimeout   time.Duration
	pingInterval   time.Duration
	pongTimeout    time.Duration
	maxConnections int

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

	// Upgrader.CheckOrigin is deliberately left nil, which is the fork's
	// same-origin default: an Origin header naming another host is refused with
	// 403 before the socket exists. A WebSocket that accepts any origin is CSRF
	// over a socket -- the browser attaches the cookies either way, and there is
	// no preflight to stop it.
	//
	// There is no configuration field that widens it, and that is the decision.
	// A [ConnectPolicy] sees the Origin verbatim on the [Handshake] and may
	// refuse it; it cannot allow one this check refuses, so the two can only
	// narrow, never disagree. Cross-origin sockets are not available in the
	// first version, and when they are it will be this one function that
	// changes, not a list in a config file (ADR 0052, RULE 9).
	s.upgrader = ws.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	s.mux = http.NewServeMux()
	s.mux.HandleFunc("GET /app/{appKey}", s.handleSocket)
	s.mux.HandleFunc("POST /apps/{appId}/events", s.handleEvents)
	s.mux.HandleFunc("POST /apps/{appId}/batch_events", s.handleBatchEvents)
	s.mux.HandleFunc("GET /apps/{appId}/connections", s.handleConnections)
	s.mux.HandleFunc("GET /apps/{appId}/channels", s.handleChannels)
	s.mux.HandleFunc("GET /apps/{appId}/channels/{channel}", s.handleChannel)
	s.mux.HandleFunc("GET /apps/{appId}/channels/{channel}/users", s.handleChannelUsers)
	s.mux.HandleFunc("POST /apps/{appId}/users/{userId}/terminate_connections", s.handleTerminate)
	s.mux.HandleFunc("GET /up", s.handleUp)

	return s, nil
}

// ServeHTTP routes the nine routes.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Connections is how many sockets this process holds for the Grant's tenant.
//
// It takes a Grant for the reason [Broker.All] does: a count of who is
// connected is a read, and a count that spans tenants tells one customer how
// many of another's people are online. The Grant has to be one a
// [ConnectPolicy] issued -- auth.Grant.Check is what says so -- and the tenant
// it carries is the only filter, because it is the only one that did not come
// in with the request (RULE 14).
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
// It is the route Reverb calls terminate_connections, and it is what a sign-out
// or a revoked membership calls: the socket was authorized once, at the
// handshake, and nothing about it expires on its own.
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
		s.fail(w, r, err)
		return
	}

	// The fork's same-origin check runs inside Upgrade and writes its own 403.
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
		// 4004 is the code the Pusher clients already branch on, and it is the
		// one Reverb sends here, so a client that handles Reverb handles this.
		//
		// Saying so is best effort: a write that fails is logged and the close
		// goes ahead, because a socket that will not take the frame is a socket
		// there is nothing left to say to. context.WithoutCancel for the reason
		// Terminate uses it -- the request's context is often already cancelled
		// by the time an upgraded socket is refused, and a cancelled context
		// would drop the very frame that stops the loop.
		closing := context.WithoutCancel(ctx)
		if quota, encodeErr := Encode(ErrOverQuota.Frame()); encodeErr != nil {
			s.log.ErrorContext(ctx, "joaju: encoding the over-quota refusal failed",
				slog.Any("error", encodeErr))
		} else if sendErr := conn.Send(closing, quota); sendErr != nil {
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

	for {
		_, message, err := socket.ReadMessage()
		if err != nil {
			if ws.IsUnexpectedClose(err, ws.CloseNormalClosure, ws.CloseGoingAway) {
				s.log.InfoContext(ctx, "joaju: the socket ended",
					slog.String("socket", string(conn.ID())), slog.Any("error", err))
			}
			return
		}
		_ = socket.SetReadDeadline(time.Now().Add(s.pongTimeout))

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

// publishRequest is the body of POST /apps/{appId}/events, and one element of
// the batch of POST /apps/{appId}/batch_events.
//
// Data is a string and not a json.RawMessage because the Pusher protocol says
// so: the payload travels as a JSON string containing JSON, in both directions.
// [Event.Data] holds it decoded, which is the one place the double encoding is
// undone.
type publishRequest struct {
	Name     string   `json:"name"`
	Data     string   `json:"data"`
	Channel  string   `json:"channel"`
	Channels []string `json:"channels"`
	SocketID string   `json:"socket_id"`
}

// handleEvents is POST /apps/{appId}/events: publish one event.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	grant, ok := s.enter(w, r)
	if !ok {
		return
	}

	var body publishRequest
	if !s.decode(w, r, &body) {
		return
	}
	if err := s.publish(r.Context(), grant, body); err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, struct{}{})
}

// handleBatchEvents is POST /apps/{appId}/batch_events: publish many.
func (s *Server) handleBatchEvents(w http.ResponseWriter, r *http.Request) {
	grant, ok := s.enter(w, r)
	if !ok {
		return
	}

	var body struct {
		Batch []publishRequest `json:"batch"`
	}
	if !s.decode(w, r, &body) {
		return
	}
	// Each element is authorized on its own. A batch is a convenience for the
	// caller and never a way to reach a channel one of its elements could not
	// have reached alone.
	for _, one := range body.Batch {
		if err := s.publish(r.Context(), grant, one); err != nil {
			s.fail(w, r, err)
			return
		}
	}

	writeJSON(w, http.StatusOK, struct{}{})
}

// publish delivers one event to every channel it names.
//
// A channel nobody is subscribed to is not an error: the Broker has no channel
// under that name, and an event with no listener is delivered to no one, which
// is what happened either way.
func (s *Server) publish(ctx context.Context, connected auth.Grant, body publishRequest) error {
	if body.Name == "" {
		return errors.New("joaju: an event needs a name")
	}
	// The two reserved namespaces are the server's own voice. A client that
	// could publish pusher_internal:member_added could invent members, and one
	// that could publish pusher:error could tell another client its
	// subscription was refused.
	if strings.HasPrefix(body.Name, ProtocolPrefix) || strings.HasPrefix(body.Name, InternalPrefix) {
		return fmt.Errorf("joaju: %q is a reserved event name, and only the server may send one", body.Name)
	}
	// The payload is a JSON string containing JSON, and this is where the outer
	// string is undone. It is checked here rather than trusted, because
	// [Event.Data] is a json.RawMessage from here on and an invalid one fails
	// at the far end of a fanout, on somebody else's socket.
	if !json.Valid([]byte(body.Data)) {
		return errors.New("joaju: the data of an event has to be JSON")
	}

	requested := body.Channels
	if body.Channel != "" {
		requested = append([]string{body.Channel}, requested...)
	}
	if len(requested) == 0 {
		return errors.New("joaju: an event needs a channel")
	}

	for _, one := range requested {
		name, err := NewChannelName(connected, one)
		if err != nil {
			return err
		}
		grant, err := s.reach(ctx, connected.Subject(), name, "")
		if err != nil {
			return err
		}

		channel, err := s.broker.Find(ctx, grant, name)
		if errors.Is(err, ErrNoChannel) {
			continue
		}
		if err != nil {
			return err
		}

		// Broadcast and not BroadcastToAll: an empty [Event.Socket] already
		// means everybody, so the sender is excluded when there is one and
		// nobody is when there is not, through one call.
		if err := channel.Broadcast(ctx, Event{
			Name:    body.Name,
			Channel: name,
			Data:    json.RawMessage(body.Data),
			Socket:  SocketID(body.SocketID),
		}); err != nil {
			return err
		}
	}

	return nil
}

// handleConnections is GET /apps/{appId}/connections: how many sockets.
func (s *Server) handleConnections(w http.ResponseWriter, r *http.Request) {
	grant, ok := s.enter(w, r)
	if !ok {
		return
	}

	open, err := s.Connections(grant)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"connections": open})
}

// handleChannels is GET /apps/{appId}/channels: the channel list.
func (s *Server) handleChannels(w http.ResponseWriter, r *http.Request) {
	connected, ok := s.enter(w, r)
	if !ok {
		return
	}

	// The collection question, asked of the same policy that answers about one
	// channel: auth.Policy is asked about the zero resource for a collection
	// action, and a Grant issued for anything but broadcasting.ChannelJoin
	// reaches no Broker method.
	grant, err := auth.Authorize(r.Context(), s.subscribe, connected.Subject(), broadcasting.ChannelJoin, Subscription{})
	if err != nil {
		s.fail(w, r, err)
		return
	}

	channels, err := s.broker.All(r.Context(), grant)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	// The key is [ChannelName.Requested] and never [ChannelName.String]: the
	// caller asked about its own channels and the tenant they are held under is
	// not its to read back.
	listed := make(map[string]any, len(channels))
	for _, channel := range channels {
		entry := map[string]any{"occupied": len(channel.Connections()) > 0}
		if channel.Name().Type().Presence() {
			entry["user_count"] = countMembers(channel)
		}
		listed[channel.Name().Requested()] = entry
	}

	writeJSON(w, http.StatusOK, map[string]any{"channels": listed})
}

// handleChannel is GET /apps/{appId}/channels/{channel}: one channel.
func (s *Server) handleChannel(w http.ResponseWriter, r *http.Request) {
	channel, ok := s.channel(w, r)
	if !ok {
		return
	}

	subscribers := channel.Connections()
	body := map[string]any{
		"occupied":           len(subscribers) > 0,
		"subscription_count": len(subscribers),
	}
	if channel.Name().Type().Presence() {
		body["user_count"] = countMembers(channel)
	}

	writeJSON(w, http.StatusOK, body)
}

// handleChannelUsers is GET /apps/{appId}/channels/{channel}/users: the members
// of a presence channel.
func (s *Server) handleChannelUsers(w http.ResponseWriter, r *http.Request) {
	channel, ok := s.channel(w, r)
	if !ok {
		return
	}
	if !channel.Name().Type().Presence() {
		s.refuse(w, r, http.StatusBadRequest, "Only a presence channel has users.", nil)
		return
	}

	seen := make(map[string]bool)
	users := make([]map[string]string, 0, len(channel.Connections()))
	for _, subscriber := range channel.Connections() {
		// One member may hold several sockets on one channel -- two tabs -- and
		// the member list names people, not sockets.
		if id := subscriber.Member.UserID; id != "" && !seen[id] {
			seen[id] = true
			users = append(users, map[string]string{"id": id})
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"users": users})
}

// handleTerminate is POST /apps/{appId}/users/{userId}/terminate_connections.
func (s *Server) handleTerminate(w http.ResponseWriter, r *http.Request) {
	grant, ok := s.enter(w, r)
	if !ok {
		return
	}

	// The tenant comes off the Grant, so a caller naming another customer's
	// user id closes nothing: the loop matches on both, and the userId in the
	// path only ever narrows what the Grant already scoped.
	if _, err := s.Terminate(r.Context(), grant, r.PathValue("userId")); err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, struct{}{})
}

// handleUp is GET /up: the health check.
//
// It is the one route with no Grant, and it can be, because it reads nothing:
// it says this process is answering, which whoever can reach the port already
// knows.
func (s *Server) handleUp(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// enter is the first half of every API route: the app has to be this one, the
// request has to carry a subject, and the [ConnectPolicy] has to allow it.
func (s *Server) enter(w http.ResponseWriter, r *http.Request) (auth.Grant, bool) {
	if r.PathValue("appId") != s.appID {
		s.refuse(w, r, http.StatusNotFound, "Unknown app.", nil)
		return auth.Grant{}, false
	}

	subject, ok := auth.SubjectFrom(r.Context())
	if !ok {
		s.refuse(w, r, http.StatusUnauthorized, "Unauthenticated.", nil)
		return auth.Grant{}, false
	}

	// Socket is empty: an API caller is asking whether it may act on this
	// server, which is the question [Connect] answers, and it is not asking for
	// a socket to be opened. Origin is carried through because a browser
	// calling the API sends one and the policy that judges it is the same one.
	grant, err := auth.Authorize(r.Context(), s.connect, subject, Connect, Handshake{
		Origin: r.Header.Get("Origin"),
	})
	if err != nil {
		s.fail(w, r, err)
		return auth.Grant{}, false
	}

	return grant, true
}

// channel is the second half of the three routes that name one: [Server.enter],
// then the name built out of the Grant, then the [SubscriptionPolicy], then the
// Broker.
func (s *Server) channel(w http.ResponseWriter, r *http.Request) (Channel, bool) {
	connected, ok := s.enter(w, r)
	if !ok {
		return nil, false
	}

	name, err := NewChannelName(connected, r.PathValue("channel"))
	if err != nil {
		s.fail(w, r, err)
		return nil, false
	}
	grant, err := s.reach(r.Context(), connected.Subject(), name, "")
	if err != nil {
		s.fail(w, r, err)
		return nil, false
	}

	channel, err := s.broker.Find(r.Context(), grant, name)
	if err != nil {
		s.fail(w, r, err)
		return nil, false
	}

	return channel, true
}

// reach runs the [SubscriptionPolicy] for one channel and answers the Grant it
// issued, which is the only thing a [Broker] accepts.
func (s *Server) reach(ctx context.Context, subject auth.Subject, name ChannelName, socket SocketID) (auth.Grant, error) {
	return auth.Authorize(ctx, s.subscribe, subject, broadcasting.ChannelJoin, Subscription{
		Channel: name,
		Socket:  socket,
	})
}

// decode reads a JSON body of at most MaxBodySize.
func (s *Server) decode(w http.ResponseWriter, r *http.Request, into any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodySize)
	// Unknown fields are ignored rather than refused: the Pusher HTTP API has
	// fields this server does not read -- "info" is one -- and a client SDK
	// that sends one is not making a mistake.
	if err := json.NewDecoder(r.Body).Decode(into); err != nil {
		s.refuse(w, r, http.StatusBadRequest, "The body is not the JSON this route reads.", err)
		return false
	}

	return true
}

// fail answers an error from a policy, a name or a broker.
//
// A refusal says only that it was refused. The sentence a Policy wrote names
// the subject and often the resource, and it goes to the log rather than to
// whoever was refused.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, auth.ErrForbidden):
		s.refuse(w, r, http.StatusForbidden, "Forbidden.", err)
	case errors.Is(err, ErrNoChannel):
		s.refuse(w, r, http.StatusNotFound, "Unknown channel.", err)
	default:
		s.refuse(w, r, http.StatusBadRequest, "The request could not be served.", err)
	}
}

// refuse writes the answer and records the reason.
func (s *Server) refuse(w http.ResponseWriter, r *http.Request, status int, message string, err error) {
	if err != nil {
		s.log.InfoContext(r.Context(), "joaju: a request was refused",
			slog.String("route", r.Method+" "+r.URL.Path),
			slog.Int("status", status),
			slog.Any("error", err))
	}

	writeJSON(w, status, map[string]string{"message": message})
}

// writeJSON writes one JSON body.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// countMembers is how many distinct people a presence channel holds, which is
// not how many sockets it holds: two tabs are one member.
func countMembers(channel Channel) int {
	seen := make(map[string]bool)
	for _, subscriber := range channel.Connections() {
		if id := subscriber.Member.UserID; id != "" {
			seen[id] = true
		}
	}

	return len(seen)
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
