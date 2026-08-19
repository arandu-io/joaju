package joaju_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/joaju"
	"github.com/arandu-io/joaju/ws"
)

// The application this file's server is. One server is one application, so
// these are values and not a lookup.
const (
	serverAppID  = "app-1"
	serverAppKey = "key-1"
)

// serverConnectPolicy answers every handshake the same way, so that a test can
// say "allowed" or "refused" and look at what the server did next.
type serverConnectPolicy struct{ err error }

func (p serverConnectPolicy) Can(context.Context, auth.Subject, auth.Action, joaju.Handshake) error {
	return p.err
}

// serverSubscriptionPolicy is the same for a subscription, and it records what
// it was asked about -- which is how a test asserts that the channel a route
// reached is the channel the policy saw.
type serverSubscriptionPolicy struct {
	err error

	mu    sync.Mutex
	asked []joaju.Subscription
}

func (p *serverSubscriptionPolicy) Can(_ context.Context, _ auth.Subject, _ auth.Action, s joaju.Subscription) error {
	p.mu.Lock()
	p.asked = append(p.asked, s)
	p.mu.Unlock()

	return p.err
}

func (p *serverSubscriptionPolicy) seen() []joaju.Subscription {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]joaju.Subscription(nil), p.asked...)
}

// serverChannel is a Channel that remembers what was broadcast on it.
type serverChannel struct {
	name        joaju.ChannelName
	subscribers []joaju.Subscriber

	mu        sync.Mutex
	delivered []joaju.Event
}

func (c *serverChannel) Name() joaju.ChannelName           { return c.name }
func (c *serverChannel) Connections() []joaju.Subscriber   { return c.subscribers }
func (c *serverChannel) Subscribed(*joaju.Connection) bool { return false }
func (c *serverChannel) Data() map[string]any              { return nil }
func (c *serverChannel) Unsubscribe(context.Context, *joaju.Connection) error {
	return nil
}

func (c *serverChannel) Find(joaju.SocketID) (joaju.Subscriber, bool) {
	return joaju.Subscriber{}, false
}

func (c *serverChannel) Subscribe(context.Context, auth.Grant, *joaju.Connection, joaju.Member) error {
	return nil
}

func (c *serverChannel) Broadcast(_ context.Context, e joaju.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delivered = append(c.delivered, e)

	return nil
}

func (c *serverChannel) BroadcastToAll(ctx context.Context, e joaju.Event) error {
	return c.Broadcast(ctx, e)
}

func (c *serverChannel) events() []joaju.Event {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]joaju.Event(nil), c.delivered...)
}

// serverBroker is a Broker over a map, which records every Grant it was handed
// so that a test can assert a policy ran and not merely that one could have.
type serverBroker struct {
	mu       sync.Mutex
	channels map[string]*serverChannel
	grants   []auth.Grant
}

func newServerBroker(channels ...*serverChannel) *serverBroker {
	b := &serverBroker{channels: make(map[string]*serverChannel, len(channels))}
	for _, c := range channels {
		b.channels[c.name.String()] = c
	}

	return b
}

func (b *serverBroker) record(g auth.Grant) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.grants = append(b.grants, g)
}

func (b *serverBroker) handed() []auth.Grant {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]auth.Grant(nil), b.grants...)
}

func (b *serverBroker) Find(_ context.Context, g auth.Grant, name joaju.ChannelName) (joaju.Channel, error) {
	b.record(g)

	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.channels[name.String()]
	if !ok {
		return nil, joaju.ErrNoChannel
	}

	return c, nil
}

func (b *serverBroker) FindOrCreate(ctx context.Context, g auth.Grant, name joaju.ChannelName) (joaju.Channel, error) {
	return b.Find(ctx, g, name)
}

func (b *serverBroker) Remove(_ context.Context, g auth.Grant, name joaju.ChannelName) error {
	b.record(g)

	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.channels, name.String())

	return nil
}

func (b *serverBroker) All(_ context.Context, g auth.Grant) ([]joaju.Channel, error) {
	b.record(g)

	b.mu.Lock()
	defer b.mu.Unlock()
	all := make([]joaju.Channel, 0, len(b.channels))
	for _, c := range b.channels {
		if c.name.Tenant() == auth.Tenant(g) {
			all = append(all, c)
		}
	}

	return all, nil
}

// serverProtocol is the frame layer as a recorder. Open writes one frame, so a
// test that reads it back has proved the writing goroutine runs.
type serverProtocol struct {
	openErr error

	mu       sync.Mutex
	opened   []joaju.SocketID
	messages []string
	closed   []joaju.SocketID
}

func (p *serverProtocol) Open(ctx context.Context, conn *joaju.Connection) error {
	p.mu.Lock()
	p.opened = append(p.opened, conn.ID())
	p.mu.Unlock()

	if p.openErr != nil {
		return p.openErr
	}

	return conn.Send(ctx, []byte(`{"event":"opened"}`))
}

func (p *serverProtocol) Message(_ context.Context, _ *joaju.Connection, message []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, string(message))

	return nil
}

func (p *serverProtocol) Close(_ context.Context, conn *joaju.Connection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = append(p.closed, conn.ID())
}

func (p *serverProtocol) sockets() []joaju.SocketID {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]joaju.SocketID(nil), p.opened...)
}

func (p *serverProtocol) received() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.messages...)
}

// serverSubject is the subject hesape/auth's Authenticate middleware would have
// put on the context. Nothing in joaju authenticates: it reads what the
// framework's front door left there.
func serverSubject(next http.Handler, subject auth.Subject, present bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if present {
			r = r.WithContext(auth.WithSubject(r.Context(), subject))
		}
		next.ServeHTTP(w, r)
	})
}

// serverFixture is one running server and the parts a test looks into.
type serverFixture struct {
	http      *httptest.Server
	broker    *serverBroker
	protocol  *serverProtocol
	subscribe *serverSubscriptionPolicy
	server    *joaju.Server
}

// newServerFixture starts a server whose caller is signed in as one subject of
// the tenant every channel here belongs to.
func newServerFixture(t *testing.T, cfg joaju.ServerConfig, channels ...*serverChannel) *serverFixture {
	t.Helper()

	f := &serverFixture{
		broker:    newServerBroker(channels...),
		protocol:  &serverProtocol{},
		subscribe: &serverSubscriptionPolicy{},
	}

	cfg.AppID = serverAppID
	cfg.AppKey = serverAppKey
	if cfg.Broker == nil {
		cfg.Broker = f.broker
	}
	if cfg.Connect == nil {
		cfg.Connect = serverConnectPolicy{}
	}
	if cfg.Subscribe == nil {
		cfg.Subscribe = f.subscribe
	}
	if cfg.Protocol == nil {
		cfg.Protocol = f.protocol
	}
	if cfg.Log == nil {
		// A refusal is logged, and every test here causes one on purpose. The
		// suite's output is not where they are read.
		cfg.Log = slog.New(slog.DiscardHandler)
	}

	server, err := joaju.NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer() = %v", err)
	}
	f.server = server

	f.http = httptest.NewServer(serverSubject(server, auth.Subject{ID: "u1", Tenant: tenant}, true))
	t.Cleanup(func() {
		server.Close(context.Background())
		f.http.Close()
	})

	return f
}

// socketURL is the ws:// address of the socket route.
func (f *serverFixture) socketURL(key string) string {
	return "ws" + strings.TrimPrefix(f.http.URL, "http") + "/app/" + key
}

// host is what a same-origin request claims, and what the upgrade compares the
// Origin header against.
func (f *serverFixture) host(t *testing.T) string {
	t.Helper()

	u, err := url.Parse(f.http.URL)
	if err != nil {
		t.Fatalf("parsing %q = %v", f.http.URL, err)
	}

	return u.Host
}

// get and post are the two shapes of API call this file makes.
func (f *serverFixture) get(t *testing.T, path string) (int, []byte) {
	t.Helper()

	response, err := f.http.Client().Get(f.http.URL + path)
	if err != nil {
		t.Fatalf("GET %s = %v", path, err)
	}
	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading GET %s = %v", path, err)
	}

	return response.StatusCode, body
}

func (f *serverFixture) post(t *testing.T, path, body string) (int, []byte) {
	t.Helper()

	response, err := f.http.Client().Post(f.http.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s = %v", path, err)
	}
	defer func() { _ = response.Body.Close() }()

	read, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading POST %s = %v", path, err)
	}

	return response.StatusCode, read
}

// dial opens the socket with the given Origin header. An empty origin sends no
// header at all, which is what a non-browser client does.
func (f *serverFixture) dial(t *testing.T, origin string) (*ws.Conn, *http.Response, error) {
	t.Helper()

	return f.dialKey(t, origin, serverAppKey)
}

func (f *serverFixture) dialKey(t *testing.T, origin, key string) (*ws.Conn, *http.Response, error) {
	t.Helper()

	header := http.Header{}
	if origin != "" {
		header.Set("Origin", origin)
	}

	dialer := *ws.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second

	return dialer.Dial(f.socketURL(key), header)
}

func TestNewServerRefusesAConfigWithNoPolicy(t *testing.T) {
	for _, one := range []struct {
		name string
		cfg  joaju.ServerConfig
	}{
		{"no app id", joaju.ServerConfig{AppKey: serverAppKey}},
		{"no app key", joaju.ServerConfig{AppID: serverAppID}},
		{"no broker", joaju.ServerConfig{AppID: serverAppID, AppKey: serverAppKey}},
		{"no connect policy", joaju.ServerConfig{
			AppID: serverAppID, AppKey: serverAppKey, Broker: newServerBroker(),
		}},
		{"no subscription policy", joaju.ServerConfig{
			AppID: serverAppID, AppKey: serverAppKey, Broker: newServerBroker(),
			Connect: serverConnectPolicy{},
		}},
		{"no protocol", joaju.ServerConfig{
			AppID: serverAppID, AppKey: serverAppKey, Broker: newServerBroker(),
			Connect: serverConnectPolicy{}, Subscribe: &serverSubscriptionPolicy{},
		}},
	} {
		t.Run(one.name, func(t *testing.T) {
			if _, err := joaju.NewServer(one.cfg); err == nil {
				t.Fatal("NewServer() accepted a config with a missing part, and a server without one refuses nothing")
			}
		})
	}
}

func TestNewServerRefusesAPongTimeoutShorterThanThePing(t *testing.T) {
	_, err := joaju.NewServer(joaju.ServerConfig{
		AppID: serverAppID, AppKey: serverAppKey, Broker: newServerBroker(),
		Connect: serverConnectPolicy{}, Subscribe: &serverSubscriptionPolicy{},
		Protocol:     &serverProtocol{},
		PingInterval: 30 * time.Second,
		PongTimeout:  10 * time.Second,
	})
	if err == nil {
		t.Fatal("NewServer() accepted a read deadline that expires before the pong it asks for")
	}
}

func TestServerRefusesASocketFromAnotherOrigin(t *testing.T) {
	f := newServerFixture(t, joaju.ServerConfig{})

	conn, response, err := f.dial(t, "http://evil.example")
	if err == nil {
		_ = conn.Close()
		t.Fatal("the socket opened for another origin, which is CSRF over a socket: the browser attaches the cookies and there is no preflight")
	}
	if response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("dialling from another origin answered %v, want %d", response, http.StatusForbidden)
	}
	if opened := f.protocol.sockets(); len(opened) != 0 {
		t.Fatalf("the protocol saw %d sockets, want none", len(opened))
	}
}

func TestServerAcceptsASocketFromTheSameOrigin(t *testing.T) {
	f := newServerFixture(t, joaju.ServerConfig{})

	conn, _, err := f.dial(t, "http://"+f.host(t))
	if err != nil {
		t.Fatalf("dialling from the same origin = %v", err)
	}
	defer func() { _ = conn.Close() }()

	// The frame the Protocol wrote in Open, read back off the wire: the writing
	// goroutine started, the Sink queued, and the socket is the one the
	// Connection holds.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, message, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("reading the first frame = %v", err)
	}
	if string(message) != `{"event":"opened"}` {
		t.Fatalf("the first frame was %q, want the one the Protocol wrote", message)
	}

	if opened := f.protocol.sockets(); len(opened) != 1 || opened[0] == "" {
		t.Fatalf("the protocol was handed %v, want one socket with an id", opened)
	}
}

func TestServerReadsWhatTheClientSendsIntoTheProtocol(t *testing.T) {
	f := newServerFixture(t, joaju.ServerConfig{})

	conn, _, err := f.dial(t, "http://"+f.host(t))
	if err != nil {
		t.Fatalf("dialling = %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.WriteMessage(ws.TextMessage, []byte(`{"event":"pusher:ping"}`)); err != nil {
		t.Fatalf("writing a frame = %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for len(f.protocol.received()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	got := f.protocol.received()
	if len(got) != 1 || got[0] != `{"event":"pusher:ping"}` {
		t.Fatalf("the protocol received %v, want the one frame the client sent", got)
	}
}

func TestServerRefusesASocketWithNoSubject(t *testing.T) {
	server, err := joaju.NewServer(joaju.ServerConfig{
		AppID: serverAppID, AppKey: serverAppKey, Broker: newServerBroker(),
		Connect: serverConnectPolicy{}, Subscribe: &serverSubscriptionPolicy{},
		Protocol: &serverProtocol{},
	})
	if err != nil {
		t.Fatalf("NewServer() = %v", err)
	}

	// No middleware, so nothing put an auth.Subject on the context.
	bare := httptest.NewServer(serverSubject(server, auth.Subject{}, false))
	defer bare.Close()

	response, err := bare.Client().Get(bare.URL + "/apps/" + serverAppID + "/channels")
	if err != nil {
		t.Fatalf("GET /channels = %v", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("a request with no subject answered %d, want %d", response.StatusCode, http.StatusUnauthorized)
	}
}

func TestServerRefusesEveryRouteOfAnotherApp(t *testing.T) {
	f := newServerFixture(t, joaju.ServerConfig{})

	for _, path := range []string{
		"/apps/other/channels",
		"/apps/other/channels/orders.17",
		"/apps/other/channels/orders.17/users",
		"/apps/other/connections",
	} {
		if status, _ := f.get(t, path); status != http.StatusNotFound {
			t.Fatalf("GET %s answered %d, want %d", path, status, http.StatusNotFound)
		}
	}

	conn, response, err := f.dialKey(t, "http://"+f.host(t), "another-key")
	if err == nil {
		_ = conn.Close()
		t.Fatal("the socket route accepted an app key it was not configured with")
	}
	if response == nil || response.StatusCode != http.StatusNotFound {
		t.Fatalf("dialling with another app key answered %v, want %d", response, http.StatusNotFound)
	}
}

func TestServerRefusesTheChannelListWhenThePolicyDoes(t *testing.T) {
	f := newServerFixture(t, joaju.ServerConfig{
		Subscribe: &serverSubscriptionPolicy{err: errors.New("not this tenant's dashboard")},
	})

	status, _ := f.get(t, "/apps/"+serverAppID+"/channels")
	if status != http.StatusForbidden {
		t.Fatalf("listing channels answered %d, want %d: listing is a read, and a read is authorized exactly like a write", status, http.StatusForbidden)
	}
}

func TestServerListsChannelsByTheNameTheClientAsked(t *testing.T) {
	orders := &serverChannel{name: channelName(t, "orders.17")}
	f := newServerFixture(t, joaju.ServerConfig{}, orders)

	status, body := f.get(t, "/apps/"+serverAppID+"/channels")
	if status != http.StatusOK {
		t.Fatalf("listing channels answered %d, want %d", status, http.StatusOK)
	}
	if strings.Contains(string(body), tenant) {
		t.Fatalf("the channel list carried the tenant: %s", body)
	}

	var listed struct {
		Channels map[string]map[string]any `json:"channels"`
	}
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatalf("decoding the channel list = %v", err)
	}
	if _, ok := listed.Channels["orders.17"]; !ok {
		t.Fatalf("the channel list was %v, want it keyed by the requested name", listed.Channels)
	}

	// The Grant the Broker was handed is the one the SubscriptionPolicy issued,
	// and it carries the tenant of the subject rather than of anything that
	// arrived with the request.
	handed := f.broker.handed()
	if len(handed) != 1 {
		t.Fatalf("the broker was handed %d grants, want 1", len(handed))
	}
	if auth.Tenant(handed[0]) != tenant {
		t.Fatalf("the broker was handed a grant for %q, want %q", auth.Tenant(handed[0]), tenant)
	}
}

func TestServerRefusesAChannelNameThatCarriesATenant(t *testing.T) {
	f := newServerFixture(t, joaju.ServerConfig{})

	status, _ := f.get(t, "/apps/"+serverAppID+"/channels/"+tenant+":orders.17")
	if status != http.StatusBadRequest {
		t.Fatalf("a channel name carrying a tenant answered %d, want %d: naming a tenant is choosing whose events you hear", status, http.StatusBadRequest)
	}
	if asked := f.subscribe.seen(); len(asked) != 0 {
		t.Fatalf("the subscription policy was asked about %v, want nothing: the name was refused before it existed", asked)
	}
}

func TestServerAnswersAboutOneChannel(t *testing.T) {
	orders := &serverChannel{
		name:        channelName(t, "presence-orders.17"),
		subscribers: []joaju.Subscriber{{Member: joaju.Member{UserID: "u1"}}, {Member: joaju.Member{UserID: "u1"}}, {Member: joaju.Member{UserID: "u2"}}},
	}
	f := newServerFixture(t, joaju.ServerConfig{}, orders)

	status, body := f.get(t, "/apps/"+serverAppID+"/channels/presence-orders.17")
	if status != http.StatusOK {
		t.Fatalf("asking about one channel answered %d, want %d", status, http.StatusOK)
	}

	var one struct {
		Occupied          bool `json:"occupied"`
		SubscriptionCount int  `json:"subscription_count"`
		UserCount         int  `json:"user_count"`
	}
	if err := json.Unmarshal(body, &one); err != nil {
		t.Fatalf("decoding the channel = %v", err)
	}
	// Three sockets, two people: one of them has two tabs open.
	if !one.Occupied || one.SubscriptionCount != 3 || one.UserCount != 2 {
		t.Fatalf("the channel answered %+v, want three subscriptions and two users", one)
	}

	asked := f.subscribe.seen()
	if len(asked) != 1 || asked[0].Channel.Requested() != "presence-orders.17" {
		t.Fatalf("the subscription policy was asked about %v, want the channel that was read", asked)
	}
}

func TestServerAnswersAboutTheMembersOfAPresenceChannel(t *testing.T) {
	orders := &serverChannel{
		name:        channelName(t, "presence-orders.17"),
		subscribers: []joaju.Subscriber{{Member: joaju.Member{UserID: "u1"}}, {Member: joaju.Member{UserID: "u1"}}},
	}
	f := newServerFixture(t, joaju.ServerConfig{}, orders)

	status, body := f.get(t, "/apps/"+serverAppID+"/channels/presence-orders.17/users")
	if status != http.StatusOK {
		t.Fatalf("asking for the members answered %d, want %d", status, http.StatusOK)
	}

	var users struct {
		Users []struct {
			ID string `json:"id"`
		} `json:"users"`
	}
	if err := json.Unmarshal(body, &users); err != nil {
		t.Fatalf("decoding the members = %v", err)
	}
	if len(users.Users) != 1 || users.Users[0].ID != "u1" {
		t.Fatalf("the members were %v, want the one person behind the two sockets", users.Users)
	}
}

func TestServerRefusesTheMembersOfAChannelThatHasNone(t *testing.T) {
	orders := &serverChannel{name: channelName(t, "orders.17")}
	f := newServerFixture(t, joaju.ServerConfig{}, orders)

	status, _ := f.get(t, "/apps/"+serverAppID+"/channels/orders.17/users")
	if status != http.StatusBadRequest {
		t.Fatalf("asking a public channel for its members answered %d, want %d", status, http.StatusBadRequest)
	}
}

func TestServerAnswersAnUnknownChannelWithNotFound(t *testing.T) {
	f := newServerFixture(t, joaju.ServerConfig{})

	status, _ := f.get(t, "/apps/"+serverAppID+"/channels/orders.17")
	if status != http.StatusNotFound {
		t.Fatalf("asking about an unheld channel answered %d, want %d", status, http.StatusNotFound)
	}
}

func TestServerPublishesToTheChannelTheGrantNamed(t *testing.T) {
	orders := &serverChannel{name: channelName(t, "orders.17")}
	f := newServerFixture(t, joaju.ServerConfig{}, orders)

	status, _ := f.post(t, "/apps/"+serverAppID+"/events",
		`{"name":"OrderShipped","channel":"orders.17","data":"{\"id\":17}","socket_id":"1.2"}`)
	if status != http.StatusOK {
		t.Fatalf("publishing answered %d, want %d", status, http.StatusOK)
	}

	delivered := orders.events()
	if len(delivered) != 1 {
		t.Fatalf("the channel received %d events, want 1", len(delivered))
	}
	e := delivered[0]
	if e.Name != "OrderShipped" || string(e.Data) != `{"id":17}` || e.Socket != "1.2" {
		t.Fatalf("the event was %+v, want the one that was published", e)
	}
	// The name the client sent has no tenant in it, and the name the channel is
	// held under does. Both, from one Grant.
	if e.Channel.Requested() != "orders.17" || e.Channel.String() != tenant+":orders.17" {
		t.Fatalf("the event went to %q, published as %q", e.Channel.Requested(), e.Channel.String())
	}
}

func TestServerRefusesToPublishAReservedEventName(t *testing.T) {
	orders := &serverChannel{name: channelName(t, "presence-orders.17")}
	f := newServerFixture(t, joaju.ServerConfig{}, orders)

	for _, name := range []string{"pusher:error", "pusher_internal:member_added"} {
		status, _ := f.post(t, "/apps/"+serverAppID+"/events",
			`{"name":"`+name+`","channel":"presence-orders.17","data":"{}"}`)
		if status != http.StatusBadRequest {
			t.Fatalf("publishing %q answered %d, want %d: a client that can send one can invent members", name, status, http.StatusBadRequest)
		}
	}
	if delivered := orders.events(); len(delivered) != 0 {
		t.Fatalf("the channel received %v, want nothing", delivered)
	}
}

func TestServerRefusesToPublishWhatIsNotJSON(t *testing.T) {
	orders := &serverChannel{name: channelName(t, "orders.17")}
	f := newServerFixture(t, joaju.ServerConfig{}, orders)

	status, _ := f.post(t, "/apps/"+serverAppID+"/events",
		`{"name":"OrderShipped","channel":"orders.17","data":"not json"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("publishing a payload that is not JSON answered %d, want %d", status, http.StatusBadRequest)
	}
}

func TestServerRefusesToPublishWhenTheSubscriptionPolicyDoes(t *testing.T) {
	orders := &serverChannel{name: channelName(t, "orders.17")}
	f := newServerFixture(t, joaju.ServerConfig{
		Subscribe: &serverSubscriptionPolicy{err: errors.New("not yours")},
	}, orders)

	status, _ := f.post(t, "/apps/"+serverAppID+"/events",
		`{"name":"OrderShipped","channel":"orders.17","data":"{}"}`)
	if status != http.StatusForbidden {
		t.Fatalf("publishing to a refused channel answered %d, want %d", status, http.StatusForbidden)
	}
	if delivered := orders.events(); len(delivered) != 0 {
		t.Fatalf("the channel received %v, want nothing", delivered)
	}
}

func TestServerAuthorizesEveryElementOfABatch(t *testing.T) {
	orders := &serverChannel{name: channelName(t, "orders.17")}
	invoices := &serverChannel{name: channelName(t, "invoices.9")}
	f := newServerFixture(t, joaju.ServerConfig{}, orders, invoices)

	status, _ := f.post(t, "/apps/"+serverAppID+"/batch_events",
		`{"batch":[{"name":"A","channel":"orders.17","data":"{}"},{"name":"B","channel":"invoices.9","data":"{}"}]}`)
	if status != http.StatusOK {
		t.Fatalf("publishing a batch answered %d, want %d", status, http.StatusOK)
	}

	asked := f.subscribe.seen()
	if len(asked) != 2 {
		t.Fatalf("the subscription policy was asked %d times, want once per element of the batch", len(asked))
	}
	if len(orders.events()) != 1 || len(invoices.events()) != 1 {
		t.Fatal("each element of the batch should have reached its own channel")
	}
}

func TestServerCountsOnlyTheConnectionsItHolds(t *testing.T) {
	f := newServerFixture(t, joaju.ServerConfig{})

	status, body := f.get(t, "/apps/"+serverAppID+"/connections")
	if status != http.StatusOK {
		t.Fatalf("counting connections answered %d, want %d", status, http.StatusOK)
	}
	if strings.TrimSpace(string(body)) != `{"connections":0}` {
		t.Fatalf("an empty server counted %s, want none", body)
	}

	conn, _, err := f.dial(t, "http://"+f.host(t))
	if err != nil {
		t.Fatalf("dialling = %v", err)
	}
	defer func() { _ = conn.Close() }()

	// The registry is written before the Protocol is opened, and the first
	// frame proves the socket got that far.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("reading the first frame = %v", err)
	}

	if _, body = f.get(t, "/apps/"+serverAppID+"/connections"); strings.TrimSpace(string(body)) != `{"connections":1}` {
		t.Fatalf("one open socket counted %s, want one", body)
	}
}

func TestServerTerminatesTheSubjectsSockets(t *testing.T) {
	f := newServerFixture(t, joaju.ServerConfig{})

	conn, _, err := f.dial(t, "http://"+f.host(t))
	if err != nil {
		t.Fatalf("dialling = %v", err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("reading the first frame = %v", err)
	}

	status, _ := f.post(t, "/apps/"+serverAppID+"/users/u1/terminate_connections", `{}`)
	if status != http.StatusOK {
		t.Fatalf("terminating answered %d, want %d", status, http.StatusOK)
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("the socket was still open after its subject's connections were terminated")
	}
}

func TestServerTerminatesNobodyForAnotherTenantsSubject(t *testing.T) {
	f := newServerFixture(t, joaju.ServerConfig{})

	conn, _, err := f.dial(t, "http://"+f.host(t))
	if err != nil {
		t.Fatalf("dialling = %v", err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("reading the first frame = %v", err)
	}

	// The subject in the path is not the one holding the socket. The tenant on
	// the Grant already scoped the search; the path only ever narrows it.
	if status, _ := f.post(t, "/apps/"+serverAppID+"/users/somebody-else/terminate_connections", `{}`); status != http.StatusOK {
		t.Fatalf("terminating answered %d, want %d", status, http.StatusOK)
	}

	_, body := f.get(t, "/apps/"+serverAppID+"/connections")
	if strings.TrimSpace(string(body)) != `{"connections":1}` {
		t.Fatalf("after terminating another subject the count was %s, want the socket still open", body)
	}
}

func TestServerAnswersItsHealthWithoutAGrant(t *testing.T) {
	server, err := joaju.NewServer(joaju.ServerConfig{
		AppID: serverAppID, AppKey: serverAppKey, Broker: newServerBroker(),
		Connect:  serverConnectPolicy{err: errors.New("nobody may connect")},
		Protocol: &serverProtocol{}, Subscribe: &serverSubscriptionPolicy{err: errors.New("nobody may listen")},
	})
	if err != nil {
		t.Fatalf("NewServer() = %v", err)
	}

	// No subject, no policy that allows anything: /up reads nothing, so it
	// needs nothing.
	bare := httptest.NewServer(serverSubject(server, auth.Subject{}, false))
	defer bare.Close()

	response, err := bare.Client().Get(bare.URL + "/up")
	if err != nil {
		t.Fatalf("GET /up = %v", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /up answered %d, want %d", response.StatusCode, http.StatusOK)
	}
}

func TestServerRefusesASocketTheConnectPolicyRefuses(t *testing.T) {
	f := newServerFixture(t, joaju.ServerConfig{
		Connect: serverConnectPolicy{err: errors.New("not from here")},
	})

	conn, response, err := f.dial(t, "http://"+f.host(t))
	if err == nil {
		_ = conn.Close()
		t.Fatal("the socket opened for a handshake the ConnectPolicy refused")
	}
	if response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("a refused handshake answered %v, want %d", response, http.StatusForbidden)
	}
	if opened := f.protocol.sockets(); len(opened) != 0 {
		t.Fatalf("the protocol saw %v, want nothing: the decision happens before the upgrade", opened)
	}
}

// serverLimitObserver records why the sockets it saw ended, so a test can say
// the refusal still reaches an Observer with the reason it always carried.
type serverLimitObserver struct {
	joaju.NopObserver

	mu      sync.Mutex
	reasons []string
}

func (o *serverLimitObserver) ConnectionClosed(_ context.Context, _ joaju.SocketID, _, reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reasons = append(o.reasons, reason)
}

func (o *serverLimitObserver) closed() []string {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([]string(nil), o.reasons...)
}

// A socket refused because the tenant is full is told 4004, and told it before
// the close.
//
// A close on its own is exactly what a dropped network looks like, and what a
// Pusher client does about a dropped network is dial again -- so a tenant at
// its limit would be reconnected to for as long as it stayed there.
func TestServerTellsARefusedSocketItIsOverQuotaBeforeClosingIt(t *testing.T) {
	observer := &serverLimitObserver{}
	f := newServerFixture(t, joaju.ServerConfig{MaxConnections: 1, Observer: observer})

	held, _, err := f.dial(t, "http://"+f.host(t))
	if err != nil {
		t.Fatalf("dialling the one connection the limit allows = %v", err)
	}
	defer func() { _ = held.Close() }()

	// Read what the Protocol wrote on the admitted socket, so the second dial
	// happens with the first one registered rather than still in flight.
	_ = held.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := held.ReadMessage(); err != nil {
		t.Fatalf("reading the admitted socket's first frame = %v", err)
	}

	refused, response, err := f.dial(t, "http://"+f.host(t))
	if err != nil {
		t.Fatalf("dialling past the limit = %v, answered %v -- the limit is refused after the upgrade, not during it", err, response)
	}
	defer func() { _ = refused.Close() }()

	_ = refused.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, message, err := refused.ReadMessage()
	if err != nil {
		t.Fatalf("reading the refusal = %v -- the socket was closed without saying why", err)
	}

	var frame struct {
		Event string `json:"event"`
		Data  string `json:"data"`
	}
	if err := json.Unmarshal(message, &frame); err != nil {
		t.Fatalf("the refusal %q is not a frame: %v", message, err)
	}
	if frame.Event != joaju.EventError {
		t.Fatalf("the refused socket was sent %q, want %q", frame.Event, joaju.EventError)
	}

	var data struct {
		Code    joaju.ErrorCode `json:"code"`
		Message string          `json:"message"`
	}
	if err := json.Unmarshal([]byte(frame.Data), &data); err != nil {
		t.Fatalf("the refusal's data %q is not the error payload: %v", frame.Data, err)
	}
	if data.Code != joaju.CodeOverQuota {
		t.Fatalf("the refused socket was told %d, want %d", data.Code, joaju.CodeOverQuota)
	}

	// And then it closes, because the frame is a refusal and not a connection.
	_ = refused.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := refused.ReadMessage(); err == nil {
		t.Fatal("the socket refused by the tenant's limit stayed open")
	}

	// The close the client just saw happened after the Observer was told, so
	// this reads what the refusal reported and not a half-written slice.
	if got := observer.closed(); len(got) != 1 || got[0] != joaju.ReasonLimit {
		t.Fatalf("the Observer was told %v, want one close with the reason %q", got, joaju.ReasonLimit)
	}

	// The Protocol never saw the refused socket: the limit is checked before it
	// is handed over, so nothing subscribed on a connection that was refused.
	if opened := f.protocol.sockets(); len(opened) != 1 {
		t.Fatalf("the protocol was handed %v, want only the admitted socket", opened)
	}
}

// skipOpenFrame reads the frame the fixture's Protocol writes in Open, so that
// what a test reads afterwards is an answer to what it sent.
func (f *serverFixture) skipOpenFrame(t *testing.T, conn *ws.Conn) {
	t.Helper()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatalf("reading the frame the Protocol wrote on open = %v", err)
	}
}

// waitFor waits until the fixture's Protocol has been handed the frame, and
// answers everything it holds. A frame the server dropped never arrives, which
// is what the deadline is for.
func (f *serverFixture) waitFor(t *testing.T, frame string) []string {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		held := f.protocol.received()
		for _, one := range held {
			if one == frame {
				return held
			}
		}
		if time.Now().After(deadline) {
			return held
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Past the limit the client is told 4301 and keeps its socket. There is no
// setting that closes it instead: a refusal that answers two ways is two
// behaviours to explain, and the client a limit is aimed at is the one worth
// keeping addressable.
func TestServerRefusesTheFramesPastTheSocketsRateLimit(t *testing.T) {
	const limit = 4
	f := newServerFixture(t, joaju.ServerConfig{MaxMessagesPerSecond: limit})

	conn, _, err := f.dial(t, "http://"+f.host(t))
	if err != nil {
		t.Fatalf("dialling = %v", err)
	}
	defer func() { _ = conn.Close() }()
	f.skipOpenFrame(t, conn)

	const burst = limit + 4
	for i := range burst {
		if err := conn.WriteMessage(ws.TextMessage, []byte(`{"event":"pusher:ping"}`)); err != nil {
			t.Fatalf("writing frame %d of the burst = %v", i, err)
		}
	}

	// The socket answered, which a closed one could not have done.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, message, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("reading the answer to a burst past the limit = %v", err)
	}
	if want := encode(t, joaju.ErrRateLimited.Frame()); string(message) != want {
		t.Fatalf("the answer to a burst past the limit was %s, want %s", message, want)
	}

	// And it is still serving: one refill later a frame gets through, and it
	// reaches the Protocol like any other.
	time.Sleep(time.Second/limit + 100*time.Millisecond)
	const after = `{"event":"pusher:pong"}`
	if err := conn.WriteMessage(ws.TextMessage, []byte(after)); err != nil {
		t.Fatalf("writing after the limit tripped = %v", err)
	}

	held := f.waitFor(t, after)
	if len(held) == 0 || held[len(held)-1] != after {
		t.Fatalf("the protocol holds %v, want the frame sent after the refill: the socket did not survive the refusal", held)
	}
	if len(held) > burst {
		t.Fatalf("the protocol was handed %d frames of the %d sent, so the limit dropped none of them", len(held), burst+1)
	}
}

// The protocol is JSON over text frames, and a binary one carrying the same
// JSON would be a second way to send every message there is.
//
// The frame is dropped and the socket stays, which is what happens to every
// other frame this server cannot act on.
func TestServerDropsAFrameThatIsNotText(t *testing.T) {
	f := newServerFixture(t, joaju.ServerConfig{})

	conn, _, err := f.dial(t, "http://"+f.host(t))
	if err != nil {
		t.Fatalf("dialling = %v", err)
	}
	defer func() { _ = conn.Close() }()
	f.skipOpenFrame(t, conn)

	// Bytes a text frame would have carried through, so what is being refused
	// is the framing and not the content.
	const binary = `{"event":"pusher:ping"}`
	if err := conn.WriteMessage(ws.BinaryMessage, []byte(binary)); err != nil {
		t.Fatalf("writing a binary frame = %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, message, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("reading the answer to a binary frame = %v", err)
	}
	if want := encode(t, joaju.ErrInvalidMessage.Frame()); string(message) != want {
		t.Fatalf("the answer to a binary frame was %s, want %s", message, want)
	}

	// The same bytes in a text frame do reach the Protocol, and the one sent
	// before them never did.
	const text = `{"event":"pusher:pong"}`
	if err := conn.WriteMessage(ws.TextMessage, []byte(text)); err != nil {
		t.Fatalf("writing the text frame after the binary one = %v", err)
	}
	held := f.waitFor(t, text)
	if len(held) != 1 || held[0] != text {
		t.Fatalf("the protocol was handed %v, want only %s: a binary frame reached the protocol, or the socket did not survive the refusal", held, text)
	}
}

// Under the limit nothing happens at all: every frame reaches the Protocol and
// the client is told nothing.
func TestServerLeavesASocketUnderItsRateLimitAlone(t *testing.T) {
	f := newServerFixture(t, joaju.ServerConfig{MaxMessagesPerSecond: 100})

	conn, _, err := f.dial(t, "http://"+f.host(t))
	if err != nil {
		t.Fatalf("dialling = %v", err)
	}
	defer func() { _ = conn.Close() }()
	f.skipOpenFrame(t, conn)

	frames := []string{
		`{"event":"pusher:subscribe","data":{"channel":"orders.17"}}`,
		`{"event":"pusher:ping"}`,
		`{"event":"pusher:unsubscribe","data":{"channel":"orders.17"}}`,
	}
	for i, one := range frames {
		if err := conn.WriteMessage(ws.TextMessage, []byte(one)); err != nil {
			t.Fatalf("writing frame %d = %v", i, err)
		}
	}

	if held := f.waitFor(t, frames[len(frames)-1]); len(held) != len(frames) {
		t.Fatalf("the protocol was handed %v, want all %d frames: none of them was past the limit", held, len(frames))
	}

	// Nothing came back. The deadline is the assertion: a 4301 would arrive
	// long before it, and a closed socket would answer with a close error.
	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, message, err := conn.ReadMessage()
	if err == nil {
		t.Fatalf("a socket under its limit was sent %s", message)
	}
	var closed *ws.CloseError
	if errors.As(err, &closed) {
		t.Fatalf("a socket under its limit was closed: %v", closed)
	}
}
