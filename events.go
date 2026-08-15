package joaju

import (
	"context"
	"sync"
)

// Observer is told what the server did, after it did it.
//
// It exists so that an application can count, log or audit without the server
// knowing about counting, logging or auditing. Reverb fires five events for the
// same purpose, through Laravel's dispatcher; there is no dispatcher here
// (ADR 0001), so the destination is a value somebody passes in.
//
// # Every method runs after the fact, and its return value is ignored
//
// None of these can refuse anything. A channel is created because somebody
// authorised a subscription; a message is sent because it was already
// broadcast. An observer that could veto would be a second authorisation path,
// and RULE 17 allows one -- the [SubscriptionPolicy] is it.
//
// # It must not block
//
// The server calls these on the path that serves connections. An observer that
// talks to a database on MessageSent turns every broadcast into a round trip,
// and a thousand messages a second into a thousand of them. Count in memory,
// and let something else read the counter.
//
// Nothing here is called with a lock held, so an observer may call back into
// the server without deadlocking. That is deliberate and it is tested.
type Observer interface {
	// ChannelCreated is the first subscription to a channel that did not exist.
	ChannelCreated(ctx context.Context, name ChannelName)

	// ChannelRemoved is the last subscriber leaving.
	ChannelRemoved(ctx context.Context, name ChannelName)

	// ConnectionOpened is a socket that finished the upgrade and was
	// authorised. A refused upgrade never reaches here.
	ConnectionOpened(ctx context.Context, id SocketID, tenant string)

	// ConnectionClosed is a socket that went away, for any reason: the client
	// closed it, the pong did not arrive in time, or the server terminated it.
	//
	// Reverb splits this into a close and a ConnectionPruned, because it prunes
	// on a schedule and has to say which happened. Here the read deadline does
	// the pruning, so there is one event and a reason string.
	ConnectionClosed(ctx context.Context, id SocketID, tenant, reason string)

	// MessageReceived is a frame that arrived from a client, before it is acted
	// on. The raw bytes, because that is what a diagnosis wants.
	MessageReceived(ctx context.Context, id SocketID, message []byte)

	// MessageSent is a frame written to a client.
	MessageSent(ctx context.Context, id SocketID, message []byte)
}

// Reasons a connection ended, as [Observer.ConnectionClosed] receives them.
const (
	// ReasonClient is the client hanging up, which is the ordinary case.
	ReasonClient = "client"
	// ReasonTimeout is nothing heard within PongTimeout.
	ReasonTimeout = "timeout"
	// ReasonTerminated is POST /apps/{id}/users/{userId}/terminate_connections.
	ReasonTerminated = "terminated"
	// ReasonShutdown is the server closing.
	ReasonShutdown = "shutdown"
	// ReasonLimit is the connection limit, refused after the upgrade.
	ReasonLimit = "limit"
)

// NopObserver ignores everything, and is what a server without one uses.
//
// It exists so that the server never checks for nil on a path it runs per
// message. The check would be free; the branch in six places would not be, and
// a nil that reaches one of them is a panic on a live connection.
type NopObserver struct{}

func (NopObserver) ChannelCreated(context.Context, ChannelName)                {}
func (NopObserver) ChannelRemoved(context.Context, ChannelName)                {}
func (NopObserver) ConnectionOpened(context.Context, SocketID, string)         {}
func (NopObserver) ConnectionClosed(context.Context, SocketID, string, string) {}
func (NopObserver) MessageReceived(context.Context, SocketID, []byte)          {}
func (NopObserver) MessageSent(context.Context, SocketID, []byte)              {}

var _ Observer = NopObserver{}

// Counter is an Observer that counts, and the one an application usually wants.
//
// It answers what GET /apps/{appId}/connections and the Pulse cards in Reverb
// answer: how many are connected, how many messages crossed, how many channels
// exist. It holds numbers and nothing else -- no history, no per-connection
// record -- because a server that remembers every connection is a server whose
// memory grows with churn.
//
// Safe for concurrent use. Every method may be called from any connection's
// goroutine at any time, which is the normal case rather than the exception.
type Counter struct {
	mu        sync.Mutex
	perTenant map[string]*TenantCount
}

// TenantCount is one tenant's numbers.
//
// Per tenant and never global, for the reason RULE 14 gives everywhere else: a
// single number tells one customer how busy another one is. The rate of a
// competitor's traffic is a business fact, and an operator reading a dashboard
// is not a reason to publish it.
type TenantCount struct {
	// Connections is how many are open right now. It goes down.
	Connections int64
	// Channels is how many exist right now. It goes down.
	Channels int64
	// Received and Sent are totals since the process started. They only go up,
	// so a reader can subtract two readings and get a rate.
	Received int64
	Sent     int64
}

// NewCounter has no Reverb counterpart: Laravel's recorders are registered into
// Pulse, and this is the value that plays their part.
func NewCounter() *Counter {
	return &Counter{perTenant: make(map[string]*TenantCount)}
}

// Read is one tenant's numbers, copied.
//
// A copy rather than a pointer, so that a caller reading a dashboard cannot
// hold a reference that changes while it renders -- or write through it.
func (c *Counter) Read(tenant string) TenantCount {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t := c.perTenant[tenant]; t != nil {
		return *t
	}
	return TenantCount{}
}

// Tenants is every tenant this counter has seen, for an operator's dashboard.
//
// It is NOT reachable from a request handler: the HTTP routes answer for the
// Grant's tenant and one tenant only. Anything calling this is running with the
// operator's own authority, outside the request path.
func (c *Counter) Tenants() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.perTenant))
	for name := range c.perTenant {
		names = append(names, name)
	}
	return names
}

func (c *Counter) bump(tenant string, f func(*TenantCount)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.perTenant[tenant]
	if t == nil {
		t = &TenantCount{}
		c.perTenant[tenant] = t
	}
	f(t)
}

func (c *Counter) ChannelCreated(_ context.Context, name ChannelName) {
	c.bump(name.Tenant(), func(t *TenantCount) { t.Channels++ })
}

func (c *Counter) ChannelRemoved(_ context.Context, name ChannelName) {
	c.bump(name.Tenant(), func(t *TenantCount) {
		if t.Channels > 0 {
			t.Channels--
		}
	})
}

func (c *Counter) ConnectionOpened(_ context.Context, _ SocketID, tenant string) {
	c.bump(tenant, func(t *TenantCount) { t.Connections++ })
}

func (c *Counter) ConnectionClosed(_ context.Context, _ SocketID, tenant, _ string) {
	c.bump(tenant, func(t *TenantCount) {
		if t.Connections > 0 {
			t.Connections--
		}
	})
}

// MessageReceived and MessageSent count frames and never keep them.
//
// The bytes are on the Observer interface because a diagnosis wants them; a
// counter that stored them would be a full traffic log nobody asked for, of
// customer data, growing without bound.
func (c *Counter) MessageReceived(_ context.Context, _ SocketID, _ []byte) {
	c.bump(counterAllTenants, func(t *TenantCount) { t.Received++ })
}

func (c *Counter) MessageSent(_ context.Context, _ SocketID, _ []byte) {
	c.bump(counterAllTenants, func(t *TenantCount) { t.Sent++ })
}

// counterAllTenants is where message totals land.
//
// The two message events carry a SocketID and no tenant, and looking the tenant
// up would mean the counter holding a socket-to-tenant map -- a second registry
// beside the server's, kept in step by hand (RULE 9). The totals are the one
// number here that is not per tenant, and this name says so rather than an empty
// string doing it silently.
const counterAllTenants = "*"

var _ Observer = (*Counter)(nil)
