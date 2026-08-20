package joaju

import (
	"context"
	"sync"
)

// Counter is an Observer that counts, and the one an application usually wants.
//
// It answers what GET /apps/{appId}/connections answers: how many are
// connected, how many messages crossed, how many channels exist. It holds
// numbers and nothing else -- no history, no per-connection record -- because a
// server that remembers every connection is a server whose memory grows with
// churn.
//
// Safe for concurrent use. Every method may be called from any connection's
// goroutine at any time, which is the normal case rather than the exception.
type Counter struct {
	mu        sync.Mutex
	perTenant map[string]*TenantCount
}

// TenantCount is one tenant's numbers.
//
// Per tenant and never global, for the reason the tenant is on everything else
// here: a single number tells one customer how busy another one is. The rate
// of a competitor's traffic is a business fact, and an operator reading a
// dashboard is not a reason to publish it.
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

// NewCounter builds a [Counter] with no tenant recorded yet. A tenant appears
// the first time something is counted for it.
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
// beside the server's, kept in step by hand. The totals are the one number here
// that is not per tenant, and this name says so rather than an empty string
// doing it silently.
const counterAllTenants = "*"

var _ Observer = (*Counter)(nil)
