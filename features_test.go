package joaju

import (
	"context"
	"sync"
	"testing"
	"time"
)

// connFor is a connection held by user of tenant, using the same handshake
// policy the channel tests use.
func connFor(t *testing.T, tenant, user string) *Connection {
	t.Helper()
	conn, _ := channelTestConnection(t, tenant, user)
	return conn
}

// The limit is per tenant, and this is the test that says so.
//
// A global limit lets one customer's traffic refuse another customer's
// connections -- a denial of service one of them did not cause and cannot see.
func TestTheConnectionLimitIsPerTenantAndNotGlobal(t *testing.T) {
	s := &Server{
		maxConnections: 2,
		conns:          make(map[SocketID]*Connection),
		perTenant:      make(map[string]int),
		observer:       NopObserver{},
	}

	acme1 := connFor(t, "acme", "1")
	acme2 := connFor(t, "acme", "2")
	acme3 := connFor(t, "acme", "3")
	other := connFor(t, "globex", "4")

	if err := s.register(acme1); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := s.register(acme2); err != nil {
		t.Fatalf("second: %v", err)
	}
	if err := s.register(acme3); err == nil {
		t.Fatal("the third connection of a tenant at its limit was admitted")
	}

	// The other tenant is untouched. This is the whole point.
	if err := s.register(other); err != nil {
		t.Fatalf("another tenant was refused because acme was full: %v", err)
	}
}

// A refused connection must not decrement the count when it unwinds, or it
// lowers the limit for the ones that were admitted.
func TestARefusedConnectionDoesNotFreeSomebodyElsesSlot(t *testing.T) {
	s := &Server{
		maxConnections: 1,
		conns:          make(map[SocketID]*Connection),
		perTenant:      make(map[string]int),
		observer:       NopObserver{},
	}

	admitted := connFor(t, "acme", "1")
	refused := connFor(t, "acme", "2")

	if err := s.register(admitted); err != nil {
		t.Fatalf("admitting the first: %v", err)
	}
	if err := s.register(refused); err == nil {
		t.Fatal("the second was admitted past the limit")
	}

	// The refused one unwinds through the same defer the admitted one uses.
	s.unregister(refused)

	if got := s.perTenant["acme"]; got != 1 {
		t.Fatalf("the tenant holds %d connections, want 1 -- the refused one freed a slot it never took", got)
	}
}

// The tenant's entry goes when its last connection does, so a tenant that
// connected once and left costs nothing for the life of the process.
func TestTheTenantIsForgottenWhenItsLastConnectionLeaves(t *testing.T) {
	s := &Server{
		maxConnections: 10,
		conns:          make(map[SocketID]*Connection),
		perTenant:      make(map[string]int),
		observer:       NopObserver{},
	}

	c := connFor(t, "acme", "1")
	if err := s.register(c); err != nil {
		t.Fatal(err)
	}
	s.unregister(c)

	if _, held := s.perTenant["acme"]; held {
		t.Error("the tenant is still in the map after its last connection left")
	}
}

// Counting is per tenant for the same reason the limit is: one number tells one
// customer how busy another one is.
func TestTheCounterKeepsTenantsApart(t *testing.T) {
	c := NewCounter()
	ctx := context.Background()

	c.ConnectionOpened(ctx, "1", "acme")
	c.ConnectionOpened(ctx, "2", "acme")
	c.ConnectionOpened(ctx, "3", "globex")
	c.ConnectionClosed(ctx, "1", "acme", ReasonClient)

	if got := c.Read("acme").Connections; got != 1 {
		t.Errorf("acme holds %d, want 1", got)
	}
	if got := c.Read("globex").Connections; got != 1 {
		t.Errorf("globex holds %d, want 1", got)
	}
	if got := c.Read("nobody").Connections; got != 0 {
		t.Errorf("a tenant nobody connected for reports %d", got)
	}
}

// A count that goes below zero is a count that lies, and a double close is the
// ordinary way to reach one.
func TestTheCounterDoesNotGoNegativeOnADoubleClose(t *testing.T) {
	c := NewCounter()
	ctx := context.Background()

	c.ConnectionOpened(ctx, "1", "acme")
	c.ConnectionClosed(ctx, "1", "acme", ReasonClient)
	c.ConnectionClosed(ctx, "1", "acme", ReasonShutdown)

	if got := c.Read("acme").Connections; got != 0 {
		t.Errorf("the count is %d after a double close, want 0", got)
	}
}

// Every method is called from any connection's goroutine, which is the normal
// case rather than the exception.
func TestTheCounterIsSafeUnderConcurrency(t *testing.T) {
	c := NewCounter()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.ConnectionOpened(ctx, "x", "acme")
			c.MessageSent(ctx, "x", []byte("{}"))
			c.ConnectionClosed(ctx, "x", "acme", ReasonClient)
		}()
	}
	wg.Wait()

	if got := c.Read("acme").Connections; got != 0 {
		t.Errorf("connections settled at %d, want 0", got)
	}
	if got := c.Read(counterAllTenants).Sent; got != 50 {
		t.Errorf("counted %d sends, want 50", got)
	}
}

// The rate limit is a bucket and not a counted window, and the difference is
// that time buys the allowance back a piece at a time rather than all at once
// on a boundary.
func TestTheRateLimitRefillsByTheTimeThatPassed(t *testing.T) {
	start := time.Now()
	l := newRateLimiter(2, start)

	if !l.allow(start) || !l.allow(start) {
		t.Fatal("the bucket refused a frame inside the second it was given")
	}
	if l.allow(start) {
		t.Fatal("the bucket allowed a third frame in an instant that buys two")
	}

	// Half a second at two a second is worth one frame, and exactly one.
	half := start.Add(500 * time.Millisecond)
	if !l.allow(half) {
		t.Fatal("half a second earned nothing back at two frames a second")
	}
	if l.allow(half) {
		t.Fatal("half a second earned back both frames, so the refill is not the time that passed")
	}
}

// Credit does not accumulate: an hour of silence is still worth one second, or
// a client that says nothing all night gets to spend the night at once.
func TestTheRateLimitDoesNotBankAnIdleSocketsAllowance(t *testing.T) {
	start := time.Now()
	l := newRateLimiter(2, start)

	if !l.allow(start) || !l.allow(start) || l.allow(start) {
		t.Fatal("the bucket did not start at the limit it was given")
	}

	idle := start.Add(time.Hour)
	if !l.allow(idle) || !l.allow(idle) {
		t.Fatal("an idle hour did not refill the bucket")
	}
	if l.allow(idle) {
		t.Fatal("an idle hour banked more than the bucket holds")
	}
}

// Zero is what a server nobody configured a limit on has, and it is not a limit
// of zero frames.
func TestTheRateLimitIsOffWhenItIsNotSet(t *testing.T) {
	for _, limit := range []int{0, -1} {
		l := newRateLimiter(limit, time.Now())
		now := time.Now()
		for i := range 1_000 {
			if !l.allow(now) {
				t.Fatalf("limit %d refused frame %d, and an unset limit refuses nothing", limit, i)
			}
		}
	}
}
