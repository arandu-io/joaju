package joaju

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
)

// relayTestPolicy allows or refuses every subscription, so that a test can mint
// the auth.Grant a real [SubscriptionPolicy] would have issued.
type relayTestPolicy struct{ err error }

func (p relayTestPolicy) Can(context.Context, auth.Subject, auth.Action, Subscription) error {
	return p.err
}

// relayTestGrant issues a Grant for the given tenant and action.
func relayTestGrant(t *testing.T, tenant string, action auth.Action) auth.Grant {
	t.Helper()

	subject := auth.Subject{ID: "subscriber", Tenant: tenant}
	g, err := auth.Authorize(context.Background(), relayTestPolicy{}, subject, action, Subscription{})
	if err != nil {
		t.Fatalf("authorizing %s for %s: %v", action, tenant, err)
	}

	return g
}

// relayTestName builds a channel name for the tenant, the only way there is.
func relayTestName(t *testing.T, tenant, requested string) ChannelName {
	t.Helper()

	name, err := NewChannelName(relayTestGrant(t, tenant, broadcasting.ChannelJoin), requested)
	if err != nil {
		t.Fatalf("naming %s for %s: %v", requested, tenant, err)
	}

	return name
}

// relayTestChannel is a [Channel] that records what it was asked to deliver.
type relayTestChannel struct {
	name ChannelName

	mu       sync.Mutex
	received []Event
	err      error
}

func (c *relayTestChannel) Name() ChannelName                { return c.name }
func (c *relayTestChannel) Connections() []Subscriber        { return nil }
func (c *relayTestChannel) Find(SocketID) (Subscriber, bool) { return Subscriber{}, false }
func (c *relayTestChannel) Subscribed(*Connection) bool      { return false }
func (c *relayTestChannel) Data() map[string]any             { return nil }

func (c *relayTestChannel) Subscribe(context.Context, auth.Grant, *Connection, Member) error {
	return nil
}

func (c *relayTestChannel) Unsubscribe(context.Context, *Connection) error { return nil }

func (c *relayTestChannel) Broadcast(_ context.Context, e Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.received = append(c.received, e)

	return c.err
}

func (c *relayTestChannel) BroadcastToAll(ctx context.Context, e Event) error {
	return c.Broadcast(ctx, e)
}

func (c *relayTestChannel) delivered() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]Event(nil), c.received...)
}

// relayTestBus is a [Bus] in memory: one process standing in for Redis, so that
// two relays can be run in one test.
type relayTestBus struct {
	mu           sync.Mutex
	subscribers  map[string][]chan string
	publishErr   error
	subscribeErr error
	publishes    int
}

func newRelayTestBus() *relayTestBus {
	return &relayTestBus{subscribers: make(map[string][]chan string)}
}

func (b *relayTestBus) Publish(_ context.Context, channel string, message any) (int64, error) {
	b.mu.Lock()
	if b.publishErr != nil {
		err := b.publishErr
		b.mu.Unlock()

		return 0, err
	}
	b.publishes++
	targets := append([]chan string(nil), b.subscribers[channel]...)
	b.mu.Unlock()

	payload, _ := message.(string)
	for _, target := range targets {
		select {
		case target <- payload:
		default:
		}
	}

	return int64(len(targets)), nil
}

func (b *relayTestBus) Subscribe(ctx context.Context, channels []string, callback func(message, channel string)) error {
	b.mu.Lock()
	if b.subscribeErr != nil {
		err := b.subscribeErr
		b.mu.Unlock()

		return err
	}
	messages := make(chan string, 64)
	for _, channel := range channels {
		b.subscribers[channel] = append(b.subscribers[channel], messages)
	}
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		for _, channel := range channels {
			kept := b.subscribers[channel][:0]
			for _, other := range b.subscribers[channel] {
				if other != messages {
					kept = append(kept, other)
				}
			}
			b.subscribers[channel] = kept
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message := <-messages:
			callback(message, channels[0])
		}
	}
}

func (b *relayTestBus) listeners(topic string) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.subscribers[topic])
}

func (b *relayTestBus) fail(publish, subscribe error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.publishErr, b.subscribeErr = publish, subscribe
}

// relayTestLog is a logger writing into a buffer a test can read.
type relayTestLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *relayTestLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.buf.Write(p)
}

func (l *relayTestLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.buf.String()
}

func (l *relayTestLog) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// waitFor polls until the condition holds, and fails the test if it never does.
func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", what)
}

func TestTopicCarriesTheTenant(t *testing.T) {
	t.Parallel()

	one := Topic(relayTestName(t, "acme", "orders"))
	other := Topic(relayTestName(t, "globex", "orders"))

	if one == other {
		t.Fatalf("two tenants share the topic %q for the same channel name", one)
	}
	if want := TopicPrefix + "acme:orders"; one != want {
		t.Fatalf("topic is %q, want %q", one, want)
	}
	if !strings.HasPrefix(other, TopicPrefix) {
		t.Fatalf("topic %q does not begin with %q", other, TopicPrefix)
	}
	if got := Topic(ChannelName{}); got != "" {
		t.Fatalf("the zero channel name has the topic %q, want none", got)
	}
}

func TestRelayDeliversAcrossInstancesAndDropsItsOwnMessage(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()
	name := relayTestName(t, "acme", "private-orders.17")
	grant := relayTestGrant(t, "acme", broadcasting.ChannelJoin)
	topic := Topic(name)

	log := &relayTestLog{}
	first, err := NewRelay(context.Background(), "instance-one", bus, log.logger())
	if err != nil {
		t.Fatalf("building the first relay: %v", err)
	}
	defer func() { _ = first.Close() }()

	second, err := NewRelay(context.Background(), "instance-two", bus, log.logger())
	if err != nil {
		t.Fatalf("building the second relay: %v", err)
	}
	defer func() { _ = second.Close() }()

	here := &relayTestChannel{name: name}
	there := &relayTestChannel{name: name}

	if err := first.Join(grant, here); err != nil {
		t.Fatalf("joining on the first relay: %v", err)
	}
	if err := second.Join(grant, there); err != nil {
		t.Fatalf("joining on the second relay: %v", err)
	}

	waitFor(t, "both instances to be listening", func() bool { return bus.listeners(topic) == 2 })

	fromFirst := Event{
		Name:    "order.paid",
		Channel: name,
		Data:    json.RawMessage(`{"id":17}`),
		Socket:  SocketID("1234.5678"),
	}
	if err := first.Publish(context.Background(), fromFirst); err != nil {
		t.Fatalf("publishing from the first relay: %v", err)
	}

	fromSecond := Event{Name: "order.shipped", Channel: name, Data: json.RawMessage(`{"id":17}`)}
	if err := second.Publish(context.Background(), fromSecond); err != nil {
		t.Fatalf("publishing from the second relay: %v", err)
	}

	waitFor(t, "the second instance to deliver", func() bool { return len(there.delivered()) > 0 })
	waitFor(t, "the first instance to deliver", func() bool { return len(here.delivered()) > 0 })

	// Each instance sees both messages on the bus and delivers exactly the one
	// it did not send. Anything else is the message a client would read twice.
	delivered := there.delivered()
	if len(delivered) != 1 {
		t.Fatalf("the second instance delivered %d messages, want 1: %+v", len(delivered), delivered)
	}
	if delivered[0].Name != fromFirst.Name {
		t.Fatalf("the second instance delivered %q, want %q", delivered[0].Name, fromFirst.Name)
	}
	if delivered[0].Socket != fromFirst.Socket {
		t.Fatalf("the excluded socket is %q, want %q", delivered[0].Socket, fromFirst.Socket)
	}
	if string(delivered[0].Data) != string(fromFirst.Data) {
		t.Fatalf("the payload is %s, want %s", delivered[0].Data, fromFirst.Data)
	}
	if delivered[0].Channel.String() != name.String() {
		t.Fatalf("the channel is %q, want %q", delivered[0].Channel.String(), name.String())
	}

	delivered = here.delivered()
	if len(delivered) != 1 {
		t.Fatalf("the first instance delivered %d messages, want 1: %+v", len(delivered), delivered)
	}
	if delivered[0].Name != fromSecond.Name {
		t.Fatalf("the first instance delivered %q, want %q", delivered[0].Name, fromSecond.Name)
	}
}

func TestRelayKeepsTenantsApartOnTheBus(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()
	log := &relayTestLog{}

	relay, err := NewRelay(context.Background(), "instance-one", bus, log.logger())
	if err != nil {
		t.Fatalf("building the relay: %v", err)
	}
	defer func() { _ = relay.Close() }()

	other, err := NewRelay(context.Background(), "instance-two", bus, log.logger())
	if err != nil {
		t.Fatalf("building the other relay: %v", err)
	}
	defer func() { _ = other.Close() }()

	acme := relayTestName(t, "acme", "orders")
	globex := relayTestName(t, "globex", "orders")

	mine := &relayTestChannel{name: acme}
	theirs := &relayTestChannel{name: globex}

	if err := relay.Join(relayTestGrant(t, "acme", broadcasting.ChannelJoin), mine); err != nil {
		t.Fatalf("joining acme: %v", err)
	}
	if err := other.Join(relayTestGrant(t, "globex", broadcasting.ChannelJoin), theirs); err != nil {
		t.Fatalf("joining globex: %v", err)
	}

	waitFor(t, "acme to be listening", func() bool { return bus.listeners(Topic(acme)) == 1 })
	waitFor(t, "globex to be listening", func() bool { return bus.listeners(Topic(globex)) == 1 })

	if err := relay.Publish(context.Background(), Event{Name: "order.paid", Channel: acme}); err != nil {
		t.Fatalf("publishing on acme: %v", err)
	}

	// Nothing proves a negative, so wait for the message to have gone round the
	// bus at all: the acme instance dropping its own is what makes the topic the
	// only thing that could have carried it to globex.
	waitFor(t, "the message to reach the bus", func() bool {
		bus.mu.Lock()
		defer bus.mu.Unlock()

		return bus.publishes == 1
	})
	time.Sleep(20 * time.Millisecond)

	if got := theirs.delivered(); len(got) != 0 {
		t.Fatalf("globex received acme's traffic: %+v", got)
	}
}

func TestRelayRefusesAGrantThatAuthorizedSomethingElse(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()
	log := &relayTestLog{}

	relay, err := NewRelay(context.Background(), "instance-one", bus, log.logger())
	if err != nil {
		t.Fatalf("building the relay: %v", err)
	}
	defer func() { _ = relay.Close() }()

	name := relayTestName(t, "acme", "private-orders.17")
	channel := &relayTestChannel{name: name}

	if err := relay.Join(relayTestGrant(t, "acme", Connect), channel); err == nil {
		t.Fatal("a Grant issued for Connect joined a channel")
	}

	err = relay.Join(relayTestGrant(t, "globex", broadcasting.ChannelJoin), channel)
	if !errors.Is(err, ErrWrongTenant) {
		t.Fatalf("joining another tenant's channel answered %v, want ErrWrongTenant", err)
	}

	if err := relay.Join(auth.Grant{}, channel); err == nil {
		t.Fatal("the zero Grant joined a channel")
	}

	if joined := relay.Joined(); joined != 0 {
		t.Fatalf("the relay joined %d channels on refused Grants, want 0", joined)
	}
}

func TestRelayDegradesWhenTheBusIsUnreachable(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()
	unreachable := errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")
	bus.fail(unreachable, unreachable)

	log := &relayTestLog{}
	relay, err := NewRelay(context.Background(), "instance-one", bus, log.logger())
	if err != nil {
		t.Fatalf("building the relay: %v", err)
	}
	defer func() { _ = relay.Close() }()

	relay.retry = time.Hour // one attempt is enough to report the outage

	if relay.Degraded() {
		t.Fatal("the relay reported an outage before it had spoken to the bus")
	}

	name := relayTestName(t, "acme", "private-orders.17")
	channel := &relayTestChannel{name: name}

	// The subscription is accepted. An instance that refused here would turn a
	// Redis outage into every client failing to subscribe, when what the outage
	// actually costs is only the other instances.
	if err := relay.Join(relayTestGrant(t, "acme", broadcasting.ChannelJoin), channel); err != nil {
		t.Fatalf("joining while the bus is down: %v", err)
	}
	if joined := relay.Joined(); joined != 1 {
		t.Fatalf("the relay joined %d channels, want 1", joined)
	}

	// The publish is accepted too, for the same reason: the caller has already
	// delivered to this instance's own connections.
	if err := relay.Publish(context.Background(), Event{Name: "order.paid", Channel: name}); err != nil {
		t.Fatalf("publishing while the bus is down: %v", err)
	}

	waitFor(t, "the outage to be reported", relay.Degraded)

	written := log.String()
	if !strings.Contains(written, "cannot reach the message bus") {
		t.Fatalf("the outage was not written to the log:\n%s", written)
	}
	if !strings.Contains(written, "serving its own connections only") {
		t.Fatalf("the log does not say what the instance is still doing:\n%s", written)
	}
	if !strings.Contains(written, unreachable.Error()) {
		t.Fatalf("the log does not carry the error that caused it:\n%s", written)
	}
	if got := strings.Count(written, "cannot reach the message bus"); got != 1 {
		t.Fatalf("one outage was written to the log %d times, want 1:\n%s", got, written)
	}
}

func TestRelayRecoversWhenTheBusAnswersAgain(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()
	unreachable := errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")
	bus.fail(unreachable, unreachable)

	log := &relayTestLog{}
	relay, err := NewRelay(context.Background(), "instance-one", bus, log.logger())
	if err != nil {
		t.Fatalf("building the relay: %v", err)
	}
	defer func() { _ = relay.Close() }()

	relay.retry = 2 * time.Millisecond

	name := relayTestName(t, "acme", "orders")
	channel := &relayTestChannel{name: name}

	if err := relay.Join(relayTestGrant(t, "acme", broadcasting.ChannelJoin), channel); err != nil {
		t.Fatalf("joining while the bus is down: %v", err)
	}

	waitFor(t, "the outage to be reported", relay.Degraded)

	bus.fail(nil, nil)

	waitFor(t, "the subscription to be open again", func() bool { return bus.listeners(Topic(name)) == 1 })

	if err := relay.Publish(context.Background(), Event{Name: "order.paid", Channel: name}); err != nil {
		t.Fatalf("publishing once the bus answers: %v", err)
	}

	waitFor(t, "the recovery to be reported", func() bool { return !relay.Degraded() })

	if written := log.String(); !strings.Contains(written, "reached the message bus again") {
		t.Fatalf("the recovery was not written to the log:\n%s", written)
	}
}

func TestRelayWithoutABusServesItsOwnConnections(t *testing.T) {
	t.Parallel()

	log := &relayTestLog{}
	relay, err := NewRelay(context.Background(), "instance-alone", nil, log.logger())
	if err != nil {
		t.Fatalf("building the relay: %v", err)
	}
	defer func() { _ = relay.Close() }()

	if !relay.Degraded() {
		t.Fatal("a relay with no bus does not report that it is alone")
	}
	if written := log.String(); !strings.Contains(written, "no message bus") {
		t.Fatalf("a relay with no bus said nothing about it:\n%s", written)
	}

	name := relayTestName(t, "acme", "orders")
	channel := &relayTestChannel{name: name}

	if err := relay.Join(relayTestGrant(t, "acme", broadcasting.ChannelJoin), channel); err != nil {
		t.Fatalf("joining with no bus: %v", err)
	}
	if err := relay.Publish(context.Background(), Event{Name: "order.paid", Channel: name}); err != nil {
		t.Fatalf("publishing with no bus: %v", err)
	}
	if joined := relay.Joined(); joined != 1 {
		t.Fatalf("the relay joined %d channels, want 1", joined)
	}
	if err := relay.Leave(name); err != nil {
		t.Fatalf("leaving with no bus: %v", err)
	}
	if joined := relay.Joined(); joined != 0 {
		t.Fatalf("the relay still holds %d channels after leaving, want 0", joined)
	}
}

func TestRelayJoinIsIdempotentAndLeaveStopsTheSubscription(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()
	log := &relayTestLog{}

	relay, err := NewRelay(context.Background(), "instance-one", bus, log.logger())
	if err != nil {
		t.Fatalf("building the relay: %v", err)
	}
	defer func() { _ = relay.Close() }()

	name := relayTestName(t, "acme", "orders")
	grant := relayTestGrant(t, "acme", broadcasting.ChannelJoin)
	channel := &relayTestChannel{name: name}

	for range 3 {
		if err := relay.Join(grant, channel); err != nil {
			t.Fatalf("joining: %v", err)
		}
	}

	waitFor(t, "the subscription to open", func() bool { return bus.listeners(Topic(name)) == 1 })

	if joined := relay.Joined(); joined != 1 {
		t.Fatalf("three joins of one channel left %d subscriptions, want 1", joined)
	}

	if err := relay.Leave(name); err != nil {
		t.Fatalf("leaving: %v", err)
	}

	waitFor(t, "the subscription to close", func() bool { return bus.listeners(Topic(name)) == 0 })

	if err := relay.Leave(name); err != nil {
		t.Fatalf("leaving a channel that was never joined: %v", err)
	}
}

func TestRelayDropsAMessageItCannotAttributeOrAddress(t *testing.T) {
	t.Parallel()

	log := &relayTestLog{}
	relay, err := NewRelay(context.Background(), "instance-one", newRelayTestBus(), log.logger())
	if err != nil {
		t.Fatalf("building the relay: %v", err)
	}
	defer func() { _ = relay.Close() }()

	name := relayTestName(t, "acme", "orders")
	channel := &relayTestChannel{name: name}

	for _, payload := range []string{
		`{`,
		`{"origin":"","event":"order.paid","channel":"orders"}`,
		`{"origin":"instance-two","event":"order.paid","channel":"invoices"}`,
		`{"origin":"instance-one","event":"order.paid","channel":"orders"}`,
	} {
		relay.deliver(channel, name, payload)
	}

	if got := channel.delivered(); len(got) != 0 {
		t.Fatalf("the relay delivered %d messages it should have dropped: %+v", len(got), got)
	}

	relay.deliver(channel, name, `{"origin":"instance-two","event":"order.paid","channel":"orders"}`)

	got := channel.delivered()
	if len(got) != 1 {
		t.Fatalf("the relay delivered %d messages, want 1", len(got))
	}
	if got[0].Channel.Tenant() != "acme" {
		t.Fatalf("the delivered channel belongs to %q, want acme", got[0].Channel.Tenant())
	}
}

func TestRelayRefusesEverythingOnceClosed(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()
	log := &relayTestLog{}

	relay, err := NewRelay(context.Background(), "instance-one", bus, log.logger())
	if err != nil {
		t.Fatalf("building the relay: %v", err)
	}

	name := relayTestName(t, "acme", "orders")
	grant := relayTestGrant(t, "acme", broadcasting.ChannelJoin)
	channel := &relayTestChannel{name: name}

	if err := relay.Join(grant, channel); err != nil {
		t.Fatalf("joining: %v", err)
	}

	waitFor(t, "the subscription to open", func() bool { return bus.listeners(Topic(name)) == 1 })

	if err := relay.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	if err := relay.Close(); err != nil {
		t.Fatalf("closing twice: %v", err)
	}

	if got := bus.listeners(Topic(name)); got != 0 {
		t.Fatalf("%d subscriptions survived Close, want 0", got)
	}
	if err := relay.Join(grant, channel); !errors.Is(err, ErrRelayClosed) {
		t.Fatalf("joining a closed relay answered %v, want ErrRelayClosed", err)
	}
	if err := relay.Publish(context.Background(), Event{Name: "order.paid", Channel: name}); !errors.Is(err, ErrRelayClosed) {
		t.Fatalf("publishing on a closed relay answered %v, want ErrRelayClosed", err)
	}
}

func TestNewRelayRefusesWhatItCannotWorkWithout(t *testing.T) {
	t.Parallel()

	//nolint:staticcheck // a nil context is exactly what this checks for.
	if _, err := NewRelay(nil, "instance-one", newRelayTestBus(), nil); err == nil {
		t.Fatal("a relay was built with no context")
	}
	if _, err := NewRelay(context.Background(), "", newRelayTestBus(), nil); err == nil {
		t.Fatal("a relay was built with no instance id")
	}
}

func TestRelayRefusesAnEventItCannotAddress(t *testing.T) {
	t.Parallel()

	log := &relayTestLog{}
	relay, err := NewRelay(context.Background(), "instance-one", newRelayTestBus(), log.logger())
	if err != nil {
		t.Fatalf("building the relay: %v", err)
	}
	defer func() { _ = relay.Close() }()

	if err := relay.Publish(context.Background(), Event{Name: "order.paid"}); err == nil {
		t.Fatal("an event with no channel was published")
	}
	if err := relay.Publish(context.Background(), Event{Channel: relayTestName(t, "acme", "orders")}); err == nil {
		t.Fatal("an event with no name was published")
	}
	if err := relay.Join(relayTestGrant(t, "acme", broadcasting.ChannelJoin), nil); err == nil {
		t.Fatal("a nil channel was joined")
	}
	if err := relay.Leave(ChannelName{}); err == nil {
		t.Fatal("the zero channel name was left")
	}
}
