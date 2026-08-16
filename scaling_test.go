package joaju

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
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
	// subscribers are who is on it, which is what the metrics routes read. A
	// channel that only has to receive leaves it empty.
	subscribers []Subscriber

	mu       sync.Mutex
	received []Event
	err      error
}

func (c *relayTestChannel) Name() ChannelName                { return c.name }
func (c *relayTestChannel) Connections() []Subscriber        { return c.subscribers }
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
	// carried is every payload that crossed, keyed by topic and in order. What
	// two instances say to each other is a wire format like any other, and this
	// is where a test reads the bytes of it.
	carried map[string][]string
}

func newRelayTestBus() *relayTestBus {
	return &relayTestBus{
		subscribers: make(map[string][]chan string),
		carried:     make(map[string][]string),
	}
}

func (b *relayTestBus) Publish(_ context.Context, channel string, message any) (int64, error) {
	payload, _ := message.(string)

	b.mu.Lock()
	if b.publishErr != nil {
		err := b.publishErr
		b.mu.Unlock()

		return 0, err
	}
	b.publishes++
	b.carried[channel] = append(b.carried[channel], payload)
	targets := append([]chan string(nil), b.subscribers[channel]...)
	b.mu.Unlock()

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

// published is what crossed one topic, in order.
func (b *relayTestBus) published(topic string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]string(nil), b.carried[topic]...)
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

// The application every server in this file is. One server is one application,
// so these are values and not a lookup.
const (
	fleetTestAppID  = "app-1"
	fleetTestAppKey = "key-1"
)

// The fixture below is a second one, and server_test.go's is the first. It is
// not a duplicate by choice: that file is the package's EXTERNAL test binary
// and this one is the internal binary, so nothing there can see the in-memory
// [Bus] declared here -- and a bus, which has to stand in for Redis pub/sub for
// two instances at once, is much the harder of the two to be asked to write
// twice. So the bus stays and the smaller half is restated: a Broker over a
// map, a Protocol that does nothing, and the middleware every route needs.

// fleetTestBroker is a [Broker] over a map, filtered by the Grant's tenant --
// which is the whole reason [Broker.All] takes one.
type fleetTestBroker struct {
	mu       sync.Mutex
	channels map[string]Channel
}

func newFleetTestBroker(channels ...*relayTestChannel) *fleetTestBroker {
	b := &fleetTestBroker{channels: make(map[string]Channel, len(channels))}
	for _, one := range channels {
		b.channels[one.name.String()] = one
	}

	return b
}

func (b *fleetTestBroker) Find(_ context.Context, g auth.Grant, name ChannelName) (Channel, error) {
	if auth.Tenant(g) != name.Tenant() {
		return nil, ErrWrongTenant
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	held, ok := b.channels[name.String()]
	if !ok {
		return nil, ErrNoChannel
	}

	return held, nil
}

func (b *fleetTestBroker) FindOrCreate(ctx context.Context, g auth.Grant, name ChannelName) (Channel, error) {
	return b.Find(ctx, g, name)
}

func (b *fleetTestBroker) Remove(_ context.Context, _ auth.Grant, name ChannelName) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.channels, name.String())

	return nil
}

func (b *fleetTestBroker) All(_ context.Context, g auth.Grant) ([]Channel, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	all := make([]Channel, 0, len(b.channels))
	for _, held := range b.channels {
		if held.Name().Tenant() == auth.Tenant(g) {
			all = append(all, held)
		}
	}

	return all, nil
}

// fleetTestProtocol is a [Protocol] that does nothing. No metrics route reaches
// one, and [NewServer] will not build a server without one.
type fleetTestProtocol struct{}

func (fleetTestProtocol) Open(context.Context, *Connection) error            { return nil }
func (fleetTestProtocol) Message(context.Context, *Connection, []byte) error { return nil }
func (fleetTestProtocol) Close(context.Context, *Connection)                 {}

// fleetTestInstance is one joaju process: a relay on the shared bus, and the
// server attached to it.
type fleetTestInstance struct {
	server *Server
	relay  *Relay
	log    *relayTestLog
}

// newFleetTestInstance starts one instance holding the given channels.
//
// bus may be nil, and an instance built that way is the degraded one: it has no
// fleet to ask and answers its metrics routes out of its own state.
func newFleetTestInstance(t *testing.T, id InstanceID, bus Bus, wait time.Duration, channels ...*relayTestChannel) *fleetTestInstance {
	t.Helper()

	log := &relayTestLog{}
	relay, err := NewRelay(context.Background(), id, bus, log.logger())
	if err != nil {
		t.Fatalf("building the relay of %s: %v", id, err)
	}
	t.Cleanup(func() { _ = relay.Close() })

	server, err := NewServer(ServerConfig{
		AppID:          fleetTestAppID,
		AppKey:         fleetTestAppKey,
		Broker:         RelayedBroker(newFleetTestBroker(channels...), relay),
		Connect:        channelTestConnectPolicy{},
		Subscribe:      relayTestPolicy{},
		Protocol:       fleetTestProtocol{},
		Log:            log.logger(),
		Relay:          relay,
		MetricsTimeout: wait,
	})
	if err != nil {
		t.Fatalf("building the server of %s: %v", id, err)
	}
	t.Cleanup(func() { server.Close(context.Background()) })

	// A channel is part of the fleet because it was joined into it, and
	// [Relay.Join] is the only way in: it takes the Grant that authorized the
	// subscription, and refuses one issued for another tenant.
	for _, one := range channels {
		if err := relay.Join(relayTestGrant(t, one.name.Tenant(), broadcasting.ChannelJoin), one); err != nil {
			t.Fatalf("joining %s on %s: %v", one.name, id, err)
		}
	}

	return &fleetTestInstance{server: server, relay: relay, log: log}
}

// hold registers one socket of user, which is what the socket route does once
// the handshake is authorized.
func (i *fleetTestInstance) hold(t *testing.T, tenant, user string) {
	t.Helper()

	if err := i.server.register(connFor(t, tenant, user)); err != nil {
		t.Fatalf("registering the socket of %s of %s: %v", user, tenant, err)
	}
}

// get calls one metrics route as a subject of the tenant, in place of hesape's
// Authenticate middleware -- which is the only thing anywhere that puts an
// auth.Subject on a request.
func (i *fleetTestInstance) get(t *testing.T, tenant, path string) (int, string) {
	t.Helper()

	request := httptest.NewRequest(http.MethodGet, path, nil)
	request = request.WithContext(auth.WithSubject(request.Context(), auth.Subject{ID: "reader", Tenant: tenant}))

	answer := httptest.NewRecorder()
	i.server.ServeHTTP(answer, request)

	return answer.Code, strings.TrimSpace(answer.Body.String())
}

// post calls one of the two publishing routes as a subject of the tenant, and
// is [fleetTestInstance.get] for the direction that writes.
func (i *fleetTestInstance) post(t *testing.T, tenant, path, body string) (int, string) {
	t.Helper()

	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request = request.WithContext(auth.WithSubject(request.Context(), auth.Subject{ID: "publisher", Tenant: tenant}))

	answer := httptest.NewRecorder()
	i.server.ServeHTTP(answer, request)

	return answer.Code, strings.TrimSpace(answer.Body.String())
}

// route is one of the four, for the application this file's servers are.
func fleetTestRoute(path string) string { return "/apps/" + fleetTestAppID + path }

// fleetTestListening waits until the whole fleet is subscribed.
//
// Until then Bus.Publish counts fewer subscribers than there are instances, and
// a question that nobody has subscribed to yet is one nobody answers. It is not
// a race the server has -- it is this test starting instances and asking them
// something in the same breath.
func fleetTestListening(t *testing.T, bus *relayTestBus, asker InstanceID, instances int) {
	t.Helper()

	waitFor(t, "the fleet to be listening for questions", func() bool {
		return bus.listeners(MetricsTopic) == instances
	})
	waitFor(t, "the asking instance to be listening for answers", func() bool {
		return bus.listeners(MetricsReplyTopic(asker)) == 1
	})
}

// fleetTestChannelBody is what the three channel routes answer with.
type fleetTestChannelBody struct {
	Occupied          bool `json:"occupied"`
	SubscriptionCount int  `json:"subscription_count"`
	UserCount         int  `json:"user_count"`
}

func fleetTestDecode(t *testing.T, body string, into any) {
	t.Helper()

	if err := json.Unmarshal([]byte(body), into); err != nil {
		t.Fatalf("decoding %s = %v", body, err)
	}
}

func TestAPublishedEventReachesTheOtherInstancesOnceAndStaysInItsTenant(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()

	// The same requested name under two customers. A client sees one name on the
	// wire, and what keeps the two apart is the tenant inside the [Topic].
	mine := relayTestName(t, "acme", "private-orders.17")
	theirs := relayTestName(t, "globex", "private-orders.17")

	here := &relayTestChannel{name: mine}
	there := &relayTestChannel{name: mine}
	elsewhere := &relayTestChannel{name: theirs}

	first := newFleetTestInstance(t, "instance-one", bus, time.Second, here)
	newFleetTestInstance(t, "instance-two", bus, time.Second, there, elsewhere)

	waitFor(t, "both instances to relay acme's channel", func() bool {
		return bus.listeners(Topic(mine)) == 2
	})
	waitFor(t, "globex's channel to be relayed", func() bool {
		return bus.listeners(Topic(theirs)) == 1
	})

	status, body := first.post(t, "acme", fleetTestRoute("/events"),
		`{"name":"order.paid","channel":"private-orders.17","data":"{\"id\":17}","socket_id":"1234.5678"}`)
	if status != http.StatusOK {
		t.Fatalf("publishing answered %d, want %d: %s", status, http.StatusOK, body)
	}

	waitFor(t, "the other instance to deliver", func() bool { return len(there.delivered()) > 0 })
	// Nothing proves a negative. By the time the other instance has delivered,
	// the message has been round the bus -- which is the moment the instance that
	// published it would have delivered it a second time.
	time.Sleep(20 * time.Millisecond)

	delivered := there.delivered()
	if len(delivered) != 1 {
		t.Fatalf("the other instance delivered %d messages, want 1: %+v", len(delivered), delivered)
	}
	if delivered[0].Name != "order.paid" || string(delivered[0].Data) != `{"id":17}` {
		t.Fatalf("the other instance delivered %+v, want the event that was published", delivered[0])
	}
	// The excluded socket travels, because the exclusion is fleet-wide: the
	// socket that published is held by whichever instance it dialled.
	if delivered[0].Socket != SocketID("1234.5678") {
		t.Fatalf("the excluded socket is %q, want 1234.5678", delivered[0].Socket)
	}
	// The name came off the subscription this instance holds and never off the
	// message, which is the whole of the tenant on this path.
	if delivered[0].Channel.Tenant() != "acme" {
		t.Fatalf("the delivered channel belongs to %q, want acme", delivered[0].Channel.Tenant())
	}

	if got := here.delivered(); len(got) != 1 {
		t.Fatalf("the instance that published delivered %d messages, want the one local broadcast: %+v", len(got), got)
	}
	if got := elsewhere.delivered(); len(got) != 0 {
		t.Fatalf("globex was handed acme's event: %+v", got)
	}
}

func TestADegradedInstanceStillDeliversWhatItWasPublished(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()
	unreachable := errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")
	bus.fail(unreachable, unreachable)

	name := relayTestName(t, "acme", "orders.17")
	channel := &relayTestChannel{name: name}

	alone := newFleetTestInstance(t, "instance-one", bus, time.Second, channel)

	// A Redis that is down costs the other instances and costs the route
	// nothing: the sockets this instance holds are still served, and a client
	// whose event was delivered may not be told that it was not.
	status, body := alone.post(t, "acme", fleetTestRoute("/events"),
		`{"name":"order.paid","channel":"orders.17","data":"{\"id\":17}"}`)
	if status != http.StatusOK {
		t.Fatalf("publishing while the bus is down answered %d, want %d: %s", status, http.StatusOK, body)
	}

	if got := channel.delivered(); len(got) != 1 {
		t.Fatalf("a degraded instance delivered %d messages to its own connections, want 1: %+v", len(got), got)
	}

	waitFor(t, "the outage to be reported", alone.relay.Degraded)

	if written := alone.log.String(); !strings.Contains(written, "cannot reach the message bus") {
		t.Fatalf("the outage was not written to the log:\n%s", written)
	}
}

func TestASubscriptionJoinsTheFleetAndTheLastOneLeavingLeavesIt(t *testing.T) {
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
	broker := relayedBroker{Broker: newFleetTestBroker(channel), relay: relay}
	grant := relayTestGrant(t, "acme", broadcasting.ChannelJoin)

	// A lookup is not a subscription. A metrics route naming a channel must not
	// open a pipe from the fleet into an instance holding nobody on it.
	if _, err := broker.Find(context.Background(), grant, name); err != nil {
		t.Fatalf("finding the channel: %v", err)
	}
	if joined := relay.Joined(); joined != 0 {
		t.Fatalf("a lookup relayed %d channels, want 0", joined)
	}

	if _, err := broker.FindOrCreate(context.Background(), grant, name); err != nil {
		t.Fatalf("subscribing to the channel: %v", err)
	}

	waitFor(t, "the subscription to open", func() bool { return bus.listeners(Topic(name)) == 1 })

	if joined := relay.Joined(); joined != 1 {
		t.Fatalf("the subscription relayed %d channels, want 1", joined)
	}

	// The Grant is what makes the join sound, so a Grant issued for something
	// else reaches no topic -- and a topic is the only way into a channel here.
	if _, err := broker.FindOrCreate(context.Background(), relayTestGrant(t, "acme", Connect), name); err == nil {
		t.Fatal("a Grant issued for Connect subscribed to a relayed channel")
	}

	if err := broker.Remove(context.Background(), grant, name); err != nil {
		t.Fatalf("removing the channel: %v", err)
	}

	waitFor(t, "the subscription to close", func() bool { return bus.listeners(Topic(name)) == 0 })

	if joined := relay.Joined(); joined != 0 {
		t.Fatalf("%d channels are still relayed after the last subscriber left, want 0", joined)
	}
}

func TestTheMetricsRoutesCountTheWholeFleetAndNotOneProcess(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()
	orders := relayTestName(t, "acme", "presence-orders.17")
	invoices := relayTestName(t, "acme", "invoices.9")

	// Bruno has a tab on each instance. He is two subscriptions and one member,
	// which is the difference between the two sums these routes do.
	here := &relayTestChannel{name: orders, subscribers: []Subscriber{
		{Member: Member{UserID: "ana"}},
		{Member: Member{UserID: "bruno"}},
	}}
	there := &relayTestChannel{name: orders, subscribers: []Subscriber{
		{Member: Member{UserID: "bruno"}},
		{Member: Member{UserID: "carla"}},
		{Member: Member{UserID: "carla"}},
	}}
	// A channel not one socket of the first instance is on. Left out of the
	// list, it would tell a customer they are talking on fewer channels than
	// they are.
	elsewhere := &relayTestChannel{name: invoices, subscribers: []Subscriber{
		{Member: Member{UserID: "ana"}},
	}}

	first := newFleetTestInstance(t, "instance-one", bus, time.Second, here)
	second := newFleetTestInstance(t, "instance-two", bus, time.Second, there, elsewhere)

	first.hold(t, "acme", "ana")
	first.hold(t, "acme", "bruno")
	second.hold(t, "acme", "bruno-second-tab")
	second.hold(t, "acme", "carla")
	second.hold(t, "acme", "dora")

	fleetTestListening(t, bus, "instance-one", 2)

	status, body := first.get(t, "acme", fleetTestRoute("/connections"))
	if status != http.StatusOK {
		t.Fatalf("counting the fleet's sockets answered %d, want %d: %s", status, http.StatusOK, body)
	}
	if body != `{"connections":5}` {
		t.Fatalf("the fleet's sockets counted %s, want two here and three on the other instance", body)
	}

	status, body = first.get(t, "acme", fleetTestRoute("/channels/presence-orders.17"))
	if status != http.StatusOK {
		t.Fatalf("asking about one channel answered %d, want %d: %s", status, http.StatusOK, body)
	}

	var one fleetTestChannelBody
	fleetTestDecode(t, body, &one)
	if !one.Occupied || one.SubscriptionCount != 5 {
		t.Fatalf("the channel answered %+v, want the two subscriptions here and the three there", one)
	}
	if one.UserCount != 3 {
		t.Fatalf("the channel answered %d members, want ana, bruno and carla -- bruno holds a socket on each instance and is one person", one.UserCount)
	}

	status, body = first.get(t, "acme", fleetTestRoute("/channels/presence-orders.17/users"))
	if status != http.StatusOK {
		t.Fatalf("asking for the members answered %d, want %d: %s", status, http.StatusOK, body)
	}

	var members struct {
		Users []struct {
			ID string `json:"id"`
		} `json:"users"`
	}
	fleetTestDecode(t, body, &members)

	named := make([]string, 0, len(members.Users))
	for _, user := range members.Users {
		named = append(named, user.ID)
	}
	if len(named) != 3 {
		t.Fatalf("the members were %v, want the three people behind the five sockets", named)
	}
	for _, want := range []string{"ana", "bruno", "carla"} {
		if !slices.Contains(named, want) {
			t.Fatalf("the members were %v, want %s among them", named, want)
		}
	}

	status, body = first.get(t, "acme", fleetTestRoute("/channels"))
	if status != http.StatusOK {
		t.Fatalf("listing the channels answered %d, want %d: %s", status, http.StatusOK, body)
	}
	if strings.Contains(body, "acme:") {
		t.Fatalf("the channel list carried the tenant: %s", body)
	}

	var listed struct {
		Channels map[string]fleetTestChannelBody `json:"channels"`
	}
	fleetTestDecode(t, body, &listed)

	if got := listed.Channels["presence-orders.17"]; !got.Occupied || got.UserCount != 3 {
		t.Fatalf("the list says %+v about presence-orders.17, want it occupied with three members", got)
	}
	held, listedElsewhere := listed.Channels["invoices.9"]
	if !listedElsewhere {
		t.Fatalf("the list is %v, want the channel only the other instance holds in it", listed.Channels)
	}
	if !held.Occupied {
		t.Fatalf("the list says %+v about invoices.9, want it occupied: somebody is on it, just not here", held)
	}
}

func TestAMetricsRouteAnswersWithoutTheInstanceThatWentQuiet(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()
	orders := relayTestName(t, "acme", "orders.17")

	here := &relayTestChannel{name: orders, subscribers: []Subscriber{{Member: Member{UserID: "ana"}}}}
	there := &relayTestChannel{name: orders, subscribers: []Subscriber{{Member: Member{UserID: "bruno"}}}}

	wait := 50 * time.Millisecond
	first := newFleetTestInstance(t, "instance-one", bus, wait, here)
	newFleetTestInstance(t, "instance-two", bus, wait, there)

	// A third subscriber that takes the question and never answers: an instance
	// being replaced, one behind a partition, one too busy to read its socket.
	// It is the failure a fleet actually has, and the route may not wait for it.
	quiet, hangUp := context.WithCancel(context.Background())
	defer hangUp()

	go func() { _ = bus.Subscribe(quiet, []string{MetricsTopic}, func(string, string) {}) }()

	fleetTestListening(t, bus, "instance-one", 3)

	started := time.Now()
	status, body := first.get(t, "acme", fleetTestRoute("/channels/orders.17"))
	took := time.Since(started)

	if status != http.StatusOK {
		t.Fatalf("asking about one channel answered %d, want %d: %s", status, http.StatusOK, body)
	}

	var one fleetTestChannelBody
	fleetTestDecode(t, body, &one)
	if one.SubscriptionCount != 2 {
		t.Fatalf("the channel answered %+v, want this instance's subscription and the one instance that answered", one)
	}
	if took < wait {
		t.Fatalf("the route answered in %s, which is less than the %s it should have waited for the quiet instance", took, wait)
	}
	if took > 5*time.Second {
		t.Fatalf("the route took %s to answer, and an instance that stopped answering held it open", took)
	}
}

func TestAFleetAnswerNeverCrossesATenant(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()

	// The same requested name under two customers, which is the case the tenant
	// is in every topic for: on the wire the client sees one name, and these two
	// channels must never meet.
	mine := relayTestName(t, "acme", "presence-orders.17")
	theirs := relayTestName(t, "globex", "presence-orders.17")

	here := &relayTestChannel{name: mine, subscribers: []Subscriber{{Member: Member{UserID: "ana"}}}}
	there := &relayTestChannel{name: theirs, subscribers: []Subscriber{
		{Member: Member{UserID: "gina"}},
		{Member: Member{UserID: "hugo"}},
	}}

	first := newFleetTestInstance(t, "instance-one", bus, time.Second, here)
	second := newFleetTestInstance(t, "instance-two", bus, time.Second, there)

	first.hold(t, "acme", "ana")
	second.hold(t, "globex", "gina")
	second.hold(t, "globex", "hugo")
	second.hold(t, "globex", "iris")

	fleetTestListening(t, bus, "instance-one", 2)

	status, body := first.get(t, "acme", fleetTestRoute("/connections"))
	if status != http.StatusOK {
		t.Fatalf("counting sockets answered %d, want %d: %s", status, http.StatusOK, body)
	}
	if body != `{"connections":1}` {
		t.Fatalf("acme was told %s sockets are open, want its own one: a sum that crosses a tenant is a leak with a number on it", body)
	}

	status, body = first.get(t, "acme", fleetTestRoute("/channels/presence-orders.17"))
	if status != http.StatusOK {
		t.Fatalf("asking about one channel answered %d, want %d: %s", status, http.StatusOK, body)
	}

	var one fleetTestChannelBody
	fleetTestDecode(t, body, &one)
	if one.SubscriptionCount != 1 || one.UserCount != 1 {
		t.Fatalf("acme's channel answered %+v, want the one subscription and the one member acme has", one)
	}

	status, body = first.get(t, "acme", fleetTestRoute("/channels/presence-orders.17/users"))
	if status != http.StatusOK {
		t.Fatalf("asking for the members answered %d, want %d: %s", status, http.StatusOK, body)
	}
	if strings.Contains(body, "gina") || strings.Contains(body, "hugo") {
		t.Fatalf("acme was handed globex's member list: %s", body)
	}
	if !strings.Contains(body, "ana") {
		t.Fatalf("acme's own member is missing from %s", body)
	}
}

func TestADegradedInstanceAnswersItsMetricsRoutesFromItsOwnState(t *testing.T) {
	t.Parallel()

	orders := relayTestName(t, "acme", "orders.17")
	channel := &relayTestChannel{name: orders, subscribers: []Subscriber{{Member: Member{UserID: "ana"}}}}

	// No bus is the degradation that cannot recover, and an instance whose
	// Redis is down is in the same state: [Relay.Degraded] is one flag and
	// there is no third answer.
	//
	// The timeout is an hour. If a degraded instance asked the fleet and waited
	// for it, this test would not finish.
	alone := newFleetTestInstance(t, "instance-alone", nil, time.Hour, channel)
	alone.hold(t, "acme", "ana")

	if !alone.relay.Degraded() {
		t.Fatal("an instance with no bus does not report that it is alone")
	}

	started := time.Now()

	status, body := alone.get(t, "acme", fleetTestRoute("/channels/orders.17"))
	if status != http.StatusOK {
		t.Fatalf("asking about one channel answered %d, want %d: %s", status, http.StatusOK, body)
	}

	var one fleetTestChannelBody
	fleetTestDecode(t, body, &one)
	if one.SubscriptionCount != 1 {
		t.Fatalf("the channel answered %+v, want the one subscription this instance holds", one)
	}

	status, body = alone.get(t, "acme", fleetTestRoute("/connections"))
	if status != http.StatusOK {
		t.Fatalf("counting sockets answered %d, want %d: %s", status, http.StatusOK, body)
	}
	if body != `{"connections":1}` {
		t.Fatalf("a degraded instance counted %s, want the one socket it holds", body)
	}

	if took := time.Since(started); took > 10*time.Second {
		t.Fatalf("a degraded instance spent %s on two metrics routes, and it has nothing to wait for", took)
	}
}

func TestAFleetAnswerAboutAnotherTenantIsRefusedByTheInstanceThatAsked(t *testing.T) {
	t.Parallel()

	// The instance that answers filters by the tenant it was asked about, and
	// the instance that asked refuses anything that came back about a different
	// one. Only the second of those two is testable from here, because a
	// well-behaved instance never sends the answer that would trip it -- and
	// that is the point of having it: an instance answering about a tenant
	// nobody asked about is either broken or is not an instance.
	log := &relayTestLog{}
	relay, err := NewRelay(context.Background(), "instance-one", newRelayTestBus(), log.logger())
	if err != nil {
		t.Fatalf("building the relay: %v", err)
	}
	defer func() { _ = relay.Close() }()

	var tally fleetTally

	tally.add(relay.log, relay.ID(), "acme", fleetAnswer{
		Origin:      "instance-two",
		Request:     "instance-one.whatever",
		Tenant:      "globex",
		Connections: 9,
		Channels: map[string]channelAnswer{
			"presence-orders.17": {Subscriptions: 9, Users: []string{"gina"}},
		},
	})

	if tally.connections != 0 || len(tally.channels) != 0 {
		t.Fatalf("an answer about globex was added to acme's numbers: %+v", tally)
	}
	if written := log.String(); !strings.Contains(written, "another tenant") {
		t.Fatalf("the refusal was not written to the log:\n%s", written)
	}

	// This instance's own answer is refused too, and for the neighbouring
	// reason: its numbers came off a Grant and were already added by the route.
	tally.add(relay.log, relay.ID(), "acme", fleetAnswer{
		Origin:      relay.ID(),
		Request:     "instance-one.whatever",
		Tenant:      "acme",
		Connections: 4,
	})

	if tally.connections != 0 {
		t.Fatalf("this instance's own sockets were counted twice: %+v", tally)
	}

	// An answer to the question that was asked, about the tenant it was asked
	// about, is the one that counts.
	tally.add(relay.log, relay.ID(), "acme", fleetAnswer{
		Origin:      "instance-two",
		Request:     "instance-one.whatever",
		Tenant:      "acme",
		Connections: 4,
		Channels: map[string]channelAnswer{
			"presence-orders.17": {Subscriptions: 2, Users: []string{"ana", "", "ana"}},
		},
	})

	if tally.connections != 4 {
		t.Fatalf("the fleet's sockets counted %d, want 4", tally.connections)
	}
	if got := tally.channel("presence-orders.17"); got.subscriptions != 2 {
		t.Fatalf("the fleet's subscriptions counted %d, want 2", got.subscriptions)
	}
	if got := tally.channel("presence-orders.17").members(); len(got) != 1 || got[0] != "ana" {
		t.Fatalf("the fleet's members are %v, want the one person named twice and no empty id", got)
	}
	if got := tally.channel("invoices.9"); got.subscriptions != 0 || got.users != nil {
		t.Fatalf("a channel nobody answered about is %+v, want nothing", got)
	}
}

func TestARequestIDCollidesWithNothing(t *testing.T) {
	t.Parallel()

	// Two instances asking at the same moment, and one instance asking twice:
	// the two ways a correlation collides, and an answer added to the wrong
	// question is one customer's numbers on another customer's dashboard.
	first, err := NewRelay(context.Background(), "instance-one", newRelayTestBus(), (&relayTestLog{}).logger())
	if err != nil {
		t.Fatalf("building the first relay: %v", err)
	}
	defer func() { _ = first.Close() }()

	second, err := NewRelay(context.Background(), "instance-two", newRelayTestBus(), (&relayTestLog{}).logger())
	if err != nil {
		t.Fatalf("building the second relay: %v", err)
	}
	defer func() { _ = second.Close() }()

	minted := make(map[string]bool, 2048)
	for range 1024 {
		for _, id := range []string{first.newRequestID(), second.newRequestID()} {
			if minted[id] {
				t.Fatalf("the request id %q was minted twice", id)
			}
			minted[id] = true
		}
	}

	// The instance is in it, so that two ids minted in the same instant on two
	// instances differ in more than luck.
	if id := first.newRequestID(); !strings.HasPrefix(id, string(first.ID())+".") {
		t.Fatalf("the request id %q does not name the instance that asked", id)
	}
}

// whisperTestInstance is one process a client event has to cross: the Pusher
// protocol over a Broker that is joined to the fleet.
//
// It is [newFleetTestInstance] with the two halves that fixture stubs out made
// real, and without the [Server]: a client event arrives as a frame and is
// answered by the [Protocol], so no route is on this path. The protocol is the
// real one because nothing else produces a client- event, and the Broker is the
// real one because what a receiver reads is written by a [channel] to the sockets
// it seated.
type whisperTestInstance struct {
	protocol Protocol
	relay    *Relay
}

func newWhisperTestInstance(t *testing.T, id InstanceID, bus Bus) *whisperTestInstance {
	t.Helper()

	relay, err := NewRelay(context.Background(), id, bus, (&relayTestLog{}).logger())
	if err != nil {
		t.Fatalf("building the relay of %s: %v", id, err)
	}
	t.Cleanup(func() { _ = relay.Close() })

	// One value, handed to the protocol -- which is what [NewServer] refuses a
	// server for not doing, and what puts the relay within the protocol's reach.
	broker := RelayedBroker(NewMemoryBroker(), relay)

	return &whisperTestInstance{
		protocol: NewPusher(broker, relayTestPolicy{}, PusherConfig{ClientEvents: ClientEventsOn}),
		relay:    relay,
	}
}

// seat opens a socket on this instance and subscribes it as user, with the frame
// a browser sends.
func (i *whisperTestInstance) seat(t *testing.T, tenant, user, requested string) (*Connection, *channelTestSink) {
	t.Helper()

	conn, sink := channelTestConnection(t, tenant, user)
	i.send(t, conn, `{"event":"pusher:subscribe","data":{"channel":"`+requested+
		`","channel_data":"{\"user_id\":\"`+user+`\"}"}}`)

	return conn, sink
}

// send hands one frame to this instance, as the goroutine reading that socket
// does.
func (i *whisperTestInstance) send(t *testing.T, conn *Connection, frame string) {
	t.Helper()

	if err := i.protocol.Message(context.Background(), conn, []byte(frame)); err != nil {
		t.Fatalf("%s answered %s with %v", i.relay.ID(), frame, err)
	}
}

// whisperTestReceived is every client event one socket was written, as bytes.
//
// A subscriber's sink also holds its subscription confirmation and the arrivals
// it was told about. This test compares whole frames, so the ones that are not
// client events are left out rather than counted around.
func whisperTestReceived(t *testing.T, sink *channelTestSink) []string {
	t.Helper()

	sink.mu.Lock()
	defer sink.mu.Unlock()

	received := make([]string, 0, len(sink.messages))
	for _, message := range sink.messages {
		var f Frame
		if err := json.Unmarshal(message, &f); err != nil {
			t.Fatalf("a socket was written something that is not a frame: %v (%s)", err, message)
		}
		if f.IsClientEvent() {
			received = append(received, string(message))
		}
	}

	return received
}

// A client event reaches the fleet, and it reaches it carrying who sent it.
//
// It used to stop at [Channel.Broadcast], so two browsers on one channel saw
// each other type only while the same process happened to hold both -- the
// defect [Server.carry] fixed for the events API, on the half that was measured
// later. What a receiver draws is "u2 is typing", so the sender's user_id has to
// survive the bus: it is stamped from the seat the channel holds on the instance
// that took the frame, and no other instance has a seat for that socket to read
// it off.
func TestAClientEventCrossesTheFleetCarryingTheSendersUserID(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()
	name := relayTestName(t, "acme", "presence-room.1")

	here := newWhisperTestInstance(t, "instance-one", bus)
	there := newWhisperTestInstance(t, "instance-two", bus)

	sender, senderSink := here.seat(t, "acme", "u2", "presence-room.1")
	_, neighbour := here.seat(t, "acme", "u1", "presence-room.1")
	_, remote := there.seat(t, "acme", "u3", "presence-room.1")

	waitFor(t, "both instances to relay the channel", func() bool {
		return bus.listeners(Topic(name)) == 2
	})

	// The frame names somebody else, and it is ignored: [ClientEvents.Accept]
	// reads the seat and never the frame.
	here.send(t, sender, `{"event":"client-typing","channel":"presence-room.1","user_id":"nobody","data":"{\"at\":1}"}`)

	want := `{"event":"client-typing","data":"{\"at\":1}","channel":"presence-room.1","user_id":"u2"}`
	waitFor(t, "the socket on the other instance to receive the client event", func() bool {
		return len(whisperTestReceived(t, remote)) > 0
	})
	// Nothing proves a negative. By the time the other instance has delivered,
	// the message has been round the bus -- which is the moment the instance it
	// came from would have delivered it a second time.
	time.Sleep(20 * time.Millisecond)

	if got := whisperTestReceived(t, remote); len(got) != 1 || got[0] != want {
		t.Fatalf("the socket on the other instance received %v, want the one frame %s", got, want)
	}
	// The sender's instance delivered it once, on the broadcast, and did not
	// deliver its own message a second time when it came back round the bus.
	if got := whisperTestReceived(t, neighbour); len(got) != 1 || got[0] != want {
		t.Fatalf("the socket on the sender's instance received %v, want the one frame %s", got, want)
	}
	// The sender drew its own message before it sent it.
	if got := whisperTestReceived(t, senderSink); len(got) != 0 {
		t.Fatalf("the sender was written its own client event back: %v", got)
	}

	// The bytes on the bus, which is where the user_id had to survive unchanged.
	// One message; the instance that sent it on the front, which is what stops
	// it being delivered twice there; no tenant in the payload, because the
	// [Topic] carries that; and the id the channel seated rather than the one the
	// frame claimed.
	carried := bus.published(Topic(name))
	wantCarried := `{"origin":"instance-one","event":"client-typing","channel":"presence-room.1","data":{"at":1},"socket":"acme.u2","user_id":"u2"}`
	if len(carried) != 1 || carried[0] != wantCarried {
		t.Fatalf("the fleet was sent %v, want the one message %s", carried, wantCarried)
	}

	// Nothing on the receiving instance was reached with a Grant built from any
	// of that: the channel there exists because a socket subscribed to it, under
	// a Grant of its own, and [Relay.deliver] takes the name from that
	// subscription. The remote instance is holding one channel and it is acme's.
	if joined := there.relay.Joined(); joined != 1 {
		t.Fatalf("the other instance relays %d channels, want the one its own socket subscribed to", joined)
	}
}
