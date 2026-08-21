package joaju

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
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

// The fixture below is a second one, and tests/Feature/server_test.go's is the
// first. It is not a duplicate by choice: that file is a test of the exported
// surface and this one is the internal binary, so nothing there can see the
// in-memory [Bus] declared here -- and a bus, which has to stand in for Redis
// pub/sub for two instances at once, is much the harder of the two to be asked
// to write twice. So the bus stays and the smaller half is restated: a Broker
// over a map, a Protocol that does nothing, and the middleware every route
// needs.

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
func (fleetTestProtocol) Refuse(Refusal) []byte                              { return nil }

// Routes are none. This file's tests reach the fleet through [Server.Fleet] and
// the registry through [Server.Connections], which is where the two halves of a
// metrics route come from -- the route that adds them is the protocol's, and a
// protocol is what the package this file is in may not import.
func (fleetTestProtocol) Routes(API) http.Handler { return nil }

// fleetTestInstance is one joaju process: a relay on the shared bus, and the
// server attached to it.
type fleetTestInstance struct {
	server *Server
	relay  *Relay
	log    *relayTestLog
	// broker is the one the server was built with, kept so that a test can hand
	// the fleet an event the way the events route does. It is a [Carrier]
	// because [RelayedBroker] made it one.
	broker Broker
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

	broker := RelayedBroker(newFleetTestBroker(channels...), relay)
	server, err := NewServer(ServerConfig{
		AppID:          fleetTestAppID,
		AppKey:         fleetTestAppKey,
		Broker:         broker,
		Connect:        connTestConnectPolicy{},
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

	return &fleetTestInstance{server: server, relay: relay, log: log, broker: broker}
}

// hold registers one socket of user, which is what the socket route does once
// the handshake is authorized.
func (i *fleetTestInstance) hold(t *testing.T, tenant, user string) {
	t.Helper()

	if err := i.server.register(connFor(t, tenant, user)); err != nil {
		t.Fatalf("registering the socket of %s of %s: %v", user, tenant, err)
	}
}

// ask is one of the four metrics routes' second half: what the rest of the
// fleet answers about the tenant.
//
// The route that adds this to what the instance holds itself is the protocol's
// and lives with the protocol, which is a package this file may not import.
// What is proved here is the half underneath it -- the question, the timeout
// and the tenant -- and the addition is proved where the route is.
func (i *fleetTestInstance) ask(t *testing.T, tenant, channel string) FleetTally {
	t.Helper()

	return i.server.Fleet(context.Background(), relayTestGrant(t, tenant, broadcasting.ChannelJoin), channel)
}

// held is how many sockets this instance holds for the tenant, which is the
// other half of the same route.
func (i *fleetTestInstance) held(t *testing.T, tenant string) int {
	t.Helper()

	open, err := i.server.Connections(relayTestGrant(t, tenant, Connect))
	if err != nil {
		t.Fatalf("counting the sockets of %s: %v", tenant, err)
	}

	return open
}

// publish is what the events route does once its policies have run: the local
// delivery, and then the fleet, in that order and never the other way round.
func (i *fleetTestInstance) publish(t *testing.T, channel Channel, e Event) {
	t.Helper()

	if err := channel.Broadcast(context.Background(), e); err != nil {
		t.Fatalf("broadcasting %s on %s: %v", e.Name, e.Channel, err)
	}

	carrier, ok := i.broker.(Carrier)
	if !ok {
		t.Fatal("the instance's broker does not carry, and RelayedBroker is what makes it one")
	}
	carrier.Carry(context.Background(), e)
}

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

	first.publish(t, here, Event{
		Name:    "order.paid",
		Channel: mine,
		Data:    json.RawMessage(`{"id":17}`),
		Socket:  SocketID("1234.5678"),
	})

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

	// A Redis that is down costs the other instances and costs the publish
	// nothing: the sockets this instance holds are still served, and a client
	// whose event was delivered may not be told that it was not. Carry answers
	// nothing at all, which is the shape of that decision.
	alone.publish(t, channel, Event{
		Name:    "order.paid",
		Channel: name,
		Data:    json.RawMessage(`{"id":17}`),
	})

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

func TestTheFleetAnswersForEveryInstanceAndNotOneProcess(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()
	orders := relayTestName(t, "acme", "presence-orders.17")
	invoices := relayTestName(t, "acme", "invoices.9")

	// Bruno has a tab on each instance. He is two subscriptions and one member,
	// which is the difference between the two sums a metrics route does.
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
	// tally, it would tell a customer they are talking on fewer channels than
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

	// The two halves a metrics route adds. This one holds two sockets and the
	// other three, and neither number is the whole of what the customer has.
	if held := first.held(t, "acme"); held != 2 {
		t.Fatalf("this instance holds %d sockets, want the two it was given", held)
	}

	whole := first.ask(t, "acme", "")
	if whole.Connections != 3 {
		t.Fatalf("the fleet answered %d sockets, want the three the other instance holds", whole.Connections)
	}

	one := first.ask(t, "acme", "presence-orders.17").Channel("presence-orders.17")
	if one.Subscriptions != 3 {
		t.Fatalf("the fleet answered %d subscriptions, want the three on the other instance", one.Subscriptions)
	}
	// Members travel as ids and not as a count, and this is why: added to the
	// two people this instance holds they are three, and summed they would be
	// four -- bruno counted once on each instance.
	if got := one.Members(); !slices.Equal(got, []string{"bruno", "carla"}) {
		t.Fatalf("the fleet answered the members %v, want bruno and carla once each", got)
	}

	// The tally is keyed by the name the client asked for, with no tenant in
	// it: the caller asked about its own channels and the tenant they are held
	// under is not its to read back.
	if strings.Contains(fmt.Sprint(whole.Channels), "acme:") {
		t.Fatalf("the fleet's channels carried the tenant: %v", whole.Channels)
	}
	if _, answered := whole.Channels["invoices.9"]; !answered {
		t.Fatalf("the fleet answered about %v, want the channel only the other instance holds among them", whole.Channels)
	}
	if got := whole.Channels["invoices.9"]; got.Subscriptions != 1 {
		t.Fatalf("the channel only the other instance holds answered %+v, want the one subscription on it", got)
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
	one := first.ask(t, "acme", "orders.17").Channel("orders.17")
	took := time.Since(started)

	if one.Subscriptions != 1 {
		t.Fatalf("the fleet answered %+v, want the one subscription of the one instance that answered", one)
	}
	if took < wait {
		t.Fatalf("the question came back in %s, which is less than the %s it should have waited for the quiet instance", took, wait)
	}
	if took > 5*time.Second {
		t.Fatalf("the question took %s to come back, and an instance that stopped answering held it open", took)
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

	// The other instance holds three sockets and two members, all of them
	// globex's. Every one of these numbers is acme's alone: a sum that crosses
	// a tenant is a leak with a number on it.
	if held := first.held(t, "acme"); held != 1 {
		t.Fatalf("acme holds %d sockets here, want its own one", held)
	}

	whole := first.ask(t, "acme", "")
	if whole.Connections != 0 {
		t.Fatalf("the fleet answered acme with %d sockets, and every one open elsewhere is globex's", whole.Connections)
	}
	if len(whole.Channels) != 0 {
		t.Fatalf("the fleet answered acme about %v, and the channel of that name elsewhere is globex's", whole.Channels)
	}

	one := first.ask(t, "acme", "presence-orders.17").Channel("presence-orders.17")
	if one.Subscriptions != 0 {
		t.Fatalf("the fleet answered %d subscriptions on acme's channel, want none: the ones under that name elsewhere are globex's", one.Subscriptions)
	}
	if got := one.Members(); len(got) != 0 {
		t.Fatalf("acme was handed the members %v, and gina and hugo are globex's", got)
	}
}

func TestFleetAnswersNothingForAGrantThatCarriesNoTenant(t *testing.T) {
	t.Parallel()

	bus := newRelayTestBus()

	orders := relayTestName(t, "acme", "presence-orders.17")
	here := &relayTestChannel{name: orders, subscribers: []Subscriber{{Member: Member{UserID: "ana"}}}}
	there := &relayTestChannel{name: orders, subscribers: []Subscriber{
		{Member: Member{UserID: "bruno"}},
		{Member: Member{UserID: "carla"}},
	}}

	first := newFleetTestInstance(t, "instance-one", bus, time.Second, here)
	second := newFleetTestInstance(t, "instance-two", bus, time.Second, there)

	first.hold(t, "acme", "ana")
	second.hold(t, "acme", "bruno")
	second.hold(t, "acme", "carla")

	fleetTestListening(t, bus, "instance-one", 2)

	// The tenant is the only filter a fleet answer has, and it comes off the
	// Grant. A Grant with none is not a Grant for everybody: there is no tenant
	// to compare an answer against, so nothing can be added without crossing a
	// customer.
	subject := auth.Subject{ID: "reader"}
	nowhere, err := auth.Authorize(context.Background(), relayTestPolicy{}, subject, broadcasting.ChannelJoin, Subscription{})
	if err != nil {
		t.Fatalf("authorizing a subject with no tenant: %v", err)
	}
	if auth.Tenant(nowhere) != "" {
		t.Fatalf("the grant carries the tenant %q, and this test needs one carrying none", auth.Tenant(nowhere))
	}

	whole := first.server.Fleet(context.Background(), nowhere, "")
	if whole.Connections != 0 || len(whole.Channels) != 0 {
		t.Fatalf("a grant with no tenant was answered %+v, want nothing", whole)
	}

	one := first.server.Fleet(context.Background(), nowhere, "presence-orders.17").Channel("presence-orders.17")
	if one.Subscriptions != 0 || len(one.Members()) != 0 {
		t.Fatalf("a grant with no tenant was answered %+v about one channel, want nothing", one)
	}

	// The same question with acme's tenant on it is answered, so what the two
	// assertions above prove is the tenant and not a fleet that was silent.
	held := relayTestGrant(t, "acme", broadcasting.ChannelJoin)
	if got := first.server.Fleet(context.Background(), held, ""); got.Connections != 2 {
		t.Fatalf("acme was answered %+v, want the two sockets the other instance holds", got)
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

	// A degraded instance does not ask, so the fleet's half is empty and the
	// route is answered out of the instance's own state -- which is what it
	// held before there was a fleet at all.
	if got := alone.ask(t, "acme", "orders.17"); got.Connections != 0 || len(got.Channels) != 0 {
		t.Fatalf("a degraded instance answered %+v about the fleet, want nothing: it cannot reach one", got)
	}
	if held := alone.held(t, "acme"); held != 1 {
		t.Fatalf("a degraded instance counted %d sockets, want the one it holds", held)
	}
	if got := len(channel.Connections()); got != 1 {
		t.Fatalf("a degraded instance reads %d subscriptions off its own Broker, want the one it holds", got)
	}

	if took := time.Since(started); took > 10*time.Second {
		t.Fatalf("a degraded instance spent %s on two questions, and it has nothing to wait for", took)
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

	var tally FleetTally

	tally.add(relay.log, relay.ID(), "acme", fleetAnswer{
		Origin:      "instance-two",
		Request:     "instance-one.whatever",
		Tenant:      "globex",
		Connections: 9,
		Channels: map[string]channelAnswer{
			"presence-orders.17": {Subscriptions: 9, Users: []string{"gina"}},
		},
	})

	if tally.Connections != 0 || len(tally.Channels) != 0 {
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

	if tally.Connections != 0 {
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

	if tally.Connections != 4 {
		t.Fatalf("the fleet's sockets counted %d, want 4", tally.Connections)
	}
	if got := tally.Channel("presence-orders.17"); got.Subscriptions != 2 {
		t.Fatalf("the fleet's subscriptions counted %d, want 2", got.Subscriptions)
	}
	if got := tally.Channel("presence-orders.17").Members(); len(got) != 1 || got[0] != "ana" {
		t.Fatalf("the fleet's members are %v, want the one person named twice and no empty id", got)
	}
	if got := tally.Channel("invoices.9"); got.Subscriptions != 0 || got.Users != nil {
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
