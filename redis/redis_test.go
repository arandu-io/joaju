package redis_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arandu-io/hesape/redis/connections"
	"github.com/arandu-io/joaju"
	"github.com/arandu-io/joaju/redis"
)

// The tests below talk to a real server, because what this package claims is a
// claim about a protocol: that a PUBLISH on one connection reaches a SUBSCRIBE
// on another, that publishing works on a connection which is subscribed, and
// that a subscription is still there after its socket died. A fake that
// implements the happy path answers none of the three.
//
// The server is REDIS_ADDRESS, or 127.0.0.1:6379 when that is unset. When
// nothing answers there, the tests that need one skip and say what to start;
// the four that check refused arguments never reach a server and run anywhere.

// defaultAddress is where a RESP server is when nobody said otherwise.
const defaultAddress = "127.0.0.1:6379"

// wait bounds every wait for a message. It is long because it is a failure
// budget and not an expectation: delivery is a millisecond, and a test that
// fails at 200ms fails on a loaded machine rather than on a bug.
const wait = 10 * time.Second

// received is one message and the channel it arrived on, in the order the
// callback is handed them.
type received struct {
	message string
	channel string
}

// address is the server, or the reason there is none.
func address(t *testing.T) string {
	t.Helper()

	addr := os.Getenv("REDIS_ADDRESS")
	if addr == "" {
		addr = defaultAddress
	}

	probe := connections.Connect(connections.Config{
		Address:     addr,
		DialTimeout: time.Second,
		ReadTimeout: time.Second,
	})
	defer func() { _ = probe.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := probe.Ping(ctx); err != nil {
		t.Skipf("no RESP server answering at %s (%v): start one, or set REDIS_ADDRESS to another", addr, err)
	}

	return addr
}

// namespace is one key prefix per test AND per run.
//
// Per test alone is not enough: pub/sub keeps nothing, but a subscriber left
// running by a previous run of the same test is still attached to the server
// for as long as its socket is, and it would be counted by the publish that
// this run waits on.
func namespace(t *testing.T) string {
	t.Helper()

	return "joaju-test-" + strings.ReplaceAll(t.Name(), "/", "-") + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// connect opens a connection that closes with the test.
func connect(t *testing.T, addr, prefix string) *connections.Connection {
	t.Helper()

	conn := connections.Connect(connections.Config{Address: addr, Prefix: prefix})
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

// bus is one bus over one connection.
func bus(t *testing.T, addr, prefix string) joaju.Bus {
	t.Helper()

	b, err := redis.NewBus(connect(t, addr, prefix))
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}

	return b
}

// listen starts a subscription and hands back what it receives.
//
// The second channel carries what Subscribe answered, and the caller waits on
// it after cancelling: a test that returned while the subscription goroutine
// was still running would be a race detector finding this file rather than the
// code under it.
func listen(ctx context.Context, t *testing.T, b joaju.Bus, topic string) (<-chan received, <-chan error) {
	t.Helper()

	messages := make(chan received, 64)
	done := make(chan error, 1)

	go func() {
		done <- b.Subscribe(ctx, []string{topic}, func(message, channel string) {
			select {
			case messages <- received{message: message, channel: channel}:
			default:
			}
		})
	}()

	return messages, done
}

// awaitDelivery publishes payload until it arrives, and answers what arrived.
//
// It publishes more than once on purpose, and the repetition is the whole
// method: a subscription becomes live on the server at a moment no client can
// observe, and after a socket dies it becomes live again at another one. A
// message published into either gap reaches nobody, because pub/sub has no
// queue -- so the test that waits for one is the test that sleeps and hopes.
func awaitDelivery(ctx context.Context, t *testing.T, publisher joaju.Bus, topic, payload string, messages <-chan received, within time.Duration) received {
	t.Helper()

	deadline := time.Now().Add(within)
	for {
		if _, err := publisher.Publish(ctx, topic, payload); err != nil {
			t.Fatalf("Publish on %s: %v", topic, err)
		}

		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case got := <-messages:
			timer.Stop()
			if got.message == payload {
				return got
			}
		case <-timer.C:
		}

		if time.Now().After(deadline) {
			t.Fatalf("nothing carrying %q arrived on %s within %s", payload, topic, within)
		}
	}
}

// TestAPublishReachesASubscriberOnAnotherConnection is the whole point of the
// package: two instances of the server, each holding its own connection, and an
// event that leaves one and arrives at the other.
//
// The topic is a string here, and that is the other half of what is being
// checked. The tenant sits in the middle of it because [joaju.Topic] put it
// there from a Grant; nothing in this package reads it, splits on it
// or knows it is there, which is what keeps the tenant boundary in the one
// place that can enforce it.
func TestAPublishReachesASubscriberOnAnotherConnection(t *testing.T) {
	addr, prefix := address(t), namespace(t)
	publisher, subscriber := bus(t, addr, prefix), bus(t, addr, prefix)

	ctx, cancel := context.WithCancel(context.Background())
	topic := joaju.TopicPrefix + "acme:orders"

	messages, done := listen(ctx, t, subscriber, topic)
	defer func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Subscribe after cancel = %v, want context.Canceled", err)
		}
	}()

	got := awaitDelivery(ctx, t, publisher, topic, `{"event":"OrderPlaced"}`, messages, wait)

	// The name handed to the callback is the one that travelled, prefix and
	// all -- not the one passed to Subscribe. A caller matching on it has to
	// prefix, and this is where that stops being a claim in a comment.
	if want := prefix + ":" + topic; got.channel != want {
		t.Errorf("the message arrived on %q, want %q", got.channel, want)
	}

	// The count is what joaju.Relay waits on: it asks the fleet a question by
	// publishing it, and this is how it knows how many instances are listening
	// and therefore how many answers to expect.
	heard, err := publisher.Publish(ctx, topic, "counted")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if heard != 1 {
		t.Errorf("Publish counted %d subscribers, want 1", heard)
	}
}

// TestPublishAndSubscribeDoNotShareASocket is the reason one connection can be
// both halves, checked rather than assumed.
//
// A RESP connection in subscribe mode refuses every command that is not a
// subscribe, which is what usually forces a separate publisher and subscriber.
// This driver dials a socket of its own for each subscription, outside the pool
// the commands use, so one connection is both halves. If that ever stops being
// true, the publish below answers "ERR only
// (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed" and this
// test says so before a fleet does.
func TestPublishAndSubscribeDoNotShareASocket(t *testing.T) {
	addr, prefix := address(t), namespace(t)
	conn := connect(t, addr, prefix)

	both, err := redis.NewBus(conn)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	topic := joaju.TopicPrefix + "acme:orders"

	messages, done := listen(ctx, t, both, topic)
	defer func() {
		cancel()
		<-done
	}()

	awaitDelivery(ctx, t, both, topic, "published while subscribed", messages, wait)

	// And a command that is not pub/sub at all, on the same connection: the
	// subscribed socket is not the pool's, so the pool still answers.
	if err := conn.Ping(ctx); err != nil {
		t.Errorf("Ping while subscribed: %v", err)
	}
}

// TestASubscriptionSurvivesTheConnectionDying is the second decision: a fleet
// whose Redis restarts must not come back with instances that no longer hear
// each other.
//
// A socket dialled again is subscribed to nothing, so the channel set has to be
// re-sent on every connection and not only on the first. The driver here keeps
// that set and re-sends it, and this test is what says so: the
// subscriber reaches the server through a proxy the test can kill, the
// publisher does not, and after every proxied socket is closed the same topic
// delivers again -- without anything in this package having noticed.
func TestASubscriptionSurvivesTheConnectionDying(t *testing.T) {
	addr, prefix := address(t), namespace(t)

	broken := newProxy(t, addr)
	publisher := bus(t, addr, prefix)
	subscriber := bus(t, broken.address(), prefix)

	ctx, cancel := context.WithCancel(context.Background())
	topic := joaju.TopicPrefix + "acme:orders"

	messages, done := listen(ctx, t, subscriber, topic)
	defer func() {
		cancel()
		<-done
	}()

	awaitDelivery(ctx, t, publisher, topic, "before the outage", messages, wait)

	if dropped := broken.dropAll(); dropped == 0 {
		t.Fatal("the proxy was holding no connection to drop, so the subscriber was not reaching the server through it")
	}

	awaitDelivery(ctx, t, publisher, topic, "after the outage", messages, wait)
}

// TestNewBusRefusesANilConnection: an instance with no Redis hands
// joaju.NewRelay a nil Bus, which is a deployment it supports and reports. A
// bus over no connection is the same intention expressed where it becomes a
// panic in a subscription goroutine instead.
func TestNewBusRefusesANilConnection(t *testing.T) {
	if _, err := redis.NewBus(nil); err == nil {
		t.Fatal("NewBus(nil) succeeded, and the first publish would have dereferenced it")
	}
}

// TestSubscribeRefusesAnEmptyChannelList: the driver accepts one, subscribes to
// nothing and blocks, which from the outside is indistinguishable from a
// channel nobody is talking on.
func TestSubscribeRefusesAnEmptyChannelList(t *testing.T) {
	if err := unreachable(t).Subscribe(context.Background(), nil, func(_, _ string) {}); err == nil {
		t.Fatal("Subscribe with no channels succeeded, and would have blocked forever")
	}
}

// TestSubscribeRefusesANilCallback: it panics on the first message, in a
// goroutine started by the relay, and the first message may be hours away.
func TestSubscribeRefusesANilCallback(t *testing.T) {
	topic := joaju.TopicPrefix + "acme:orders"
	if err := unreachable(t).Subscribe(context.Background(), []string{topic}, nil); err == nil {
		t.Fatal("Subscribe with no callback succeeded")
	}
}

// TestPublishRefusesAnEmptyChannel: Redis would take it, deliver it to nobody
// and answer zero, which is a message lost by a call that reported success.
func TestPublishRefusesAnEmptyChannel(t *testing.T) {
	if _, err := unreachable(t).Publish(context.Background(), "", "{}"); err == nil {
		t.Fatal("Publish with no channel succeeded")
	}
}

// unreachable is a bus over a connection to nothing.
//
// The four tests above are refusals, and a refusal that needed a server would
// be a refusal nobody checks on a laptop. connections.Connect does not dial, so
// the address below is never asked for.
func unreachable(t *testing.T) joaju.Bus {
	t.Helper()

	b, err := redis.NewBus(connections.Connect(connections.Config{Address: "127.0.0.1:1"}))
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}

	return b
}

// proxy is a TCP relay in front of the server that the test can kill.
//
// It is how an outage is staged without asking the server to stage it: CLIENT
// KILL is an administrative command that a managed Redis may refuse and that
// the other three RESP products implement with their own filters, and a test
// that skipped where it was refused would be a test that never runs. Closing a
// socket is the same event to the client and needs nothing from the server.
type proxy struct {
	listener net.Listener

	mu    sync.Mutex
	pairs []net.Conn
}

// newProxy starts one in front of target, and stops it with the test.
func newProxy(t *testing.T, target string) *proxy {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening for the proxy: %v", err)
	}

	p := &proxy{listener: listener}
	go p.accept(target)

	t.Cleanup(func() {
		_ = listener.Close()
		p.dropAll()
	})

	return p
}

// address is what a connection is pointed at to reach the server through this.
func (p *proxy) address() string { return p.listener.Addr().String() }

// accept dials the server once per client and copies in both directions until
// one side is closed.
func (p *proxy) accept(target string) {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}

		server, err := net.Dial("tcp", target)
		if err != nil {
			_ = client.Close()
			continue
		}

		p.mu.Lock()
		p.pairs = append(p.pairs, client, server)
		p.mu.Unlock()

		go copyUntilClosed(client, server)
		go copyUntilClosed(server, client)
	}
}

// dropAll closes every socket the proxy is holding and answers how many pairs
// went with it. That is the outage: to the client it is the server going away.
func (p *proxy) dropAll() int {
	p.mu.Lock()
	held := p.pairs
	p.pairs = nil
	p.mu.Unlock()

	for _, conn := range held {
		_ = conn.Close()
	}

	return len(held) / 2
}

// copyUntilClosed moves bytes one way and closes both ends when it stops, so a
// dropped socket takes its partner with it rather than leaving a half-open one
// the client believes in.
func copyUntilClosed(to, from net.Conn) {
	_, _ = io.Copy(to, from)
	_ = to.Close()
	_ = from.Close()
}
