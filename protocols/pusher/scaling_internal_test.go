package pusher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/arandu-io/joaju"
)

// This file is the protocol across a fleet: one client event, two instances,
// and the bus between them. Everything else about relaying is the root
// package's, which is where [joaju.Relay] and its tests are -- what is proved
// here is the half that only exists once a [joaju.Protocol] is on both ends.

// whisperTestBus is a [joaju.Bus] in memory: one process standing in for Redis,
// so that two relays can be run in one test.
type whisperTestBus struct {
	mu          sync.Mutex
	subscribers map[string][]chan string
	// carried is every payload that crossed, keyed by topic and in order. What
	// two instances say to each other is a wire format like any other, and this
	// is where a test reads the bytes of it.
	carried map[string][]string
}

func newWhisperTestBus() *whisperTestBus {
	return &whisperTestBus{
		subscribers: make(map[string][]chan string),
		carried:     make(map[string][]string),
	}
}

func (b *whisperTestBus) Publish(_ context.Context, channel string, message any) (int64, error) {
	payload, _ := message.(string)

	b.mu.Lock()
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

func (b *whisperTestBus) Subscribe(ctx context.Context, channels []string, callback func(message, channel string)) error {
	messages := make(chan string, 64)

	b.mu.Lock()
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
func (b *whisperTestBus) published(topic string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]string(nil), b.carried[topic]...)
}

func (b *whisperTestBus) listeners(topic string) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.subscribers[topic])
}

// whisperTestWaitFor polls until the condition holds, and fails the test if it
// never does. What is being waited for happens on a relay's own goroutine.
func whisperTestWaitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}

	t.Fatalf("waited for %s and it did not happen", what)
}

// whisperTestInstance is one process a client event has to cross: the Pusher
// protocol over a Broker that is joined to the fleet.
//
// There is no [joaju.Server] on it: a client event arrives as a frame and is
// answered by the [joaju.Protocol], so no route is on this path. The protocol
// is the real one because nothing else produces a client- event, and the Broker
// is the real one because what a receiver reads is written by a [channel] to
// the sockets it seated.
type whisperTestInstance struct {
	protocol joaju.Protocol
	relay    *joaju.Relay
}

func newWhisperTestInstance(t *testing.T, id joaju.InstanceID, bus joaju.Bus) *whisperTestInstance {
	t.Helper()

	relay, err := joaju.NewRelay(context.Background(), id, bus,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("building the relay of %s: %v", id, err)
	}
	t.Cleanup(func() { _ = relay.Close() })

	// One value, handed to the protocol -- which is what [joaju.NewServer]
	// refuses a server for not doing, and what puts the relay within the
	// protocol's reach.
	broker := joaju.RelayedBroker(NewMemoryBroker(), relay)

	return &whisperTestInstance{
		protocol: NewPusher(broker, channelTestJoinPolicy{}, PusherConfig{ClientEvents: ClientEventsOn}),
		relay:    relay,
	}
}

// seat opens a socket on this instance and subscribes it as user, with the frame
// a browser sends.
func (i *whisperTestInstance) seat(t *testing.T, tenant, user, requested string) (*joaju.Connection, *channelTestSink) {
	t.Helper()

	conn, sink := channelTestConnection(t, tenant, user)
	i.send(t, conn, `{"event":"pusher:subscribe","data":{"channel":"`+requested+
		`","channel_data":"{\"user_id\":\"`+user+`\"}"}}`)

	return conn, sink
}

// send hands one frame to this instance, as the goroutine reading that socket
// does.
func (i *whisperTestInstance) send(t *testing.T, conn *joaju.Connection, frame string) {
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
// It used to stop at [joaju.Channel.Broadcast], so two browsers on one channel
// saw each other type only while the same process happened to hold both -- the
// defect the events API had fixed on the half that was measured first. What a
// receiver draws is "u2 is typing", so the sender's user_id has to survive the
// bus: it is stamped from the seat the channel holds on the instance that took
// the frame, and no other instance has a seat for that socket to read it off.
func TestAClientEventCrossesTheFleetCarryingTheSendersUserID(t *testing.T) {
	t.Parallel()

	bus := newWhisperTestBus()
	name := channelTestName(t, "acme", "presence-room.1")

	here := newWhisperTestInstance(t, "instance-one", bus)
	there := newWhisperTestInstance(t, "instance-two", bus)

	sender, senderSink := here.seat(t, "acme", "u2", "presence-room.1")
	_, neighbour := here.seat(t, "acme", "u1", "presence-room.1")
	_, remote := there.seat(t, "acme", "u3", "presence-room.1")

	whisperTestWaitFor(t, "both instances to relay the channel", func() bool {
		return bus.listeners(joaju.Topic(name)) == 2
	})

	// The frame names somebody else, and it is ignored: [ClientEvents.Accept]
	// reads the seat and never the frame.
	here.send(t, sender, `{"event":"client-typing","channel":"presence-room.1","user_id":"nobody","data":"{\"at\":1}"}`)

	want := `{"event":"client-typing","data":"{\"at\":1}","channel":"presence-room.1","user_id":"u2"}`
	whisperTestWaitFor(t, "the socket on the other instance to receive the client event", func() bool {
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
	// [joaju.Topic] carries that; and the id the channel seated rather than the
	// one the frame claimed.
	carried := bus.published(joaju.Topic(name))
	wantCarried := `{"origin":"instance-one","event":"client-typing","channel":"presence-room.1","data":{"at":1},"socket":"acme.u2","user_id":"u2"}`
	if len(carried) != 1 || carried[0] != wantCarried {
		t.Fatalf("the fleet was sent %v, want the one message %s", carried, wantCarried)
	}

	// Nothing on the receiving instance was reached with a Grant built from any
	// of that: the channel there exists because a socket subscribed to it, under
	// a Grant of its own, and the relay takes the name from that subscription.
	// The remote instance is holding one channel and it is acme's.
	if joined := there.relay.Joined(); joined != 1 {
		t.Fatalf("the other instance relays %d channels, want the one its own socket subscribed to", joined)
	}
}
