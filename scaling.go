package joaju

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
)

// DefaultRetryInterval is how long a [Relay] waits before opening a
// subscription again after the one it had was lost.
//
// It is a constant and not a knob because a knob would be a second way to
// answer "how fast does this reconnect" (RULE 9). A second is short enough that
// a Redis restart costs one message and long enough that a Redis that is down
// is not asked a thousand times a second by every instance at once.
const DefaultRetryInterval = time.Second

// TopicPrefix is what every Redis pub/sub channel this package publishes on
// begins with.
//
// It is there so that relay traffic is distinguishable from everything else on
// the same Redis -- in particular from what hesape's RedisBroadcaster publishes,
// which is an application talking to a relay and not two relays talking to each
// other. The two carry different payloads, and a subscriber that heard both
// would decode one of them wrong.
const TopicPrefix = "joaju:"

// ErrRelayClosed is what a [Relay] answers once [Relay.Close] has been called.
var ErrRelayClosed = errors.New("joaju: the relay is closed")

// InstanceID identifies one joaju process within a fleet of them.
//
// It is the whole of the deduplication. Redis pub/sub has no notion of a
// publisher, so an instance that publishes on a topic it is subscribed to hears
// its own message come back -- and it has already delivered that message to its
// own connections, which is what publishing was in aid of. Every relayed
// message carries the id of the instance that sent it, and an instance drops the
// ones carrying its own: without that, every message would reach half the fleet
// once and the instance it came from twice.
//
// It has to differ between instances and nothing here can check that. A
// hostname, a pod name or a random string minted at start-up are all fine; a
// constant compiled into the binary is not, because then no instance can tell
// its own traffic from its neighbour's and the deduplication silently drops
// every message in the fleet.
type InstanceID string

// Bus is the little of a Redis connection that horizontal scaling needs.
//
// The two methods are github.com/arandu-io/hesape/redis/connections.Connection's
// Publish and Subscribe, signature for signature, so that a *connections.Connection
// satisfies this by having been written -- there is no adapter to keep in step.
//
// It is declared here rather than imported for the reason the same shape is
// declared in hesape's broadcasters package: github.com/arandu-io/hesape/redis
// is a separate module, because the driver beneath it is a third-party
// dependency and Go has no optional dependency (ADR 0048). Importing it would
// put that driver in this repository's go.mod, and a graph with one entry in it
// is what forking gorilla/websocket bought (ADR 0052). Stating the contract and
// letting the application pass its connection in costs nothing and keeps the
// graph.
//
// Publish and Subscribe both prefix the channel name with the connection's own
// key prefix, on both sides, which is why nothing here compensates for it and
// why the name handed to the callback is not the name passed to Subscribe.
type Bus interface {
	// Publish sends one message to one channel and answers how many subscribers
	// received it. It is the PUBLISH command.
	Publish(ctx context.Context, channel string, message any) (int64, error)
	// Subscribe listens on a set of channels and calls callback for each
	// message, with the message first and the channel it arrived on second. It
	// blocks until ctx is cancelled or the connection is lost.
	Subscribe(ctx context.Context, channels []string, callback func(message, channel string)) error
}

// Topic is the Redis pub/sub channel a joaju channel is relayed on.
//
// It is [TopicPrefix] followed by [ChannelName.String], so the tenant is in the
// middle of it -- "joaju:acme:orders" and "joaju:globex:orders" are two topics,
// and two customers who both called a channel "orders" never hear each other.
// That is RULE 14 reaching the one place where a channel name leaves this
// process, and it holds by construction: a [ChannelName] cannot be built
// without a Grant to read the tenant off.
//
// The zero [ChannelName] has no topic and answers the empty string, which
// [Relay.Join] and [Relay.Publish] refuse before they get here.
func Topic(name ChannelName) string {
	if name.IsZero() {
		return ""
	}

	return TopicPrefix + name.String()
}

// relayMessage is what one instance sends the others: the [Event] as bytes,
// with the sender's [InstanceID] on it.
//
// It is not the Pusher frame. The frame is what a browser reads and is built by
// the protocol layer at the moment of delivery; this is what two servers say to
// each other, and the difference matters in one field -- Channel is
// [ChannelName.Requested], never [ChannelName.String], because the tenant is
// already in the topic and putting it in the payload as well would create a
// second copy for the two to disagree about.
type relayMessage struct {
	// Origin is the instance that published this. A message carrying the
	// reader's own id has already been delivered by that reader and is dropped.
	Origin InstanceID `json:"origin"`
	// Event is [Event.Name].
	Event string `json:"event"`
	// Channel is [ChannelName.Requested]: the name without the tenant.
	Channel string `json:"channel"`
	// Data is [Event.Data], still encoded, still unread.
	Data json.RawMessage `json:"data,omitempty"`
	// Socket is [Event.Socket]: the connection that published, which does not
	// receive its own message.
	//
	// It travels because the exclusion is fleet-wide. A publish through the
	// events API can name a socket that is held by any instance, and the one
	// holding it is not usually the one that took the request.
	Socket SocketID `json:"socket,omitempty"`
}

// Relay is horizontal scaling: it carries what this instance received to the
// other instances, and delivers what they received to this instance's
// connections.
//
// It is the shape ADR 0052 settles on, and it is Reverb's: every server
// publishes on Redis pub/sub, every server subscribes, and no server has to know
// which of the others holds a given socket. One topic per channel per tenant --
// see [Topic] -- so an instance receives only the traffic of channels it is
// actually holding.
//
// The flow of one message, and it is worth reading in order, because the local
// delivery is not this type's job:
//
//  1. a client publishes on a channel this instance holds;
//  2. the caller delivers it locally with [Channel.Broadcast], which skips the
//     socket that sent it;
//  3. the caller hands the same [Event] to [Relay.Publish];
//  4. every instance holding that channel receives it, this one included;
//  5. this one drops it, on the [InstanceID]; the others deliver it.
//
// Step three is called by the code that received the event from outside, and
// never by the code in step four. A relay that republished what it was relayed
// would be a fleet talking to itself forever.
//
// A Relay is safe for concurrent use, and it is meant to be: one connection is
// one goroutine, and each of them can publish.
type Relay struct {
	// id is this instance, and is what deduplication compares.
	id InstanceID
	// bus is Redis, or nil for an instance deliberately alone.
	bus Bus
	// log is where degradation is reported. Never nil.
	log *slog.Logger
	// root is the server's lifetime, and every subscription hangs off it. A
	// subscription outlives the request that started it by design, so it cannot
	// borrow that request's context.
	root context.Context
	// cancel ends root, and with it every subscription.
	cancel context.CancelFunc
	// retry is the wait between attempts at a lost subscription.
	retry time.Duration

	// degraded says Redis is unreachable. It is atomic rather than guarded by
	// mu because every delivered message reads it, and because the transition
	// is what gets logged -- CompareAndSwap reports the transition and nothing
	// else has to.
	degraded atomic.Bool

	// transition guards the pair (write the line, publish the flag) in
	// [Relay.fail] and [Relay.recovered]. It is not mu: this is the rare path,
	// and holding the subscription lock to write a log line would put every
	// join behind an outage report.
	transition sync.Mutex

	// mu guards the two fields below.
	mu sync.Mutex
	// joined is one entry per channel this instance relays, keyed by [Topic],
	// holding the cancel that ends its subscription.
	joined map[string]context.CancelFunc
	// closed is set by Close.
	closed bool
	// wg counts the running subscriptions, so Close can wait for them.
	wg sync.WaitGroup
}

// NewRelay is the only way to a [Relay].
//
// ctx is the server's lifetime and not a request's: closing it stops every
// subscription, which is what [Relay.Close] does through it.
//
// bus may be nil, and that is a supported deployment rather than a mistake: one
// instance has nobody to relay to. It starts degraded and says so once, because
// an operator who meant to configure Redis and did not needs to read that in the
// log rather than infer it from clients on different instances not seeing each
// other.
//
// log may be nil, in which case slog.Default is used.
func NewRelay(ctx context.Context, id InstanceID, bus Bus, log *slog.Logger) (*Relay, error) {
	if ctx == nil {
		return nil, errors.New("joaju: a relay needs the server's context")
	}
	if id == "" {
		return nil, errors.New("joaju: a relay needs an instance id, or it cannot tell its own messages from its neighbours'")
	}
	if log == nil {
		log = slog.Default()
	}

	ctx, cancel := context.WithCancel(ctx)
	r := &Relay{
		id:     id,
		bus:    bus,
		log:    log,
		root:   ctx,
		cancel: cancel,
		retry:  DefaultRetryInterval,
		joined: make(map[string]context.CancelFunc),
	}

	if bus == nil {
		r.degraded.Store(true)
		r.log.Warn("joaju: the relay has no message bus; this instance serves its own connections and no others",
			"instance", string(id))
	}

	return r, nil
}

// ID is which instance this is.
func (r *Relay) ID() InstanceID { return r.id }

// Degraded reports whether this instance is currently unable to reach the bus.
//
// A degraded instance still works: it accepts connections, authorizes
// subscriptions and delivers to the sockets it holds. What it has lost is the
// other instances, so a client on this one and a client on another stop hearing
// each other. That is worth answering GET /up with and worth putting on the
// metrics route, because from inside one browser it is indistinguishable from a
// quiet channel.
func (r *Relay) Degraded() bool { return r.degraded.Load() }

// Join starts relaying a channel: from now on, what the other instances publish
// on it is delivered to ch.
//
// g must be a Grant issued for broadcasting.ChannelJoin, and its tenant must be
// the tenant of ch's name, or [ErrWrongTenant]. That is the same Grant the
// [SubscriptionPolicy] answered for the first subscriber, and asking for it here
// is not ceremony: this is the call that opens a pipe from every other instance
// into this channel, and a pipe nobody authorized is RULE 17 with a network in
// the middle of it.
//
// It takes no context because it starts I/O rather than doing any -- the
// subscription runs on the lifetime the relay was built with. It is idempotent:
// a channel already joined is left as it is, since the first subscriber is what
// brings a channel into existence and every later one finds it already relayed.
//
// A relay with no bus, or one that cannot reach the bus, joins successfully.
// Refusing here would turn a Redis outage into every subscription failing, when
// what an outage actually costs is the other instances.
func (r *Relay) Join(g auth.Grant, ch Channel) error {
	if ch == nil {
		return errors.New("joaju: the relay was given no channel to join")
	}

	name := ch.Name()
	if name.IsZero() {
		return errors.New("joaju: the relay was given a channel with no name")
	}
	if err := g.Check(broadcasting.ChannelJoin); err != nil {
		return fmt.Errorf("joaju: %w", err)
	}
	if auth.Tenant(g) != name.Tenant() {
		return ErrWrongTenant
	}

	topic := Topic(name)

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRelayClosed
	}
	if _, already := r.joined[topic]; already {
		return nil
	}

	ctx, cancel := context.WithCancel(r.root)
	r.joined[topic] = cancel

	if r.bus == nil {
		return nil
	}

	r.wg.Add(1)
	go r.listen(ctx, topic, ch)

	return nil
}

// Leave stops relaying a channel.
//
// The caller is whatever removed the channel -- [Broker.Remove], when the last
// subscriber left. It takes no Grant for the reason [Channel.Unsubscribe] takes
// none: leaving discloses nothing.
//
// A channel that was never joined is not an error. It is what a broker that
// removes a channel unconditionally will do, and there is nothing to report.
func (r *Relay) Leave(name ChannelName) error {
	if name.IsZero() {
		return errors.New("joaju: the relay was given a channel with no name")
	}

	topic := Topic(name)

	r.mu.Lock()
	cancel, joined := r.joined[topic]
	delete(r.joined, topic)
	r.mu.Unlock()

	if joined {
		cancel()
	}

	return nil
}

// Joined reports how many channels this instance is relaying. It answers the
// metrics route, and it is what a test asserts on.
func (r *Relay) Joined() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.joined)
}

// Publish sends an event to the other instances.
//
// It is called once per event, by whatever received that event from outside --
// a client frame, or POST /apps/{appId}/events -- and after the local delivery,
// not instead of it. This instance's own connections are served by
// [Channel.Broadcast]; this call is only about the other instances.
//
// A bus that cannot be reached is not an error and does not come back as one.
// The event has already been delivered to every socket this instance holds, so
// there is nothing to fail: what was lost is reach, and reach is reported by
// [Relay.Degraded] and by one log line at the moment it goes. Returning an error
// would make a client whose message was delivered read that it was not, and
// would make every publish in the fleet fail for as long as Redis was down --
// which is exactly the outage the caller is being protected from.
//
// What does come back as an error is a malformed event: no channel, no name, or
// data that will not encode. Those are bugs in the caller and no amount of Redis
// fixes them.
func (r *Relay) Publish(ctx context.Context, e Event) error {
	if e.Channel.IsZero() {
		return errors.New("joaju: the relay was given an event with no channel")
	}
	if e.Name == "" {
		return errors.New("joaju: the relay was given an event with no name")
	}

	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()

	if closed {
		return ErrRelayClosed
	}
	if r.bus == nil {
		return nil
	}

	document, err := json.Marshal(relayMessage{
		Origin:  r.id,
		Event:   e.Name,
		Channel: e.Channel.Requested(),
		Data:    e.Data,
		Socket:  e.Socket,
	})
	if err != nil {
		return fmt.Errorf("joaju: encoding %s for the fleet: %w", e.Name, err)
	}

	topic := Topic(e.Channel)
	if _, err := r.bus.Publish(ctx, topic, string(document)); err != nil {
		r.fail("publish", e.Channel, err)
		return nil
	}
	r.recovered()

	return nil
}

// Close stops every subscription and waits for them.
//
// It is idempotent, and after it every [Relay.Join] and [Relay.Publish] answers
// [ErrRelayClosed] rather than working on a relay whose goroutines are gone.
func (r *Relay) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.joined = make(map[string]context.CancelFunc)
	r.mu.Unlock()

	r.cancel()
	r.wg.Wait()

	return nil
}

// listen holds one subscription open, and opens it again when it is lost.
//
// Bus.Subscribe blocks until the context is cancelled or the connection dies,
// so returning is either the end of this channel's life or a failure. The first
// is told from the second by the context, and only the second waits and tries
// again -- a loop that did not check would spin as fast as the scheduler allows
// the moment a channel is left.
func (r *Relay) listen(ctx context.Context, topic string, ch Channel) {
	defer r.wg.Done()

	name := ch.Name()

	for {
		err := r.bus.Subscribe(ctx, []string{topic}, func(message, _ string) {
			r.deliver(ch, name, message)
		})
		if ctx.Err() != nil {
			return
		}

		r.fail("subscribe", name, err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(r.retry):
		}
	}
}

// deliver hands one relayed message to this instance's connections.
//
// name is the [ChannelName] this subscription was joined with, and it is the
// name the delivered [Event] carries. That is the point of taking it from here
// rather than from the message: a name that came off the wire would be a tenant
// that came off the wire, and there is deliberately no constructor that would
// build one. The name in the payload is compared against this one and nothing
// more.
//
// Delivery is [Channel.Broadcast] and not [Channel.BroadcastToAll], because the
// exclusion the publisher asked for is fleet-wide: a publish through the events
// API names a socket that may be held anywhere, and this instance is where it
// might be.
func (r *Relay) deliver(ch Channel, name ChannelName, payload string) {
	// A message arriving is proof the bus is reachable, and it is the only
	// proof a subscriber ever gets -- Bus.Subscribe reports the connection by
	// blocking, not by answering.
	r.recovered()

	var message relayMessage
	if err := json.Unmarshal([]byte(payload), &message); err != nil {
		r.log.Warn("joaju: the relay dropped a message it could not decode",
			"tenant", name.Tenant(), "channel", name.Requested(), "error", err)
		return
	}

	if message.Origin == r.id {
		return
	}
	if message.Origin == "" {
		// Nothing in the fleet publishes without an origin, so this came from
		// something else on the same topic. It cannot be deduplicated, and a
		// message that might be this instance's own is a message delivered
		// twice.
		r.log.Warn("joaju: the relay dropped a message that names no instance of origin",
			"tenant", name.Tenant(), "channel", name.Requested())
		return
	}
	if message.Channel != name.Requested() {
		r.log.Warn("joaju: the relay dropped a message addressed to another channel",
			"tenant", name.Tenant(), "channel", name.Requested(), "addressed", message.Channel)
		return
	}

	event := Event{
		Name:    message.Event,
		Channel: name,
		Data:    message.Data,
		Socket:  message.Socket,
	}
	if err := ch.Broadcast(r.root, event); err != nil {
		r.log.Error("joaju: the relay could not deliver a relayed message to this instance's connections",
			"tenant", name.Tenant(), "channel", name.Requested(), "event", message.Event, "error", err)
	}
}

// fail records that the bus is unreachable, and logs it once.
//
// Once is the whole design of it. A Redis that is down is a Redis that fails
// every publish, and an instance carrying a thousand messages a second would
// write a thousand lines a second about the same fact -- which costs more than
// the outage and buries the line that says the outage started. The transition is
// the event; CompareAndSwap is what detects it, and the second caller in the
// same outage says nothing.
//
// This is not a mode (RULE 9). There is one delivery path and one publish path,
// and degradation is those same paths reporting that the half of them which
// reaches other instances is not answering.
func (r *Relay) fail(operation string, name ChannelName, err error) {
	// The log is written BEFORE the flag is published, and the order is the
	// whole of this function's correctness.
	//
	// It used to be the other way round: CompareAndSwap first, then the log
	// line. Anything watching Degraded saw true while the line was not written
	// yet -- a window one log write wide, which is exactly long enough for
	// somebody diagnosing an outage to read a degraded instance and find
	// nothing saying why. The test that found it waits on Degraded and then
	// reads the log, which is what an operator does.
	//
	// So: the transition is claimed under the mutex, the line is written, and
	// only then does Degraded start answering true. A reader that sees the flag
	// is guaranteed the record exists.
	r.transition.Lock()
	if r.degraded.Load() {
		r.transition.Unlock()
		return
	}

	r.log.Error("joaju: the relay cannot reach the message bus; this instance is serving its own connections only",
		"instance", string(r.id), "operation", operation,
		"tenant", name.Tenant(), "channel", name.Requested(), "error", err)

	r.degraded.Store(true)
	r.transition.Unlock()
}

// recovered records that the bus answered, and logs the recovery once.
//
// It is the other half of [Relay.fail], and it exists so that an instance which
// reported an outage also reports the end of one. An operator reading a log that
// only ever says a thing broke has to guess when it stopped.
func (r *Relay) recovered() {
	// Same order as [Relay.fail], for the same reason: the line is written
	// before the flag stops answering true.
	r.transition.Lock()
	defer r.transition.Unlock()
	if !r.degraded.Load() {
		return
	}
	defer r.degraded.Store(false)

	r.log.Info("joaju: the relay reached the message bus again; this instance is part of the fleet",
		"instance", string(r.id))
}
