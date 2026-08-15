package joaju

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
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

// DefaultMetricsTimeout is how long a metrics route waits for the rest of the
// fleet before it answers with what it has.
//
// Unlike [DefaultRetryInterval] this one is a default and not the whole answer,
// because it is the number here that depends on the deployment: instances
// sharing a datacentre answer in single-digit milliseconds and instances spread
// across a region do not. What it may not be is absent. An instance that has
// stopped answering must not hold a route open, and most of the fleet's numbers
// now is worth more than all of them never.
const DefaultMetricsTimeout = 500 * time.Millisecond

// MetricsTopic is the pub/sub channel one instance asks the others what they
// are holding on.
//
// It is one topic for the whole fleet and not one per tenant, because the
// question is asked by an instance and not by a customer: every instance
// subscribes once, when its server is built, and a topic per tenant would mean
// a subscription that changes every time a customer's first socket opens. The
// tenant travels inside the question, where it is a filter and never an
// authority -- see [Relay.answer].
//
// It cannot be mistaken for a channel's [Topic]. A channel's topic is
// [TopicPrefix], the tenant, broadcasting.TenantSeparator and the name, so it
// always carries a second colon; this carries none.
const MetricsTopic = TopicPrefix + "metrics"

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
// is what writing the ws subpackage bought (ADR 0052). Stating the contract and
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

// MetricsReplyTopic is where the answers to one instance's question go.
//
// An answer is addressed to the instance that asked rather than published back
// to the fleet, and what an answer carries is the reason: the members of a
// presence channel are people's ids, and a shared reply topic would put one
// customer's member list in front of every instance in the fleet, including the
// ones holding none of that customer's sockets.
//
// The separator is "/" and not broadcasting.TenantSeparator so that no reply
// topic can be read as a channel's [Topic]: a tenant is letters, digits, "-"
// and "_", so a topic with a "/" in that position was never a channel's.
func MetricsReplyTopic(id InstanceID) string { return MetricsTopic + "/" + string(id) }

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

// fleetQuestion is one instance asking the others what they are holding, and is
// what a metrics route publishes on [MetricsTopic].
//
// There is one question and not one per route. The four metrics routes read
// four views of the same two numbers -- how many sockets, and who is on which
// channel -- so four questions would be four payloads to keep in step, and the
// route that got out of step would be the one nobody looks at until it is
// wrong (RULE 9).
type fleetQuestion struct {
	// Origin is who asked, and is where the answer is addressed:
	// [MetricsReplyTopic] of this id.
	Origin InstanceID `json:"origin"`
	// Request is what ties an answer to this question. See
	// [Relay.newRequestID].
	Request string `json:"request"`
	// Tenant is whose numbers are being asked for, read off the Grant the route
	// was authorized with (RULE 14).
	//
	// It is a FILTER on the instance that answers and never an authority: an
	// answering instance compares it against the tenant its own channels and
	// sockets already carry, each of which came off a Grant of its own. Nothing
	// is unlocked by naming a tenant here, and an instance holding nothing of
	// that tenant answers with nothing.
	Tenant string `json:"tenant"`
	// Channel is [ChannelName.Requested] of the one channel being asked about,
	// and is empty for the whole tenant.
	//
	// It carries no tenant, for the reason [relayMessage.Channel] carries none:
	// the tenant is a field of its own and a second copy is a second thing for
	// the two ends to disagree about.
	Channel string `json:"channel,omitempty"`
}

// fleetAnswer is what one instance answers a [fleetQuestion] with: the numbers
// it would have put in its own reply had the route been called on it.
//
// The numbers are the route's own and not a summary of them, because the
// aggregator has to add them up and two of the four cannot be added. Sockets
// and subscriptions can: one socket is held by exactly one instance, so the
// fleet's count is the sum. A presence channel's member count cannot: one
// person with a tab on two instances is counted by both and is one member, so
// the ids travel and the aggregator counts what is distinct.
type fleetAnswer struct {
	// Origin is the instance that answered. An answer carrying the reader's own
	// id is dropped, for the reason [relayMessage.Origin] gives.
	Origin InstanceID `json:"origin"`
	// Request is the [fleetQuestion.Request] this answers.
	Request string `json:"request"`
	// Tenant is the tenant this instance filtered by, echoed back so that the
	// instance which asked can check it got an answer to its own question.
	Tenant string `json:"tenant"`
	// Connections is how many sockets this instance holds for the tenant.
	Connections int `json:"connections"`
	// Channels is one entry per channel of the tenant this instance relays,
	// keyed by [ChannelName.Requested].
	Channels map[string]channelAnswer `json:"channels,omitempty"`
}

// channelAnswer is one instance's part of one channel.
type channelAnswer struct {
	// Subscriptions is how many sockets this instance holds on the channel.
	Subscriptions int `json:"subscriptions"`
	// Users are the [Member.UserID]s on it, and are empty off a presence
	// channel. They are ids and not a count because a member list is a union
	// and not a sum -- see [fleetAnswer].
	Users []string `json:"users,omitempty"`
}

// fleetTally is the other instances' answers added together, and is what the
// four metrics routes add to their own numbers.
//
// It holds what the fleet said and never what this instance knows. The local
// half of every route is answered from the Broker under the Grant the route was
// authorized with, exactly as it was before there was a fleet, and the two are
// added at the point the reply is built (RULE 17: the local numbers keep coming
// from a policy).
type fleetTally struct {
	// connections is how many sockets the other instances hold for the tenant.
	connections int
	// channels is one entry per channel the other instances hold, keyed by
	// [ChannelName.Requested].
	channels map[string]channelTally
}

// channelTally is the other instances' part of one channel.
type channelTally struct {
	// subscriptions is how many sockets they hold on it, summed.
	subscriptions int
	// users are the distinct [Member.UserID]s they hold on it.
	users map[string]bool
}

// channel is what the fleet answered about one channel, or the zero value when
// no other instance holds it.
func (t fleetTally) channel(requested string) channelTally {
	return t.channels[requested]
}

// members is the fleet's user ids on one channel, in one order.
//
// Sorted for the reason [channel.Connections] is sorted: a member list that
// comes back in a different order on every request is a route nobody can diff,
// and a map's order is not an order.
func (t channelTally) members() []string {
	ids := make([]string, 0, len(t.users))
	for id := range t.users {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	return ids
}

// add folds one instance's answer into the tally, or drops it.
//
// tenant is the tenant of the Grant the route was authorized with, and an
// answer about any other is dropped rather than added. That check is the second
// of the two that keep a fleet answer inside one customer: the instance that
// answered filtered by the tenant it was asked about, and the instance that
// asked refuses anything that came back about a different one. Neither of them
// trusts the other, and a sum that crossed a tenant would need both to be
// wrong.
func (t *fleetTally) add(log *slog.Logger, mine InstanceID, tenant string, answer fleetAnswer) {
	if answer.Origin == mine {
		// This instance's own numbers came off a Grant and are added by the
		// route. Adding them again here would count every socket twice.
		return
	}
	if answer.Tenant != tenant {
		log.Warn("joaju: the relay dropped a fleet answer about another tenant",
			"instance", string(mine), "asked", tenant, "answered", answer.Tenant,
			"origin", string(answer.Origin))

		return
	}

	t.connections += answer.Connections
	for requested, one := range answer.Channels {
		if t.channels == nil {
			t.channels = make(map[string]channelTally, len(answer.Channels))
		}

		held := t.channels[requested]
		held.subscriptions += one.Subscriptions
		for _, id := range one.Users {
			if id == "" {
				continue
			}
			if held.users == nil {
				held.users = make(map[string]bool, len(one.Users))
			}
			held.users[id] = true
		}
		t.channels[requested] = held
	}
}

// fleetLocal is what a [Relay] reads off its [Server] to answer the fleet's
// questions about this instance.
//
// It is an interface with one unexported method rather than the [Server] itself
// because it is the whole of what a relay may disclose about its process, and
// naming it is what keeps that list from growing by accident. Unexported so
// that only this package can implement or call it: it reads without a Grant,
// and the only reason that is not RULE 17 with a hole in it is that the tenant
// it is given is an exact match against tenants the sockets already carried.
type fleetLocal interface {
	// tenantConnections is how many sockets this instance holds for one tenant.
	tenantConnections(tenant string) int
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
// # The other direction: the metrics routes
//
// The same bus carries the four metrics routes' questions, and it carries them
// because those routes are wrong without it. A count of open sockets served out
// of one process is a count of one process's sockets, and two instances serving
// one application answer two different numbers of which neither is the answer.
//
// So a route asks: it publishes a [fleetQuestion] on [MetricsTopic], every
// other instance answers with what it holds on [MetricsReplyTopic] of the
// asker, and the asker adds those to what it answered from its own Broker. An
// instance that does not answer in time is left out and the route replies
// anyway -- see [Relay.ask] for why a partial answer is the answer.
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

	// local is the server this relay answers the fleet's questions from, and is
	// nil until one attaches with [Relay.serve]. A relay with no server relays
	// events and answers no questions, because there is no route to ask one.
	local fleetLocal
	// wait is how long [Relay.ask] waits for the fleet. Set by [Relay.serve],
	// which is the only thing that makes asking possible in the first place.
	wait time.Duration

	// mu guards the three fields below.
	mu sync.Mutex
	// joined is one entry per channel this instance relays, keyed by [Topic].
	joined map[string]relayed
	// closed is set by Close.
	closed bool
	// wg counts the running subscriptions, so Close can wait for them.
	wg sync.WaitGroup

	// pendingMu guards pending. It is not mu, because an answer arriving must
	// not queue behind a channel being joined.
	pendingMu sync.Mutex
	// pending is one entry per question this instance is waiting on, keyed by
	// [fleetQuestion.Request]. The entry is gone the moment [Relay.ask]
	// returns, which is what makes a late answer a dropped answer.
	pending map[string]chan fleetAnswer
}

// relayed is one joined channel: what ends its subscription, and the channel.
//
// The channel is kept and not only the cancel, because it is what
// [Relay.answer] reads. The set of channels this instance relays is the set it
// may say anything about to the fleet, and every one of them arrived through
// [Relay.Join], which refuses a Grant that did not authorize
// broadcasting.ChannelJoin for that channel's own tenant. A fleet answer built
// from anything wider would be a tenant boundary that holds everywhere except
// the dashboard.
type relayed struct {
	// cancel ends the subscription carrying the other instances' traffic.
	cancel context.CancelFunc
	// channel is what was joined.
	channel Channel
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
		id:      id,
		bus:     bus,
		log:     log,
		root:    ctx,
		cancel:  cancel,
		retry:   DefaultRetryInterval,
		wait:    DefaultMetricsTimeout,
		joined:  make(map[string]relayed),
		pending: make(map[string]chan fleetAnswer),
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
	r.joined[topic] = relayed{cancel: cancel, channel: ch}

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
	held, joined := r.joined[topic]
	delete(r.joined, topic)
	r.mu.Unlock()

	if joined {
		held.cancel()
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
	r.joined = make(map[string]relayed)
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
//
// name is the channel the failed operation concerned, and is the zero value for
// the ones that concern no channel -- the fleet's questions and its answers
// travel on topics of their own. The two attributes are left off the line
// rather than written empty, because an empty tenant on an outage report reads
// like an outage that lost the tenant.
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

	attributes := []any{"instance", string(r.id), "operation", operation}
	if !name.IsZero() {
		attributes = append(attributes, "tenant", name.Tenant(), "channel", name.Requested())
	}
	attributes = append(attributes, "error", err)

	r.log.Error("joaju: the relay cannot reach the message bus; this instance is serving its own connections only",
		attributes...)

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

// fleetBacklog is how many answers to one question a [Relay] holds before it
// starts dropping them.
//
// It is a buffer and not a queue that grows, because the reader is [Relay.ask],
// which is already draining and cannot be more than a scheduling delay behind.
// A fleet larger than this would have to answer faster than one instance can
// read for a single answer to be dropped, and a dropped answer costs the
// question a wait rather than an error: the instance that sent it is simply one
// of the ones that did not arrive in time.
const fleetBacklog = 64

// serve makes this relay the answering half of the fleet's metrics, and is
// called by [NewServer] for the [ServerConfig.Relay] it was given.
//
// It is separate from [NewRelay] because a relay exists before the server that
// attaches to it -- the server is built with the relay in its config -- and
// because the two halves are genuinely independent: a relay with no server
// still carries events between instances, it just has no route through which
// anybody could ask it a question.
//
// wait is [ServerConfig.MetricsTimeout], and zero means [DefaultMetricsTimeout].
//
// A second server on one relay is refused. One process is one server (ADR 0052),
// so a second one is a wiring mistake, and the mistake it would cause is two
// servers answering the fleet's questions as one instance -- which is the same
// id twice, and the one thing [InstanceID] may not be.
func (r *Relay) serve(local fleetLocal, wait time.Duration) error {
	if local == nil {
		return errors.New("joaju: the relay was given no server to answer the fleet's questions from")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRelayClosed
	}
	if r.local != nil {
		return errors.New("joaju: this relay already answers for a server, and one process is one server")
	}

	r.local = local
	r.wait = wait
	if r.wait <= 0 {
		r.wait = DefaultMetricsTimeout
	}
	if r.bus == nil {
		// An instance deliberately alone has nobody to ask and nobody to answer.
		// It is degraded from birth, which is what makes every metrics route on
		// it reply out of its own Broker and wait for nothing.
		return nil
	}

	r.wg.Add(2)
	go r.attend(r.root, MetricsTopic, "subscribe to the fleet's questions", r.answer)
	go r.attend(r.root, MetricsReplyTopic(r.id), "subscribe to the fleet's answers", r.collect)

	return nil
}

// attend holds one of the two metrics subscriptions open, and opens it again
// when it is lost.
//
// It is [Relay.listen] for a topic with no channel behind it, and it is a
// second function rather than a parameter on that one because what they do with
// a message has nothing in common: one delivers to sockets and the other reads
// a payload the fleet sent. What they share is the retry, which is four lines.
func (r *Relay) attend(ctx context.Context, topic, operation string, handle func(payload string)) {
	defer r.wg.Done()

	for {
		err := r.bus.Subscribe(ctx, []string{topic}, func(message, _ string) {
			handle(message)
		})
		if ctx.Err() != nil {
			return
		}

		r.fail(operation, ChannelName{}, err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(r.retry):
		}
	}
}

// answer replies to one instance's question with what this one is holding.
//
// This is the half where a tenant could leak, and the shape of it is
// [Relay.deliver]'s for the same reason. The tenant in the question is used
// ONLY as an equality test against the tenant a channel or a socket already
// carries -- and those came off a Grant: a [ChannelName] cannot be built
// without one (RULE 14), and [Relay.Join] refuses a channel whose tenant is not
// the Grant's. Naming a tenant here therefore unlocks nothing. It narrows what
// this instance says, and an instance holding none of that customer's traffic
// answers with none.
//
// What it does not do is authorize. There is no policy on this path and there
// cannot be one: the subject is on the instance that took the request, and a
// subject that travelled over the bus would be authentication coming off the
// wire. The decision that let the question be asked at all was made there, by
// [ConnectPolicy] and [SubscriptionPolicy], before the route published
// anything.
func (r *Relay) answer(payload string) {
	// An arriving question is proof the bus is reachable, for the reason
	// [Relay.deliver] says so.
	r.recovered()

	var question fleetQuestion
	if err := json.Unmarshal([]byte(payload), &question); err != nil {
		r.log.Warn("joaju: the relay dropped a fleet question it could not decode",
			"instance", string(r.id), "error", err)

		return
	}

	if question.Origin == r.id {
		// Our own question, heard back off the topic we published it on. This
		// instance's numbers are answered by the route out of its own Broker,
		// under the Grant that authorized the request.
		return
	}
	if question.Origin == "" || question.Request == "" || question.Tenant == "" {
		// Nothing in the fleet asks without all three: an origin is where the
		// answer goes, a request is what ties it to the question, and a tenant
		// is the only thing narrowing what would be answered. A question
		// missing one did not come from an instance of this fleet.
		r.log.Warn("joaju: the relay dropped a fleet question that named no instance, no request or no tenant",
			"instance", string(r.id))

		return
	}

	answer := fleetAnswer{
		Origin:      r.id,
		Request:     question.Request,
		Tenant:      question.Tenant,
		Connections: r.local.tenantConnections(question.Tenant),
	}

	for _, held := range r.channels() {
		name := held.Name()
		if name.Tenant() != question.Tenant {
			continue
		}
		if question.Channel != "" && name.Requested() != question.Channel {
			continue
		}

		subscribers := held.Connections()
		one := channelAnswer{Subscriptions: len(subscribers)}
		if name.Type().Presence() {
			for _, subscriber := range subscribers {
				if id := subscriber.Member.UserID; id != "" {
					one.Users = append(one.Users, id)
				}
			}
		}

		if answer.Channels == nil {
			answer.Channels = make(map[string]channelAnswer)
		}
		// Keyed by the name the client asked for and never by
		// [ChannelName.String], for the reason the metrics routes answer by it:
		// the tenant is not the asking instance's to read back off a name.
		answer.Channels[name.Requested()] = one
	}

	document, err := json.Marshal(answer)
	if err != nil {
		r.log.Error("joaju: the relay could not encode its answer to the fleet",
			"instance", string(r.id), "tenant", question.Tenant, "error", err)

		return
	}

	if _, err := r.bus.Publish(r.root, MetricsReplyTopic(question.Origin), string(document)); err != nil {
		if r.root.Err() != nil {
			// This relay is closing, and the answer failed because of that
			// rather than because the bus is unreachable. Reporting an outage
			// here would put one on the log of every clean shutdown, which is
			// the check [Relay.listen] makes for the same reason.
			return
		}
		r.fail("answer the fleet", ChannelName{}, err)

		return
	}
	r.recovered()
}

// collect hands one answer to the question that is waiting for it.
//
// An answer to a question that has already timed out finds no one waiting and
// is dropped, which is the whole of what "late" costs: the route it belonged to
// replied without it, and there is nothing left to add it to.
func (r *Relay) collect(payload string) {
	r.recovered()

	var answer fleetAnswer
	if err := json.Unmarshal([]byte(payload), &answer); err != nil {
		r.log.Warn("joaju: the relay dropped a fleet answer it could not decode",
			"instance", string(r.id), "error", err)

		return
	}
	if answer.Request == "" {
		r.log.Warn("joaju: the relay dropped a fleet answer that names no question",
			"instance", string(r.id), "origin", string(answer.Origin))

		return
	}

	r.pendingMu.Lock()
	waiting, asked := r.pending[answer.Request]
	r.pendingMu.Unlock()

	if !asked {
		return
	}

	select {
	case waiting <- answer:
	default:
		// See [fleetBacklog]: the asker is draining, so this is a fleet that
		// answered faster than one goroutine could read. The answer is lost and
		// the question waits for it, which the timeout ends.
		r.log.Warn("joaju: the relay dropped a fleet answer it had no room for",
			"instance", string(r.id), "origin", string(answer.Origin))
	}
}

// ask puts one question to the fleet and answers with what came back before the
// timeout.
//
// g is the Grant the route was authorized with, and the tenant of the question
// is read off it and off nothing in the request (RULE 14). Nothing else is read
// off it: which action it was issued for is the route's business, and the four
// metrics routes do not agree on one -- the count of sockets is authorized by
// the [ConnectPolicy] and the three channel routes by the
// [SubscriptionPolicy]. What this checks is that a Grant carrying no tenant
// asks nothing, which is what the zero Grant is.
//
// # Why a partial answer is an answer
//
// An instance that has stopped answering must not hold this route open. It is
// the failure the fleet is most likely to actually have -- a pod being
// replaced, a network partition, an instance too busy to read its socket -- and
// a metrics route that waited for it would be a dashboard that goes down when
// one instance of ten does. So the timeout is the bound, whoever answered by
// then is in the reply, and whoever did not is out of it.
//
// It does not usually wait that long. Bus.Publish answers how many subscribers
// took the question, this instance's own answering subscription included, so
// the number of answers to expect is one less -- an instance does not answer
// itself, because its own numbers came off the Grant. When that many have
// arrived there is nobody left to wait for and the route replies at once. A
// fleet of one pays nothing at all.
//
// A degraded relay does not ask. The bus is what the question would travel on,
// and an instance that cannot reach it answers out of its own Broker rather
// than spending the timeout finding that out again on every request -- which is
// [Relay.Degraded] doing what it is for, and not a third state (RULE 9).
func (r *Relay) ask(ctx context.Context, g auth.Grant, channel string) fleetTally {
	var tally fleetTally

	tenant := auth.Tenant(g)
	if tenant == "" || r.bus == nil || r.Degraded() {
		return tally
	}

	r.mu.Lock()
	closed, wait := r.closed, r.wait
	r.mu.Unlock()

	if closed {
		return tally
	}

	request := r.newRequestID()
	waiting := make(chan fleetAnswer, fleetBacklog)

	// Registered before the question goes out, or an instance quick enough to
	// answer before this line runs would find nobody waiting.
	r.pendingMu.Lock()
	r.pending[request] = waiting
	r.pendingMu.Unlock()

	defer func() {
		r.pendingMu.Lock()
		delete(r.pending, request)
		r.pendingMu.Unlock()
	}()

	document, err := json.Marshal(fleetQuestion{
		Origin:  r.id,
		Request: request,
		Tenant:  tenant,
		Channel: channel,
	})
	if err != nil {
		r.log.Error("joaju: the relay could not encode a question for the fleet",
			"instance", string(r.id), "tenant", tenant, "error", err)

		return tally
	}

	heard, err := r.bus.Publish(ctx, MetricsTopic, string(document))
	if err != nil {
		r.fail("ask the fleet", ChannelName{}, err)

		return tally
	}
	r.recovered()

	expected := int(heard) - 1
	if expected <= 0 {
		return tally
	}

	deadline := time.NewTimer(wait)
	defer deadline.Stop()

	for range expected {
		select {
		case answer := <-waiting:
			tally.add(r.log, r.id, tenant, answer)
		case <-deadline.C:
			return tally
		case <-ctx.Done():
			return tally
		}
	}

	return tally
}

// channels is every channel this instance relays, which is every channel it may
// answer the fleet about. See [relayed].
func (r *Relay) channels() []Channel {
	r.mu.Lock()
	defer r.mu.Unlock()

	held := make([]Channel, 0, len(r.joined))
	for _, one := range r.joined {
		held = append(held, one.channel)
	}

	return held
}

// newRequestID mints the correlation of one question.
//
// Both halves are load-bearing. [Relay.ID] is what keeps two instances asking
// at the same moment apart, and it is already required to be unique across the
// fleet for the deduplication in [InstanceID] to work at all. The random half
// is what keeps two questions from ONE instance apart, and two of those happen
// whenever two requests are in flight, which on a metrics route is normal.
//
// crypto/rand and not math/rand, for the reason [newSocketID] is random rather
// than a counter: an id somebody can guess is an id somebody can answer, and an
// answer that was not asked for is numbers of unknown provenance added to a
// customer's dashboard. A timestamp is not used for the same reason and one
// more -- two questions in one clock tick collide, and a clock that steps back
// makes them collide on purpose.
func (r *Relay) newRequestID() string {
	// rand.Text is the standard library's own answer to "a random string for
	// correlation", so there is no encoding to choose here and no error to
	// handle: it cannot fail.
	return string(r.id) + "." + rand.Text()
}

// relayedBroker is the [Broker] a server holds when it was built with a
// [Relay], and it is the inbound half of horizontal scaling: what makes this
// instance receive from the bus the channels it is actually holding, and only
// those.
//
// The two methods it wraps are the two ends of a channel's life on one instance,
// which is what [Broker] already says they are: [Broker.FindOrCreate] is what a
// subscription calls, because the first subscriber is what brings a channel into
// existence, and [Broker.Remove] is what the last subscriber leaving calls. So
// the fleet's topic is joined where a channel appears here and left where it
// goes, and neither a [Protocol] nor an application's Broker has to remember
// either call.
//
// Not having to remember is the reason this wraps instead of being asked of
// every implementation. [Broker] is an interface an application writes, and a
// relay that had to be joined by hand would be joined by hand wrongly in exactly
// one deployment -- where the symptom is a client hearing nothing, which from
// inside a browser is indistinguishable from a quiet channel (RULE 9).
//
// [Broker.Find] and [Broker.All] are the embedded ones and are untouched,
// because they read. A route that looks a channel up is not a socket listening
// on one, and an instance that opened a pipe from the fleet for every channel a
// dashboard mentioned would be receiving traffic it has nobody to deliver to.
type relayedBroker struct {
	Broker

	// relay is the one this server was built with, and is never nil: a server
	// without one holds the application's Broker unwrapped.
	relay *Relay
}

// FindOrCreate is [Broker.FindOrCreate], and joins the fleet's topic for the
// channel it answers with.
//
// The Grant is the subscriber's own, and it is what makes the join sound rather
// than ceremonial: [Relay.Join] refuses one that did not authorize
// broadcasting.ChannelJoin, and refuses one whose tenant is not the channel's.
// What opens is the [Topic] of a name built from a Grant on this instance, so
// what arrives on it can only be delivered to the channel of that tenant -- see
// [Relay.deliver], which takes the name from here and never from the message.
//
// A join that fails fails the call, and what can fail it is why: a Grant issued
// for something else, a Grant for another tenant, or a relay that is closed.
// None of them is a bus that is down -- [Relay.Join] answers a
// degraded relay successfully, because an outage costs the other instances and
// not the subscription. So the only thing refused here is a subscription this
// instance would have served alone while the client believed it was on the
// fleet's channel, and that is worth refusing at the one moment it can still be
// reported to whoever asked for it.
func (b relayedBroker) FindOrCreate(ctx context.Context, g auth.Grant, name ChannelName) (Channel, error) {
	held, err := b.Broker.FindOrCreate(ctx, g, name)
	if err != nil {
		return nil, err
	}

	if err := b.relay.Join(g, held); err != nil {
		return nil, fmt.Errorf("joaju: %s exists on this instance and cannot be relayed to the others: %w",
			name.Requested(), err)
	}

	return held, nil
}

// Remove is [Broker.Remove], and stops relaying the channel it dropped.
//
// The order is the one [Relay.Leave] names its caller in: the channel goes
// first and the subscription second, so a message arriving between the two
// finds a channel with nobody on it rather than a topic this instance has
// already left. Neither costs anything -- a broadcast to no subscribers writes
// no bytes -- and one of the two orders had to be the written one.
func (b relayedBroker) Remove(ctx context.Context, g auth.Grant, name ChannelName) error {
	if err := b.Broker.Remove(ctx, g, name); err != nil {
		return err
	}

	// Remove is a request and not a promise: a Broker may keep a channel that
	// filled again between the caller deciding it was empty and this call
	// arriving, and [NewMemoryBroker] does exactly that. Leaving the topic
	// anyway would stop this instance receiving broadcasts for a channel that
	// still has subscribers on it -- messages lost, silently, for as long as
	// they stay.
	//
	// So the fleet is left only when the channel is really gone. Asking costs a
	// read, and the race it leaves is harmless in the direction that matters: a
	// channel recreated in the gap keeps a subscription it would have opened
	// again anyway.
	if _, err := b.Broker.Find(ctx, g, name); !errors.Is(err, ErrNoChannel) {
		return nil
	}

	return b.relay.Leave(name)
}

// carry hands the fleet an event this instance has already delivered, and is the
// outbound half of horizontal scaling.
//
// It is called by [Server.publish] after [Channel.Broadcast] and never instead
// of it, which is the order [Relay] describes: the sockets here were served by
// the broadcast, and this is only about the instances holding the others.
//
// Nothing it does can fail the request that caused it. [Relay.Publish] answers a
// bus it cannot reach with nil -- what an outage costs is reach, and the
// delivery has already happened -- so an error here is a malformed event or a
// closed relay, and both are recorded rather than returned. The caller was told
// its event was delivered, and it was.
func (s *Server) carry(ctx context.Context, e Event) {
	if s.relay == nil {
		return
	}

	if err := s.relay.Publish(ctx, e); err != nil {
		s.log.ErrorContext(ctx, "joaju: an event delivered to this instance's connections was not handed to the fleet",
			"tenant", e.Channel.Tenant(), "channel", e.Channel.Requested(), "event", e.Name, "error", err)
	}
}

// tenantConnections is how many sockets this instance holds for one tenant, and
// is what it answers the fleet with. It is [fleetLocal].
//
// It is the number [Server.Connections] answers, kept as a counter rather than
// counted, because register and unregister maintain it for the connection limit
// already -- and because a fleet question that walked a registry of ten
// thousand sockets would make the dashboard the most expensive route there is.
//
// It takes no Grant and there is exactly one reason it may not: the tenant it
// is given is compared for equality against a key that only a socket's own
// Grant could have created. It cannot widen, and it is unexported so that
// nothing outside this package can reach it looking for a read without a
// policy.
func (s *Server) tenantConnections(tenant string) int {
	if tenant == "" {
		return 0
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.perTenant[tenant]
}

// fleet is what the other instances hold, and is the second half of each of the
// four metrics routes.
//
// The first half is unchanged and still the whole of the answer on a deployment
// of one: the route reads its own Broker and its own registry under the Grant
// it was authorized with. This adds what the rest of the fleet answered, or
// nothing at all when there is no relay, when the relay cannot reach the bus,
// or when nobody answered in time. A route never fails because of it -- see
// [Relay.ask], and [Relay.Publish] for the same decision on the way out.
func (s *Server) fleet(ctx context.Context, g auth.Grant, channel string) fleetTally {
	if s.relay == nil {
		return fleetTally{}
	}

	return s.relay.ask(ctx, g, channel)
}

// fleetPresence reports whether a channel only the other instances hold
// publishes its members, so that the channel list can say how many people are
// on one this instance has never seen.
//
// The kind is read through [ChannelName.Type], which is the one place a
// prefix is compared (RULE 9), and the name is built under the asking Grant --
// so the [ChannelName] it goes through carries this request's own tenant and
// never one that arrived with an answer. A name the fleet answered with that
// cannot be a name here is not a presence channel and is not anything else
// either.
func fleetPresence(g auth.Grant, requested string) bool {
	name, err := NewChannelName(g, requested)

	return err == nil && name.Type().Presence()
}
