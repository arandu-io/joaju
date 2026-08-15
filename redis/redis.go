// Package redis carries [joaju.Bus] over RESP pub/sub, which is what lets two
// instances of the server deliver each other's events.
//
// It is one exported function. Everything a relayed message means -- the topic
// a channel is relayed on, the tenant in the middle of that topic, the instance
// id that stops a message coming back to its sender, the degradation reported
// when the bus is unreachable -- belongs to [joaju.Relay] and stays there. What
// is here is PUBLISH and SUBSCRIBE:
//
//	conn := connections.Connect(connections.Config{Address: address, Prefix: app})
//	bus, err := redis.NewBus(conn)
//	relay, err := joaju.NewRelay(ctx, joaju.InstanceID(hostname), bus, log)
//
// # Why it is its own module
//
// The driver underneath is a third-party dependency, and Go has no optional
// dependency (ADR 0048). The root module of this repository has one require
// line and a CI step that fails when it grows a second: subpackage ws is this
// project's own RFC 6455 precisely so that the transport costs nothing in the
// dependency graph (ADR 0052), and a Redis client imported at the root would
// spend what that bought -- on every project running a single instance, which
// needs no bus at all.
//
// # The protocol is RESP, and RESP is four products
//
// Dragonfly, Redis, Valkey and KeyDB all answer it, and the two commands used
// here are in all four. Nothing here may reach for RedisJSON, RediSearch or a
// Lua script (RULE 11): the moment it does, choosing the product stops being a
// connection string and a fleet is pinned to one vendor by its message bus.
//
// # What the driver already answers, and what is not built again here
//
// Reverb's RedisPubSubProvider opens two clients, a publisher and a subscriber,
// because a connection in subscribe mode refuses every other command and the
// ReactPHP client under it is one socket per client. This driver is not: a
// subscription is dialled on a socket of its own, outside the pool the commands
// use, so a PUBLISH issued on a connection that is subscribed goes out on a
// different socket than the SUBSCRIBE did. One [connections.Connection] is
// therefore both halves, and a second would be another pool to size rather than
// another guarantee -- see TestPublishAndSubscribeDoNotShareASocket, which
// checks that against a server rather than believing it.
//
// Reverb's subscriber re-runs its subscribe() on every connection and not only
// on the first, because a socket that was dialled again is subscribed to
// nothing. The driver keeps the set of channels and re-sends it when it dials
// again, so a server that goes away and comes back finds the subscription where
// it was -- see TestASubscriptionSurvivesTheConnectionDying.
//
// # One socket per relayed channel
//
// [joaju.Relay.Join] calls Subscribe once for one channel, and every call
// dials, because that is what a subscription costs in RESP: a connection that
// is subscribed to a set of channels and to nothing else. An instance holding a
// thousand distinct channels holds a thousand subscriber sockets, which is
// worth knowing before a server's maxclients says it. It is the shape of the
// interface and not of this package, and no wrapper here can change it: what
// would is fewer, wider subscriptions, which is a decision about the relay.
package redis

import (
	"context"
	"errors"

	"github.com/arandu-io/hesape/redis/connections"
	"github.com/arandu-io/joaju"
)

// NewBus is the only way to a [joaju.Bus] over RESP.
//
// The connection is the application's and stays the application's: it is opened
// by [connections.Connect] and closed by whatever opened it, and handing over
// the one that already serves the cache is the expected wiring rather than a
// shortcut. Nothing here opens one, because [connections.Connect] is how this
// stack opens a RESP connection and a second door onto the same thing is the
// complexity RULE 9 exists to refuse.
//
// It does not talk to the server, and a Redis that is down at this moment is
// not an error. That is [joaju.NewRelay]'s posture too -- it takes a nil bus,
// warns once, and serves the sockets it holds -- and an instance that refused
// to start because Redis blinked would take its own connections down over the
// loss of the other instances'. What an outage costs is reach, and reach is
// what [joaju.Relay.Degraded] reports. An operator who wants boot to fail on a
// mistyped address has [connections.Connection.Ping] for it, before this call.
//
// A nil connection is refused rather than read as "no Redis here". An instance
// with nobody to relay to hands [joaju.NewRelay] a nil [joaju.Bus], which is a
// supported deployment and says so in the log; a bus over a nil connection is a
// nil dereference in the driver, inside a goroutine the relay started for a
// subscription, which takes the process with it.
func NewBus(conn *connections.Connection) (joaju.Bus, error) {
	if conn == nil {
		return nil, errors.New("joaju/redis: a bus needs an open connection; an instance with no Redis passes a nil Bus to joaju.NewRelay instead")
	}

	return &bus{conn: conn}, nil
}

// bus is one connection, narrowed to the two methods [joaju.Bus] names.
//
// It is a type rather than the connection handed back as it stands -- a
// *connections.Connection satisfies [joaju.Bus] already, signature for
// signature, which is what that interface's own documentation says -- because
// the two methods below refuse three arguments the driver accepts and then
// fails on quietly, or in a goroutine. See Subscribe: each of them costs a line
// here and is otherwise found in production.
type bus struct {
	conn *connections.Connection
}

// Publish sends one message to one channel and answers how many subscribers
// received it.
//
// The count is not decoration. [joaju.Relay] asks the fleet a question by
// publishing it, and what comes back here is how many instances are listening
// -- so it knows how many answers to wait for, and stops waiting when it has
// them rather than at the timeout.
//
// The channel name is prefixed with the connection's key prefix, by the
// connection, here and on the subscriber's side alike. Two applications sharing
// one server therefore never hear each other; two instances of one application
// configured with different prefixes never hear each other either, which is a
// fleet split in half by a typo in an environment variable.
//
// An empty channel name is refused. Redis would accept it, deliver to whoever
// subscribed to the empty name -- nobody -- and answer zero, which is a message
// lost in a call that reported success.
func (b *bus) Publish(ctx context.Context, channel string, message any) (int64, error) {
	if channel == "" {
		return 0, errors.New("joaju/redis: publish was given no channel")
	}

	return b.conn.Publish(ctx, channel, message)
}

// Subscribe listens on a set of channels and calls callback for each message,
// with the message first and the channel it arrived on second. It blocks until
// ctx is cancelled, which is how a subscription is ended, and what it answers
// then is that context's error.
//
// The channel the callback is handed carries the connection's prefix, and the
// one passed here does not: what comes back is the name as it travelled on the
// wire. A caller that matches on it must prefix, and [joaju.Relay] does not
// need to -- it subscribes to one topic per call and already knows which.
//
// It does not return when the connection dies, and the relay's own retry loop
// is written to allow for that: the driver dials again and re-sends the
// subscription itself, faster than a loop out here could, and a message
// published in between is lost either way -- pub/sub has no queue and no
// replay. A second recovery path over the driver's own would be two ways to do
// one thing (RULE 9), and the slower one would be the one holding the socket.
//
// An empty channel list is refused. The driver accepts it, subscribes to
// nothing, and blocks: a call that delivers no message, reports no error and
// never returns, which reads from the outside exactly like a quiet channel. A
// nil callback is refused for the same reason it would otherwise be found late
// -- it panics on the first message, in the goroutine the relay started, and
// the first message may be hours away.
func (b *bus) Subscribe(ctx context.Context, channels []string, callback func(message, channel string)) error {
	if len(channels) == 0 {
		return errors.New("joaju/redis: subscribe was given no channels; it would block forever and deliver nothing")
	}
	if callback == nil {
		return errors.New("joaju/redis: subscribe was given no callback; the first message would panic the goroutine it arrived on")
	}

	return b.conn.Subscribe(ctx, channels, callback)
}
