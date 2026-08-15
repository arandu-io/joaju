package joaju

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
)

// Connect is the auth.Action the Grant behind a [Connection] is issued for.
//
// It is the first of the two decisions this package makes, and it is separate
// from broadcasting.ChannelJoin on purpose. This one answers whether a subject
// may hold a socket at all; the other answers whether they may hear one
// channel, and it is asked again for every channel they name. Issuing one Grant
// for both would mean the per-channel policy never runs, which is RULE 17
// undone by a constant.
const Connect auth.Action = "joaju.connect"

// The prefixes a Pusher cache channel carries.
//
// broadcasting.PrivateChannelPrefix and broadcasting.PresenceChannelPrefix
// already name the other two, and this package uses those rather than spelling
// them a second time (RULE 9). Only these three are new, because the SSE fanout
// hesape/broadcasting was written for has no cache channel.
//
// A cache channel replays the last event it carried to whoever subscribes next,
// so a client that connects after the event still sees the current state
// instead of waiting for the next one.
const (
	// CacheChannelPrefix marks a public cache channel.
	CacheChannelPrefix = "cache-"
	// PrivateCacheChannelPrefix marks a cache channel that must be authorized.
	PrivateCacheChannelPrefix = broadcasting.PrivateChannelPrefix + CacheChannelPrefix
	// PresenceCacheChannelPrefix marks a cache channel that publishes its
	// members.
	PresenceCacheChannelPrefix = broadcasting.PresenceChannelPrefix + CacheChannelPrefix
)

// The two reserved namespaces of the Pusher protocol.
//
// An event a client publishes may use neither: the first is the protocol's own
// traffic, and the second is the server talking about channel state. A client
// that could send pusher_internal:member_added could invent members.
const (
	// ProtocolPrefix is the namespace of the protocol's own events.
	ProtocolPrefix = "pusher:"
	// InternalPrefix is the namespace of events only the server may send.
	InternalPrefix = "pusher_internal:"
)

// The protocol event names.
//
// The four the Reverb clone spells are [EventConnectionEstablished],
// [EventError], [EventMemberAdded] and [EventMemberRemoved]. The rest are the
// Pusher protocol's, and are here so that the two ends of this repository --
// the code that reads a frame and the code that writes one -- name them once.
const (
	// EventConnectionEstablished carries the [SocketID] to a client that has
	// just connected. It is the first frame the server sends, and the client
	// needs it: the socket id is what it puts in a publish so its own broadcast
	// does not come back to it.
	EventConnectionEstablished = ProtocolPrefix + "connection_established"
	// EventError reports a refusal. A subscription denied by a Policy is one.
	EventError = ProtocolPrefix + "error"
	// EventSubscribe is a client asking to listen on a channel.
	EventSubscribe = ProtocolPrefix + "subscribe"
	// EventUnsubscribe is a client asking to stop.
	EventUnsubscribe = ProtocolPrefix + "unsubscribe"
	// EventPing is a client checking the socket is alive.
	EventPing = ProtocolPrefix + "ping"
	// EventPong answers [EventPing].
	EventPong = ProtocolPrefix + "pong"

	// EventSubscriptionSucceeded confirms a subscription and carries
	// [Channel.Data], which for a presence channel is the member list.
	EventSubscriptionSucceeded = InternalPrefix + "subscription_succeeded"
	// EventMemberAdded tells a presence channel a member joined.
	EventMemberAdded = InternalPrefix + "member_added"
	// EventMemberRemoved tells a presence channel a member left.
	EventMemberRemoved = InternalPrefix + "member_removed"
)

// ErrNoChannel is what [Broker.Find] answers when no channel is held under the
// name.
var ErrNoChannel = errors.New("joaju: no such channel")

// ErrNoGrant is what a constructor answers when the Grant it was handed
// authorized nothing.
//
// It wraps auth.ErrForbidden, so a caller that already distinguishes a refusal
// from a failure keeps doing so.
var ErrNoGrant = fmt.Errorf("%w: joaju: no grant", auth.ErrForbidden)

// ErrWrongTenant is what a [Broker] answers when the Grant it was handed and
// the [ChannelName] it was handed disagree about whose channel it is.
//
// It cannot happen through [NewChannelName], which reads the tenant off the
// Grant. It can happen when a name built under one Grant is used with another,
// which is the mistake worth a distinct error: both values are valid, and
// nothing but this comparison notices.
// ErrConnectionLimit is the tenant already holding as many sockets as
// [ServerConfig.MaxConnections] allows.
//
// It is refused AFTER the upgrade, not before, and that is on purpose: the
// client is a websocket by then, so it can be told why in a protocol frame it
// understands instead of getting an HTTP status it has no handler for. Reverb
// answers the same way, with ConnectionLimitExceeded.
//
// It is not an auth.ErrForbidden. Nothing was refused on authority -- the same
// client with the same Grant is admitted the moment somebody disconnects.
var ErrConnectionLimit = errors.New("joaju: this tenant is holding as many connections as it may")

var ErrWrongTenant = fmt.Errorf("%w: joaju: the grant and the channel belong to different tenants", auth.ErrForbidden)

// SocketID identifies one open socket, and is the id the protocol calls
// socket_id.
//
// It is handed to the client in [EventConnectionEstablished] and comes back in
// a publish as the sender, so [Channel.Broadcast] can skip it. Pusher's clients
// expect the shape "<digits>.<digits>"; nothing here reads it, and the package
// that mints one keeps to that shape because the clients print it.
type SocketID string

// ChannelType is which of the six kinds of channel a name denotes.
//
// The kind is read off the name's prefix rather than stored, because the client
// chooses it by what it asks for: a client that sends "private-orders.17" is
// asking for a private channel by naming one. [ChannelName.Type] is the rule,
// and it is the only place the prefixes are compared.
type ChannelType uint8

// The six channel kinds, which are Reverb's six Channel classes.
//
// Reverb's Channels directory holds a seventh file, ChannelBroker, which is not
// a kind of channel: it is the rule that picks one, and here that rule is
// [ChannelName.Type] and the thing that hands channels out is [Broker].
const (
	// PublicChannel is Reverb's Channel: no prefix, no authorization, anyone
	// connected may listen.
	//
	// It still cannot cross a tenant. Every name carries one (RULE 14), so
	// "public" means public within one customer's namespace and never wider.
	PublicChannel ChannelType = iota
	// PrivateChannel is "private-": a subscription a [SubscriptionPolicy] has
	// to allow.
	PrivateChannel
	// PresenceChannel is "presence-": private, and its subscribers are
	// published to each other as [Member] values.
	PresenceChannel
	// CacheChannel is "cache-": public, and it replays its last event to
	// whoever subscribes next.
	CacheChannel
	// PrivateCacheChannel is "private-cache-": [PrivateChannel] and
	// [CacheChannel] together.
	PrivateCacheChannel
	// PresenceCacheChannel is "presence-cache-": [PresenceChannel] and
	// [CacheChannel] together.
	PresenceCacheChannel
)

// String names the kind, for a log line and for the metrics routes.
func (t ChannelType) String() string {
	switch t {
	case PublicChannel:
		return "public"
	case PrivateChannel:
		return "private"
	case PresenceChannel:
		return "presence"
	case CacheChannel:
		return "cache"
	case PrivateCacheChannel:
		return "private-cache"
	case PresenceCacheChannel:
		return "presence-cache"
	}

	return fmt.Sprintf("channel type %d", uint8(t))
}

// Guarded reports whether a subscription to this kind of channel has to be
// authorized by a [SubscriptionPolicy].
//
// It is Reverb's UsePusherChannelConventions::isGuardedChannel, and it answers
// no for the public kinds -- which does not make them reachable across a
// tenant, because the tenant is in every name and comes from the Grant that
// built it.
func (t ChannelType) Guarded() bool {
	switch t {
	case PrivateChannel, PresenceChannel, PrivateCacheChannel, PresenceCacheChannel:
		return true
	}

	return false
}

// Presence reports whether this kind of channel publishes its subscribers to
// each other, which is what makes [Member] and [Channel.Data] mean anything.
func (t ChannelType) Presence() bool {
	return t == PresenceChannel || t == PresenceCacheChannel
}

// Cache reports whether this kind of channel replays its last event to whoever
// subscribes next.
func (t ChannelType) Cache() bool {
	switch t {
	case CacheChannel, PrivateCacheChannel, PresenceCacheChannel:
		return true
	}

	return false
}

// ChannelName is a channel name with its tenant already in it.
//
// It has only unexported fields, so it cannot be written as a struct literal
// with a tenant somebody chose. [NewChannelName] is the way to one, and it
// reads the tenant off an auth.Grant -- which is RULE 14 expressed as a type,
// the same shape auth.Grant itself uses to make an authorization decision
// unforgeable.
//
// It has two string forms and they are not interchangeable:
//
//	[ChannelName.String]    "acme:private-orders.17"  the key, and what Redis publishes on
//	[ChannelName.Requested] "private-orders.17"       what the client sent, and what goes back to it
//
// The client never sees the first. It asked for the second, it is answered
// about the second, and the tenant it was scoped to is not its to know or to
// choose.
//
// The zero value is not a channel. [ChannelName.IsZero] reports it and
// [ChannelName.String] answers the empty string rather than a bare separator,
// so a zero value cannot be mistaken for a channel named "".
type ChannelName struct {
	tenant    string
	requested string
}

// NewChannelName builds the name a channel is held under, out of the Grant and
// the raw name the client asked for.
//
// requested is exactly what came off the wire, prefix and all:
// "private-orders.17". It is refused if it is empty or contains
// broadcasting.TenantSeparator, because that is where the tenant goes and a
// client that names one is a client choosing whose events it hears. The Grant
// is refused if it carries no tenant, or one that cannot be a namespace.
//
// Both refusals are broadcasting.RequestedChannel and
// broadcasting.TenantChannel doing the work, because the SSE fanout already
// authenticates channel names and two definitions of a valid one are how the
// looser of the two gets found (RULE 9).
func NewChannelName(g auth.Grant, requested string) (ChannelName, error) {
	c, err := broadcasting.RequestedChannel(requested)
	if err != nil {
		return ChannelName{}, err
	}
	// The result is discarded and the error is not: this call is here to run
	// the tenant half of the same rule -- a Grant with no tenant, or with one
	// that cannot be a namespace, reaches no channel. Rebuilding the string
	// from the parts below rather than keeping this one is what lets
	// [ChannelName.Requested] answer without cutting the name back apart.
	if _, err := broadcasting.TenantChannel(g, c); err != nil {
		return ChannelName{}, err
	}

	return ChannelName{tenant: auth.Tenant(g), requested: c.Name}, nil
}

// Tenant is whose channel this is. It came from the Grant and from nowhere
// else.
func (n ChannelName) Tenant() string { return n.tenant }

// Requested is the name the client asked for, with no tenant in it.
//
// This is the value that goes back on the wire, in the "channel" field of every
// frame, because it is the name the client used and the only one it knows.
func (n ChannelName) Requested() string { return n.requested }

// String is the published name, "<tenant>:<channel>".
//
// It is the key a [Broker] holds the channel under and the name a message is
// published on for the other servers in the fleet, and it is never sent to a
// client.
func (n ChannelName) String() string {
	if n.IsZero() {
		return ""
	}

	return n.tenant + broadcasting.TenantSeparator + n.requested
}

// IsZero reports whether this is the zero value, which is not a channel.
func (n ChannelName) IsZero() bool { return n.tenant == "" || n.requested == "" }

// Type reads the kind off the prefix, which is Reverb's ChannelBroker::create.
//
// The order matters and is the reason this is one function: "private-cache-"
// also begins with "private-", so the compound prefixes are tested first. Two
// copies of this switch would be two copies of that ordering, and the second
// one is where a private cache channel quietly becomes a plain private one.
//
// It reads [ChannelName.Requested] and not [ChannelName.String], because the
// prefix is at the front of the name the client sent and the published name has
// the tenant in front of it. That exact mistake is on record in
// hesape/broadcasting: IsGuardedChannel asked the published name whether it
// began with "private-", so it answered false for every private channel it
// existed to protect.
func (n ChannelName) Type() ChannelType {
	switch name := n.requested; {
	case strings.HasPrefix(name, PrivateCacheChannelPrefix):
		return PrivateCacheChannel
	case strings.HasPrefix(name, PresenceCacheChannelPrefix):
		return PresenceCacheChannel
	case strings.HasPrefix(name, CacheChannelPrefix):
		return CacheChannel
	case strings.HasPrefix(name, broadcasting.PrivateChannelPrefix):
		return PrivateChannel
	case strings.HasPrefix(name, broadcasting.PresenceChannelPrefix):
		return PresenceChannel
	}

	return PublicChannel
}

// Sink is the write half of a socket: where a [Connection]'s bytes go.
//
// It is an interface so that this package declares no transport. The [ws]
// subpackage implements it over a real socket, and a test implements it over a
// slice.
//
// It is named for what it is in Go terms rather than after Reverb's names for
// the same two operations, which are methods on the Connection class itself --
// send() and terminate(). Splitting them out is what lets [Connection] be a
// struct with a constructor that cannot be bypassed: an interface cannot have
// one.
type Sink interface {
	// Send writes one message. The message is a complete protocol frame,
	// already encoded.
	Send(ctx context.Context, message []byte) error
	// Terminate closes the socket. It is Reverb's Connection::terminate.
	Terminate(ctx context.Context) error
}

// Connection is one client's open socket.
//
// It has only unexported fields and one constructor, so it cannot be written as
// a struct literal: [NewConnection] takes an auth.Grant and refuses a Grant
// that authorized nothing. That is the same shape auth.Grant uses on itself,
// and it is here for the same reason -- a Connection carries the tenant every
// channel name it reaches is built from, and a Connection assembled by hand
// would be a tenant somebody typed.
//
// It is a struct and not an interface because there is one kind of connection.
// What varies is where the bytes go, and that is [Sink].
type Connection struct {
	id    SocketID
	grant auth.Grant
	sink  Sink
}

// NewConnection is the only way to a [Connection].
//
// g must be a Grant issued for [Connect] -- that is, one an auth.Policy
// answered about this handshake. A zero Grant, or one issued for another
// action, is refused here rather than at the first channel the client names.
func NewConnection(g auth.Grant, id SocketID, sink Sink) (*Connection, error) {
	if err := g.Check(Connect); err != nil {
		return nil, fmt.Errorf("joaju: %w", err)
	}
	if auth.Tenant(g) == "" {
		return nil, fmt.Errorf("%w: the grant carries no tenant", ErrNoGrant)
	}
	if id == "" {
		return nil, errors.New("joaju: a connection needs a socket id")
	}
	if sink == nil {
		return nil, errors.New("joaju: a connection needs somewhere to write")
	}

	return &Connection{id: id, grant: g, sink: sink}, nil
}

// ID is the socket id, which is Reverb's Connection::id.
func (c *Connection) ID() SocketID { return c.id }

// Subject is who this socket was authenticated as.
func (c *Connection) Subject() auth.Subject { return c.grant.Subject() }

// Tenant is whose socket this is, read off the Grant and from nowhere else.
func (c *Connection) Tenant() string { return auth.Tenant(c.grant) }

// Grant is the proof the handshake was authorized.
//
// It is issued for [Connect] and it is not a subscription: a caller that has it
// still has to run a [SubscriptionPolicy] to reach a channel, because
// auth.Grant.Check refuses a Grant issued for another action.
func (c *Connection) Grant() auth.Grant { return c.grant }

// Send writes one encoded frame to this client.
func (c *Connection) Send(ctx context.Context, message []byte) error {
	return c.sink.Send(ctx, message)
}

// Terminate closes this client's socket.
func (c *Connection) Terminate(ctx context.Context) error {
	return c.sink.Terminate(ctx)
}

// Member is what a presence channel publishes about one subscriber.
//
// The field names are Pusher's, because they are the JSON keys a browser client
// reads out of [EventMemberAdded] and out of the presence block of
// [Channel.Data].
//
// UserID is not the tenant and is not authority. It says which person a
// subscriber is inside one customer's channel; whose channel it is was settled
// by the Grant that built the [ChannelName] before this value was looked at.
type Member struct {
	// UserID is user_id: who this subscriber is, within the tenant.
	UserID string `json:"user_id"`
	// Info is user_info: whatever else the application publishes about them,
	// left encoded because this package never looks inside it.
	Info json.RawMessage `json:"user_info,omitempty"`
}

// Subscriber is one [Connection]'s membership of one [Channel].
//
// It is Reverb's ChannelConnection, which exists there for the same reason:
// presence data belongs to the pair, not to the socket. One socket may be in a
// presence channel as one member and in another as a different one.
type Subscriber struct {
	// Conn is the socket.
	Conn *Connection
	// Member is the presence data, and is the zero value on a channel that is
	// not a presence channel.
	Member Member
}

// Event is what travels: one message, on one channel.
//
// It has no JSON tags, and that is deliberate -- it is the value this server
// passes around, not the frame. The protocol layer builds the frame, and two
// fields do not go out as they are held:
//
//   - Channel goes out as [ChannelName.Requested], never as
//     [ChannelName.String]. The client asked for "private-orders.17" and that
//     is the name it is answered about; the tenant is not its to see.
//   - Data goes out as a JSON string containing JSON, which is what the Pusher
//     protocol specifies and what Reverb's json_encode of the data field
//     produces. It is held decoded here so that nothing double-encodes twice.
type Event struct {
	// Name is the event name, and goes out in the frame's "event" field.
	// [ProtocolPrefix] and [InternalPrefix] are reserved: an event a client
	// published may use neither.
	Name string
	// Channel is where it goes.
	Channel ChannelName
	// Data is the payload, left encoded because this package never reads it.
	Data json.RawMessage
	// Socket is who sent it, and it exists so that they do not receive it.
	//
	// A client that publishes a message has already drawn its own result on
	// screen; delivering it back makes the message appear twice. The client
	// learns its own id from [EventConnectionEstablished] and puts it in the
	// publish. [Channel.Broadcast] skips it; [Channel.BroadcastToAll] ignores
	// this field, which is the only difference between the two.
	//
	// Empty means the event came from the server and goes to everyone.
	Socket SocketID
}

// Handshake is what a [ConnectPolicy] decides about: one client asking to open
// a socket.
//
// The origin is here rather than in configuration because refusing an origin is
// a decision about who may connect, and this repository has one place where
// those are made. Reverb keeps an allowed-origins list per application; a
// policy answers the same question and answers it beside every other one.
type Handshake struct {
	// Socket is the id minted for this client.
	Socket SocketID
	// Origin is the Origin header of the upgrade request, verbatim. It is what
	// the browser claims, so it is evidence for a policy and never a tenant.
	Origin string
}

// Subscription is what a [SubscriptionPolicy] decides about: one client asking
// to listen on one channel.
//
// The Channel is a [ChannelName], which means the tenant was settled before the
// policy ran. A policy is asked whether this subject may hear this channel; it
// is never asked whose channel it is, because that was not the client's to say.
type Subscription struct {
	// Channel is the channel asked for, tenant already in it.
	Channel ChannelName
	// Member is the presence data the client offered, and is the zero value
	// off a presence channel. It came from the client, so a policy that lets a
	// subscriber name their own Member.UserID is a policy that lets them
	// impersonate one -- compare it against the subject.
	Member Member
	// Socket is which of the subject's connections is asking.
	Socket SocketID
}

// ConnectPolicy decides whether a client may open a socket. Its Grant is issued
// for [Connect].
//
// It is an alias and not a new interface because there is one way to authorize
// in this ecosystem, and it is auth.Policy (RULE 9). The alias exists so the
// resource type is named at the point the decision is described.
type ConnectPolicy = auth.Policy[Handshake]

// SubscriptionPolicy decides whether a client may listen on a channel. Its
// Grant is issued for broadcasting.ChannelJoin, which hesape/broadcasting
// already declares for the SSE fanout's half of the same question.
//
// It runs for every channel, on every subscription, including a resubscription
// after a reconnect. Subscribing is a read, and RULE 17 has no exception for
// reads: a channel a policy never saw is a channel whose contents nobody
// decided anyone could have.
type SubscriptionPolicy = auth.Policy[Subscription]

// Channel is a group of sockets that receive the same events.
//
// The method names are Reverb's Channel class, so that the six kinds of channel
// read as what they are there. The differences are three:
//
//   - Subscribe takes a context and an auth.Grant. Reverb's takes a connection
//     and two optional strings, one of which is a signature it verifies itself.
//     A subscription that a Policy did not decide is the leak this repository
//     exists not to have, and a Grant in the signature is how the compiler asks
//     for the decision.
//   - Find takes a [SocketID], and so covers Reverb's find() and findById()
//     both.
//   - The reads carry no context and return no error, because the sockets in a
//     channel are the ones this process holds and looking at them is not I/O.
//     Everything that writes to a socket does both.
//
// Implementations are safe for concurrent use. One connection is one goroutine
// and a channel is shared between all of them, so the alternative is a lock
// every caller has to remember.
type Channel interface {
	// Name is the channel's name, with its tenant.
	Name() ChannelName

	// Connections is every current subscriber.
	//
	// Reverb returns a map keyed by connection id; this returns a slice,
	// because every caller of it iterates. [Channel.Find] is the keyed lookup.
	Connections() []Subscriber

	// Find is the subscriber on this channel with the given socket id, if it
	// is subscribed.
	Find(id SocketID) (Subscriber, bool)

	// Subscribe adds a connection to the channel.
	//
	// g must be a Grant issued for broadcasting.ChannelJoin by a
	// [SubscriptionPolicy] asked about this exact [Subscription], and its
	// tenant must be the tenant in [Channel.Name] -- otherwise
	// [ErrWrongTenant]. An implementation checks the Grant before it touches
	// its state, on every kind of channel and not only the guarded ones:
	// [ChannelType.Guarded] says whether a policy may allow a subscription
	// freely, not whether one is consulted.
	//
	// member is the presence data the client offered, and is ignored off a
	// presence channel. On a presence channel, subscribing announces the new
	// member to the others with [EventMemberAdded].
	Subscribe(ctx context.Context, g auth.Grant, conn *Connection, member Member) error

	// Unsubscribe removes a connection from the channel, and on a presence
	// channel announces its departure with [EventMemberRemoved].
	//
	// It takes no Grant. Leaving is not a read: nothing is disclosed by it, and
	// a socket that drops has no one left to ask.
	Unsubscribe(ctx context.Context, conn *Connection) error

	// Subscribed reports whether this connection is on this channel.
	Subscribed(conn *Connection) bool

	// Broadcast delivers the event to every subscriber except the one named by
	// [Event.Socket].
	Broadcast(ctx context.Context, e Event) error

	// BroadcastToAll delivers the event to every subscriber, [Event.Socket]
	// included. It is what the server itself sends on -- a member list, a
	// refusal -- where there is no sender to spare.
	BroadcastToAll(ctx context.Context, e Event) error

	// Data is what [EventSubscriptionSucceeded] carries to a new subscriber.
	//
	// It is empty on every kind of channel but a presence one, where it is
	// Reverb's presence block: {"presence": {"count": n, "ids": [...],
	// "hash": {...}}}. It stays a map because it is a payload on its way to
	// JSON and nothing in this repository reads a field out of it.
	Data() map[string]any
}

// Broker holds the channels and hands them out. It is the only way to one.
//
// It is Reverb's ChannelManager and its ChannelBroker at once. Those are two
// classes there because one picks the subclass by prefix and the other keeps
// the instances; here picking is [ChannelName.Type] and keeping is this, and a
// caller that could get a channel from either of two places would have two
// answers to compare (RULE 9).
//
// Every method takes an auth.Grant, and that is RULE 17 rather than ceremony.
// The metrics routes -- the channel list, one channel, its members -- are reads
// of who is talking to whom, and a read with no policy behind it is a tenant
// boundary that exists everywhere except the dashboard.
//
// Implementations are safe for concurrent use.
type Broker interface {
	// Find is the channel held under the name, or [ErrNoChannel].
	//
	// It returns [ErrWrongTenant] when auth.Tenant(g) is not
	// [ChannelName.Tenant] -- which [NewChannelName] cannot produce, and a
	// name carried over from another Grant can.
	Find(ctx context.Context, g auth.Grant, name ChannelName) (Channel, error)

	// FindOrCreate is Find, creating the channel of the kind
	// [ChannelName.Type] names when it is not there yet.
	//
	// It is what a subscription calls: the first subscriber to a channel is
	// what brings it into existence. Creating one discloses nothing and grants
	// nothing -- [Channel.Subscribe] still asks a [SubscriptionPolicy].
	FindOrCreate(ctx context.Context, g auth.Grant, name ChannelName) (Channel, error)

	// Remove drops the channel. An implementation calls it when the last
	// subscriber leaves, which is what Reverb's Channel::unsubscribe does.
	Remove(ctx context.Context, g auth.Grant, name ChannelName) error

	// All is every channel in the Grant's tenant, and never another tenant's.
	//
	// It answers GET /apps/{appId}/channels. A broker holding several tenants
	// filters here; the filter is the whole reason this takes a Grant.
	All(ctx context.Context, g auth.Grant) ([]Channel, error)
}
