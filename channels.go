package joaju

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
)

// This file is the [Channel] implementation, and there is one of it.
//
// There are six kinds of channel and one type for them, because what actually
// varies between the kinds is two booleans, and both are already on the name:
//
//	[ChannelType.Presence]  publish the subscribers to each other
//	[ChannelType.Cache]     replay the last event to whoever subscribes next
//
// Six types would be six copies of the subscriber map, the lock, the delivery
// loop and the tenant check, differing in those two booleans -- and the tenant
// check is the one thing in this repository that may not exist in six copies,
// because five of them would be right. So there is one type and it reads
// [ChannelName.Type].
//
// What is NOT in this file: the wire format, and the frames the server sends
// around a subscription rather than through it.
//
// The frames are built by pusher.go -- [EventFrame], [MemberAdded],
// [MemberRemoved], [CacheMiss] -- and encoded by [Encode]. A channel decides
// who receives and pusher.go decides what the bytes look like, so the rule that
// [ChannelName.Requested] goes out and [ChannelName.String] never does is
// enforced in one function instead of in six.
//
// [EventSubscriptionSucceeded] carries [Channel.Data] and is the server
// answering the client that asked -- the channel is not involved, and it has no
// way to tell which frame is an answer to whom. [EventMemberAdded],
// [EventMemberRemoved] and the cache replay are here, because those go to
// somebody other than the asker and the channel is what knows who.

// NewChannel is the only way to a [Channel], and it makes the kind
// [ChannelName.Type] names.
//
// There is no argument that says which kind to build: the kind is in the name,
// the name came from a Grant, and a caller who could ask for a public channel
// under a private name would have found the way around [ChannelType.Guarded].
//
// The zero [ChannelName] is refused. It is what a failed [NewChannelName] hands
// back, and a channel built from one would be a channel with no tenant.
func NewChannel(name ChannelName) (Channel, error) {
	if name.IsZero() {
		return nil, errors.New("joaju: a channel needs a name, and the zero name is not one")
	}

	return &channel{name: name, subscribers: make(map[SocketID]Subscriber)}, nil
}

// channel is every kind of channel.
//
// It is safe for concurrent use, which [Channel] requires of an implementation:
// one connection is one goroutine, and every one of them reaches the same
// channel value.
//
// Nothing is sent while the lock is held. A Send writes to a socket, a slow
// client blocks it, and a lock held across that stops every other subscriber of
// the channel behind one reader who stopped reading. Each method takes what it
// needs out of the map, releases, and then delivers.
type channel struct {
	// name is fixed at construction and carries the tenant, so it is read
	// without the lock.
	name ChannelName

	mu          sync.RWMutex
	subscribers map[SocketID]Subscriber
	// cached is the last event broadcast on a cache channel, and cachedSet says
	// whether there has been one. A zero [Event] is not distinguishable from an
	// unset one by looking at it, and "there has been no event yet" has to be
	// answerable without guessing.
	cached    Event
	cachedSet bool
}

// Name is the channel's name, tenant included.
func (c *channel) Name() ChannelName { return c.name }

// Connections is every current subscriber, ordered by socket id.
//
// The order is not the protocol's business and no client sees it; it is sorted
// because the map is not ordered, and a metrics route that lists the same
// sockets in a different order on every request is a route nobody can diff.
func (c *channel) Connections() []Subscriber {
	c.mu.RLock()
	defer c.mu.RUnlock()

	subscribers := make([]Subscriber, 0, len(c.subscribers))
	for _, s := range c.subscribers {
		subscribers = append(subscribers, s)
	}
	slices.SortFunc(subscribers, func(a, b Subscriber) int {
		return strings.Compare(string(a.Conn.ID()), string(b.Conn.ID()))
	})

	return subscribers
}

// Find is the subscriber with this socket id, if it is on this channel.
func (c *channel) Find(id SocketID) (Subscriber, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	s, ok := c.subscribers[id]

	return s, ok
}

// Subscribed reports whether this connection is on this channel.
func (c *channel) Subscribed(conn *Connection) bool {
	if conn == nil {
		return false
	}
	_, ok := c.Find(conn.ID())

	return ok
}

// Subscribe adds a connection to the channel.
//
// Four things are checked before the map is touched, and the order is the order
// of what they protect:
//
//   - the Grant was issued for broadcasting.ChannelJoin, so a
//     [SubscriptionPolicy] answered about this subscription and not something
//     else. Subscribing is a read, and a read with no policy behind it is data
//     nobody decided anyone could have;
//   - the Grant's tenant is the channel's tenant. The tenant is already in the
//     name -- [NewChannelName] put it there off a Grant -- so this catches the
//     one case that construction cannot: a name built under one Grant, carried
//     to another;
//   - the connection's tenant is the channel's tenant. The socket's tenant came
//     off the socket's own Grant at [NewConnection], and a subscription is two
//     Grants meeting. Without this line a Grant for the right tenant would seat
//     a socket belonging to another one, and every event on the channel would
//     be delivered to it;
//   - the Grant authorizes the subject holding the socket. A policy that
//     answered about one person does not seat another.
//
// On a presence channel the member is required and announced to the others with
// [EventMemberAdded]; off one it is discarded, because a channel that is not a
// presence channel publishes nothing about who is listening. On a cache channel
// the last event is replayed to the new subscriber and to nobody else, or
// [EventCacheMiss] is sent when there has not been one.
//
// A socket already on the channel is not seated twice: its member data is
// updated and no arrival is announced. The Pusher protocol has one membership
// per socket per channel, and a client that resubscribes after a reconnect
// would otherwise announce itself again to people who never saw it leave.
//
// The error is about delivery, not about membership: once the checks pass the
// socket is on the channel, and a subscriber whose socket has since died does
// not undo that. The caller logs it; the reader loop is what removes a dead
// socket.
func (c *channel) Subscribe(ctx context.Context, g auth.Grant, conn *Connection, member Member) error {
	if conn == nil {
		return errors.New("joaju: a subscription needs a connection")
	}
	if err := g.Check(broadcasting.ChannelJoin); err != nil {
		return fmt.Errorf("joaju: subscribing to %s: %w", c.name.Requested(), err)
	}
	if auth.Tenant(g) != c.name.Tenant() {
		return fmt.Errorf("%w: the grant is for %q and the channel belongs to %q",
			ErrWrongTenant, auth.Tenant(g), c.name.Tenant())
	}
	if conn.Tenant() != c.name.Tenant() {
		return fmt.Errorf("%w: socket %s belongs to %q and the channel belongs to %q",
			ErrWrongTenant, conn.ID(), conn.Tenant(), c.name.Tenant())
	}
	if g.Subject().ID != conn.Subject().ID {
		return fmt.Errorf("%w: joaju: the grant authorizes %q and socket %s is held by %q",
			auth.ErrForbidden, g.Subject().ID, conn.ID(), conn.Subject().ID)
	}

	presence := c.name.Type().Presence()
	if presence {
		if member.UserID == "" {
			return fmt.Errorf("joaju: %s is a presence channel and the subscription names no member", c.name.Requested())
		}
	} else {
		member = Member{}
	}

	c.mu.Lock()
	_, seated := c.subscribers[conn.ID()]
	announce := presence && !seated && !c.holds(member.UserID)
	c.subscribers[conn.ID()] = Subscriber{Conn: conn, Member: member}
	var others []Subscriber
	if announce {
		others = c.gather(conn.ID())
	}
	cached, replayed := c.cached, c.cachedSet
	c.mu.Unlock()

	if seated {
		return nil
	}

	arrived := []Subscriber{{Conn: conn, Member: member}}

	var errs []error
	if announce {
		f, err := MemberAdded(c.name, member)
		errs = append(errs, c.emit(ctx, others, f, err))
	}
	switch {
	case !c.name.Type().Cache():
	case replayed:
		f, err := EventFrame(cached)
		errs = append(errs, c.emit(ctx, arrived, f, err))
	default:
		// Nothing has been broadcast here yet. A cache channel that stays
		// silent is indistinguishable from one that is slow, so the protocol
		// says so out loud and the client stops waiting.
		f, err := CacheMiss(c.name)
		errs = append(errs, c.emit(ctx, arrived, f, err))
	}

	return errors.Join(errs...)
}

// Unsubscribe removes a connection from the channel.
//
// It takes no Grant, which [Channel] says and this repeats because it looks
// like an omission: leaving discloses nothing, and a socket that has already
// dropped has nobody left to ask.
//
// On a presence channel the departure is announced with [EventMemberRemoved],
// and only when the member has no other socket left on the channel. One person
// with two tabs who closes one has not left, and telling the others they did
// would empty them out of a member list they are still in.
//
// Removing a socket that is not on the channel is not an error. The reader loop
// calls this when a socket dies, and it does not know which channels the socket
// had reached.
func (c *channel) Unsubscribe(ctx context.Context, conn *Connection) error {
	if conn == nil {
		return errors.New("joaju: unsubscribing needs a connection")
	}

	c.mu.Lock()
	gone, seated := c.subscribers[conn.ID()]
	if !seated {
		c.mu.Unlock()

		return nil
	}
	delete(c.subscribers, conn.ID())
	announce := c.name.Type().Presence() && !c.holds(gone.Member.UserID)
	var remaining []Subscriber
	if announce {
		remaining = c.gather("")
	}
	c.mu.Unlock()

	if !announce {
		return nil
	}

	f, err := MemberRemoved(c.name, gone.Member)

	return c.emit(ctx, remaining, f, err)
}

// Broadcast delivers the event to every subscriber except the one that sent it,
// which is [Event.Socket].
func (c *channel) Broadcast(ctx context.Context, e Event) error {
	return c.send(ctx, e, e.Socket)
}

// BroadcastToAll delivers the event to every subscriber, the sender included.
func (c *channel) BroadcastToAll(ctx context.Context, e Event) error {
	return c.send(ctx, e, "")
}

// Data is what [EventSubscriptionSucceeded] carries to a new subscriber.
//
// It is the protocol's presence block, and it is empty off a presence channel:
//
//	{"presence": {"count": 2, "ids": ["u1","u2"], "hash": {"u1": {...}, "u2": {...}}}}
//
// A member with two sockets is one member. count is how many people are
// listening, not how many tabs are open, and Pusher's clients draw it as a list
// of people.
//
// The ids are sorted, for the same reason [channel.Connections] is.
//
// Every member in it is a member of this channel, and this channel has one
// tenant: it was in [Channel.Name] before the first subscriber, and
// [channel.Subscribe] refuses a socket or a Grant from any other. There is no
// filtering to do here, and that is the point -- a member list that had to be
// filtered would be a member list that could be forgotten.
func (c *channel) Data() map[string]any {
	if !c.name.Type().Presence() {
		return map[string]any{}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	ids := make([]string, 0, len(c.subscribers))
	hash := make(map[string]json.RawMessage, len(c.subscribers))
	for _, s := range c.subscribers {
		if _, seen := hash[s.Member.UserID]; seen {
			continue
		}
		info := s.Member.Info
		if len(info) == 0 {
			// A member with nothing published about them is an empty object,
			// not a null: the clients index into this map.
			info = json.RawMessage("{}")
		}
		ids = append(ids, s.Member.UserID)
		hash[s.Member.UserID] = info
	}
	slices.Sort(ids)

	return map[string]any{
		"presence": map[string]any{
			"count": len(ids),
			"ids":   ids,
			"hash":  hash,
		},
	}
}

// send is the body of both broadcasts, and skip is the socket that does not
// receive -- the sender for [channel.Broadcast], nobody for
// [channel.BroadcastToAll].
//
// The empty socket id skips nothing, because [NewConnection] refuses a
// connection without one.
func (c *channel) send(ctx context.Context, e Event, skip SocketID) error {
	if err := c.accepts(e); err != nil {
		return err
	}
	f, err := EventFrame(e)
	if err != nil {
		return err
	}

	c.mu.Lock()
	// A cache channel replays the last event so that a client arriving late
	// sees the current state. The protocol's own traffic is not state:
	// replaying [EventMemberAdded] would tell a new subscriber that somebody
	// joined at a moment before they were listening.
	if c.name.Type().Cache() && !f.IsProtocol() && !f.IsInternal() {
		c.cached, c.cachedSet = e, true
	}
	recipients := c.gather(skip)
	c.mu.Unlock()

	return c.deliver(ctx, recipients, f)
}

// accepts refuses an event that is not this channel's.
//
// A tenant mismatch is [ErrWrongTenant] and is the one that matters: an event
// published on one customer's channel, handed to the channel of the same name
// belonging to another, is delivered to every subscriber there. It cannot come
// out of [NewChannelName], and it can come out of a lookup that matched on
// [ChannelName.Requested] -- which is why the [Broker] holds channels under
// [ChannelName.String] and why this compares both halves.
func (c *channel) accepts(e Event) error {
	if e.Name == "" {
		return fmt.Errorf("joaju: an event on %s needs a name", c.name.Requested())
	}
	if e.Channel.Tenant() != c.name.Tenant() {
		return fmt.Errorf("%w: the event is for %q and the channel belongs to %q",
			ErrWrongTenant, e.Channel.Tenant(), c.name.Tenant())
	}
	if e.Channel.Requested() != c.name.Requested() {
		return fmt.Errorf("joaju: an event for %q was handed to %q",
			e.Channel.Requested(), c.name.Requested())
	}

	return nil
}

// gather copies the subscribers out of the map, skipping one socket id.
//
// The caller holds the lock. It exists so that delivery happens after the lock
// is released, which is the rule this type is built on.
func (c *channel) gather(skip SocketID) []Subscriber {
	recipients := make([]Subscriber, 0, len(c.subscribers))
	for id, s := range c.subscribers {
		if id == skip {
			continue
		}
		recipients = append(recipients, s)
	}

	return recipients
}

// holds reports whether any socket on the channel belongs to this member.
//
// The caller holds the lock. It is what makes a member's arrival and departure
// about the person rather than about the tab.
func (c *channel) holds(userID string) bool {
	if userID == "" {
		return false
	}
	for _, s := range c.subscribers {
		if s.Member.UserID == userID {
			return true
		}
	}

	return false
}

// emit is [channel.deliver] over a frame one of the builders in pusher.go
// returned, so that the error of building it is answered once rather than at
// each of the four call sites.
func (c *channel) emit(ctx context.Context, to []Subscriber, f Frame, err error) error {
	if err != nil {
		return err
	}

	return c.deliver(ctx, to, f)
}

// deliver encodes the frame once and writes it to each recipient.
//
// One encoding for the whole channel is the reason a channel exists: the bytes
// are identical for everyone on it, and encoding them per socket would be the
// same message built a thousand times.
//
// The frame is built by pusher.go and encoded by [Encode], and this file has no
// second way to turn an [Event] into bytes: what goes out on the wire is one
// piece of code, which is also the piece that drops the tenant from the channel
// name.
//
// A socket that refuses the write does not stop the others. The errors are
// joined so the caller learns about all of them.
func (c *channel) deliver(ctx context.Context, to []Subscriber, f Frame) error {
	if len(to) == 0 {
		return nil
	}

	message, err := Encode(f)
	if err != nil {
		return fmt.Errorf("joaju: encoding %s on %s: %w", f.Event, c.name.Requested(), err)
	}

	var errs []error
	for _, s := range to {
		if err := s.Conn.Send(ctx, message); err != nil {
			errs = append(errs, fmt.Errorf("joaju: sending %s on %s to socket %s: %w",
				f.Event, c.name.Requested(), s.Conn.ID(), err))
		}
	}

	return errors.Join(errs...)
}
