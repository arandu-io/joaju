package joaju

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
)

// This file is the [Protocol] implementation, and there is one of it.
//
// It is the state machine over the codecs in pusher.go: what a frame means once
// [Decode] has read it, and which frame answers it. Nothing here writes a byte
// of the wire format and nothing here decides who receives a message -- the
// first is pusher.go and the second is channels.go -- so what is left is the
// four events a client may send, the authorization each of them needs, and the
// record of which channels a socket reached, so that they can be left when it
// dies.
//
// laravel/reverb splits the same job across two classes, Protocols\Pusher\Server
// and Protocols\Pusher\EventHandler, and the split is its framework's rather
// than the protocol's: one dispatches and the other answers. The four events are
// one switch here, and the three methods below are the three that class has.
//
// # What is NOT here
//
// The socket. The upgrade, the two goroutines, the read deadline, the rate limit
// and the connection registry belong to [Server], which owns the transport and
// no part of the protocol. This type is handed a [Connection] and answers in
// frames.
//
// The tenant. It is on the [Connection]'s Grant before any of this runs, and
// [NewChannelName] is the only thing that reads it (RULE 14). The name a client
// sends is the second half of a channel name and never the first.
//
// Who receives a frame. [channel.Subscribe] announces [EventMemberAdded] to the
// others and replays a cache channel to the newcomer, because those go to
// somebody other than the asker and the channel is what knows who. This file
// sends the frames that answer the socket that asked: [EventConnectionEstablished],
// [EventSubscriptionSucceeded], [EventPong] and [EventError].
//
// # The decision, which is the point
//
// Every pusher:subscribe runs the [SubscriptionPolicy], on every kind of channel,
// public ones included. Subscribing is a read and RULE 17 opens no exception for
// reads; [ChannelType.Guarded] says whether a policy may allow a subscription
// freely, never whether one is asked. A refusal reaches the client as 4009 and
// nothing else -- the sentence the policy wrote names the subject and the
// channel, and it goes to the caller's log through the error this returns.

// PusherConfig is the rest of what [NewPusher] takes: the settings whose zero
// value is a decision rather than an omission.
//
// The two things the protocol cannot work without are arguments to the
// constructor and not fields here, because a field nobody filled in is a server
// that starts and a Grant nobody asked for.
type PusherConfig struct {
	// ActivityTimeout is how long a client may stay silent before it should send
	// [EventPing], and it is what [EventConnectionEstablished] carries. Zero
	// leaves the field out of the frame and the client falls back to its own
	// default.
	//
	// It is the protocol's number and not the socket's, which is why it is here
	// rather than read off [ServerConfig.PongTimeout]. That one is a read
	// deadline, and any frame at all resets it -- including the WebSocket ping
	// the writer sends on its own every [ServerConfig.PingInterval], which the
	// client's stack answers without the client knowing. This one is an
	// instruction a browser obeys, and it belongs below PongTimeout: a client
	// that pings after the deadline pings a socket that has already been hung up
	// on.
	ActivityTimeout time.Duration

	// ClientEvents is whether one browser's frame may be relayed to the others on
	// the channel. It is off in its zero value, which is Pusher's default and
	// Reverb's; [ClientEvents] is where the reason is written down.
	ClientEvents ClientEvents

	// Observer is told when a channel came into existence and when it went. Nil
	// means [NopObserver].
	//
	// It is the interface [ServerConfig.Observer] takes and an application hands
	// one value to both, because the two halves cannot be announced from one
	// place: a socket is opened and closed by the server, and a channel is
	// created and dropped by nothing but this.
	//
	// [Observer.MessageReceived] and [Observer.MessageSent] are deliberately not
	// announced from here. Only one of the two could be -- a frame a client sends
	// arrives at [Protocol.Message] unless the socket's rate limit dropped it,
	// while a frame a client receives is usually written by a [Channel]
	// delivering a broadcast, which this type never sees. Counting one side of a
	// conversation would show a server that receives a thousand messages and
	// sends none, and a number that is wrong in a knowable direction is worse
	// than the zero it replaces.
	Observer Observer
}

// NewPusher is the [Protocol] that speaks the Pusher protocol, and the one a
// [Server] is built with.
//
// broker is where a channel is reached and where one begins and ends: the first
// subscriber to a name brings the channel into existence and the last one to
// leave drops it, which is what [Broker.FindOrCreate] and [Broker.Remove] exist
// for. subscribe is asked about every subscription, on every kind of channel.
//
// It takes neither the app id, nor the socket's limits, nor a logger, and that
// is the line these arguments are drawn on: what the [Server] holds it holds
// because it owns the socket, a second copy here would be a second answer the
// first time a deployment changed one of them, and every method of [Protocol] is
// handed the [Connection] that carries the rest. What is left is [PusherConfig],
// which is what nothing else could tell this type.
//
// It answers a [Protocol] and no error, because there is nothing here that can
// fail. The one exception is a nil Broker or a nil SubscriptionPolicy, and it
// panics: that is a wiring mistake, it happens in the same breath as the
// [NewServer] call that would have refused the same omission in its config, and
// the alternative is a nil interface reached from a live socket's goroutine at
// the first subscription -- with a stack that names hesape/auth rather than
// whoever built this.
func NewPusher(broker Broker, subscribe SubscriptionPolicy, cfg PusherConfig) Protocol {
	switch {
	case broker == nil:
		panic("joaju: a Pusher protocol needs a Broker, which is the only way to a channel")
	case subscribe == nil:
		panic("joaju: a Pusher protocol needs a SubscriptionPolicy, because subscribing to a channel is a read")
	}

	p := &pusher{
		broker:    broker,
		subscribe: subscribe,
		events:    cfg.ClientEvents,
		timeout:   cfg.ActivityTimeout,
		observer:  cfg.Observer,
		seats:     make(map[SocketID]map[string]seat),
	}
	if p.observer == nil {
		// Never nil, for the reason [NopObserver] gives: the branch would be free
		// and the nil that reaches one of the four call sites is a panic on a live
		// connection.
		p.observer = NopObserver{}
	}

	return p
}

// seat is one socket's membership of one channel, as this type remembers it.
//
// The [Channel] is kept rather than looked up again because leaving takes no
// Grant -- [Channel.Unsubscribe] says so -- and a socket that has already
// dropped has nobody left to ask a [SubscriptionPolicy] about.
//
// The Grant is kept for the one call at the end that does need one:
// [Broker.Remove], which drops the channel the last subscriber just left. It is
// the Grant a policy issued for this socket on this channel, so the tenant on it
// is the channel's and the action is broadcasting.ChannelJoin. Reaching for a
// fresh one at close time would mean asking a policy about a subject whose
// socket is already gone, and a policy that has since changed its mind would
// leave the channel behind forever.
type seat struct {
	name    ChannelName
	channel Channel
	grant   auth.Grant
}

// pusher is the Pusher protocol over one server's sockets.
//
// It is safe for concurrent use, which [Protocol] requires: calls about one
// connection are ordered and calls about different ones are not, and all of them
// reach this one value.
//
// The lock covers the seats and the two ends of a channel's life. It is taken
// where a subscription arrives or leaves and nowhere else -- never on a
// broadcast, and never across a write to a socket. That is the rule [channel] is
// built on and it is here for the same reason: a Send blocks on a client that
// stopped reading, and a lock held across one stops every other socket's
// subscription behind the one that went quiet.
//
// It is taken before the [Broker]'s lock and before a [Channel]'s, never after.
// Neither of those knows this type exists -- [NewChannel] takes a name and
// [NewMemoryBroker] takes nothing -- so there is no path on which the order
// inverts.
type pusher struct {
	broker    Broker
	subscribe SubscriptionPolicy
	events    ClientEvents
	timeout   time.Duration
	// observer is never nil. See [NopObserver].
	observer Observer

	mu sync.Mutex
	// seats is what each socket is on, keyed by [ChannelName.String] -- the
	// tenant included, for the reason [memoryBroker] is keyed by it: every
	// customer's clients ask for the same names.
	//
	// Reverb keeps the same record in its ChannelManager, which can walk every
	// channel looking for a connection. [Broker] offers no such walk and should
	// not: it is a scan of every channel in the process on every socket that
	// closes, and what it recovers is a map this type can keep as it goes.
	seats map[SocketID]map[string]seat
}

var _ Protocol = (*pusher)(nil)

// Open sends [EventConnectionEstablished], which hands the client the socket id
// it will quote back when it publishes.
//
// It is the first frame on the socket and the client waits for it: the id is
// what excludes a publisher from its own broadcast, and pusher-js reports a
// connection only once this has arrived. An error closes the socket, which
// [Protocol] settled -- a client that never learned its id cannot use the
// connection for anything.
func (p *pusher) Open(ctx context.Context, conn *Connection) error {
	return p.write(ctx, conn, ConnectionEstablished(conn.ID(), p.timeout))
}

// Message reads one frame and answers it.
//
// The closed list of what a socket may say is [Frame.ClientMaySend] and it is
// asked first, so this switch never decides that a frame is allowed -- which is
// why it has no default case. A pusher_internal: event refused there is the one
// that matters: a client able to send pusher_internal:member_added is a client
// able to invent the members of a channel it is on.
//
// A refused frame is not a refused socket. Every path out of here that failed
// goes through [pusher.refuse], the client is told in [EventError], and it goes
// on using the channels it already has. The error is returned so the server logs
// it, and the server reads the next frame.
func (p *pusher) Message(ctx context.Context, conn *Connection, message []byte) error {
	f, err := Decode(message)
	if err != nil {
		return p.refuse(ctx, conn, err)
	}
	if err := f.ClientMaySend(); err != nil {
		return p.refuse(ctx, conn, err)
	}

	switch {
	case f.IsClientEvent():
		err = p.whisper(ctx, conn, f)
	case f.Event == EventSubscribe:
		err = p.join(ctx, conn, f)
	case f.Event == EventUnsubscribe:
		err = p.part(ctx, conn, f)
	case f.Event == EventPing:
		err = p.write(ctx, conn, Pong())
	case f.Event == EventPong:
		// Nothing is owed to a pong. It is a client saying it is still there, and
		// what that proves has already been recorded: the server reset the read
		// deadline when it read the frame, before this was called. Reverb calls
		// touch() here, which is the same deadline in the same place.
	}
	if err != nil {
		return p.refuse(ctx, conn, err)
	}

	return nil
}

// Close takes the socket off every channel it reached.
//
// It is Reverb's unsubscribeFromAll, and it is where a presence channel learns
// that somebody's last tab closed: [channel.Unsubscribe] sends
// [EventMemberRemoved] to the others. A channel left with nobody on it is
// dropped, and [Observer.ChannelRemoved] says so.
//
// It answers nothing, which [Protocol] settled, and what it swallows is worth
// naming: the failures here are writes to OTHER people's sockets. Each of those
// is a socket with a reader loop of its own, and that loop is what notices --
// there is nothing this call could do about it that is not already being done to
// the socket that failed.
func (p *pusher) Close(ctx context.Context, conn *Connection) {
	for _, s := range p.forget(conn.ID()) {
		_ = p.unseat(ctx, conn, s)
	}
}

// join is pusher:subscribe: the client asking to listen on a channel.
//
// The order is the order of what each step protects. The name is built from the
// socket's own Grant, so the tenant is settled before a policy is asked about
// anything (RULE 14). The [SubscriptionPolicy] runs next, on every kind of
// channel, and it is asked about the [Member] the client offered as well -- a
// policy that does not compare it against the subject is a policy that lets a
// subscriber join a presence channel as somebody else, which is why
// [Subscription] carries it. Only then is a channel reached, and
// [channel.Subscribe] asks the Grant again before it seats anybody.
//
// The confirmation goes out last, and it carries [Channel.Data] -- which on a
// presence channel is the member list, this subscriber included. That is why it
// cannot be sent before the seat: the client draws the list from this frame, and
// a list built a moment earlier is a list the new member is missing from.
//
// It follows that on a cache channel the replayed event, and on a presence
// channel nothing, arrive BEFORE [EventSubscriptionSucceeded] -- Reverb sends
// the confirmation first because its cache replay is in the same class that
// sends the confirmation. Here the replay belongs to the channel (channels.go),
// which cannot know which frame is an answer to whom, and a client that bound
// its handlers before subscribing reads both either way.
func (p *pusher) join(ctx context.Context, conn *Connection, f Frame) error {
	request, err := f.Subscribe()
	if err != nil {
		return err
	}
	member, err := request.Member()
	if err != nil {
		return err
	}

	name, err := NewChannelName(conn.Grant(), request.Channel)
	if err != nil {
		return err
	}
	grant, err := auth.Authorize(ctx, p.subscribe, conn.Subject(), broadcasting.ChannelJoin, Subscription{
		Channel: name,
		Member:  member,
		Socket:  conn.ID(),
	})
	if err != nil {
		return err
	}

	channel, created, err := p.reach(ctx, grant, name)
	if err != nil {
		return err
	}
	if created {
		p.observer.ChannelCreated(ctx, name)
	}

	held := seat{name: name, channel: channel, grant: grant}
	if err := channel.Subscribe(ctx, grant, conn, member); err != nil {
		// A subscription that failed leaves nothing behind. The seat is not
		// recorded, and the channel this call may have just created is dropped if
		// it is still empty -- otherwise a client subscribing to a presence
		// channel without channel_data, which [channel.Subscribe] refuses, would
		// leave one channel in the process for every name it cared to invent.
		//
		// The unseat error is dropped and this one returned, because this one is
		// the cause and the other one is what unwinding it cost.
		_ = p.unseat(ctx, conn, held)

		return err
	}
	p.take(conn.ID(), held)

	confirmation, err := SubscriptionSucceeded(name, channel.Data())
	if err != nil {
		return err
	}

	return p.write(ctx, conn, confirmation)
}

// part is pusher:unsubscribe: the client asking to stop listening.
//
// Nothing is sent back. The Pusher protocol has no confirmation for leaving, and
// a client that unsubscribes from a channel it is not on is not making a mistake
// -- it is a reconnect that raced its own cleanup. Reverb answers the same way,
// with a null-safe call that does nothing when the channel is not there.
func (p *pusher) part(ctx context.Context, conn *Connection, f Frame) error {
	requested, err := f.Unsubscribe()
	if err != nil {
		return err
	}
	name, err := NewChannelName(conn.Grant(), requested)
	if err != nil {
		return err
	}

	held, seated := p.forgetOne(conn.ID(), name)
	if !seated {
		return nil
	}

	return p.unseat(ctx, conn, held)
}

// whisper is a client- event: one browser publishing to the others on a channel.
//
// The three refusals are [ClientEvents.Accept]'s and they are made there rather
// than here, because that function is where the reasoning about them is written
// down: the switch being off is 4301, a channel no policy guarded is 4009, and a
// sender who is not on the channel is 4009. What this adds is the two facts
// Accept is given -- the [ChannelName], built from the socket's Grant so the
// tenant is never the sender's to choose, and whether the sender is on it.
//
// The seat is what answers the second, and [Channel.Find] confirms it: the
// record here says this socket subscribed, the channel says whether it is still
// seated, and what it hands back is the [Subscriber] it seated -- whose [Member]
// is who the sender is. One lookup answers both, and it has to be the same one:
// the user_id that goes out on the relayed frame is the channel's record of who
// took the seat, which is the only account of the sender's identity that the
// sender did not write. A socket with no seat for the name is not subscribed and
// the channel is never reached, so a client cannot use a client event to find out
// which channels exist.
//
// What goes out is [Channel.Broadcast] and not BroadcastToAll: [Event.Socket] is
// the sender and a browser that already drew its own message does not need it
// back.
func (p *pusher) whisper(ctx context.Context, conn *Connection, f Frame) error {
	name, err := NewChannelName(conn.Grant(), f.Channel)
	if err != nil {
		return err
	}

	held, seated := p.seatOf(conn.ID(), name)
	var sender Subscriber
	var subscribed bool
	if seated {
		sender, subscribed = held.channel.Find(conn.ID())
	}

	event, err := p.events.Accept(f, name, conn.ID(), sender.Member, subscribed)
	if err != nil {
		return err
	}

	return held.channel.Broadcast(ctx, event)
}

// reach is [Broker.FindOrCreate] and the answer it does not give: whether the
// channel it handed back had to be made.
//
// [Observer.ChannelCreated] is "the first subscription to a channel that did not
// exist", and the only call that would know says (Channel, error). So it is
// asked twice: [Broker.Find] answers [ErrNoChannel] for a name nothing holds,
// and what was missing a moment before FindOrCreate is what FindOrCreate
// created. Neither [Broker] nor [Channel] changes shape for it -- both are
// depended on elsewhere, and an interface widened for an announcement would be
// an interface every application implementing one has to grow a method for.
//
// The pair is one critical section, and that is the whole of the correctness
// here: two sockets subscribing to the same new channel would otherwise both
// find it missing and both announce it. A count that goes up twice and down once
// is a dashboard reporting channels nobody is on, forever.
//
// It is exact as long as nothing else creates a channel on this Broker, which is
// the design rather than an assumption: [Broker.FindOrCreate] is what a
// subscription calls, a subscription arrives on a socket, and every socket's
// frames arrive here. The API routes reach a channel with [Broker.Find] and
// create none.
//
// The mirror image of this is already in the repository. relayedBroker.Remove
// asks Find AFTER the removal, because Remove is a request and not a promise;
// this asks before, because FindOrCreate is a promise that says nothing about
// what it did.
//
// The announcement is the caller's, once the lock is released. [Observer] says
// nothing is called with a lock held, so that an observer may reach back into
// the server without deadlocking.
func (p *pusher) reach(ctx context.Context, g auth.Grant, name ChannelName) (Channel, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, err := p.broker.Find(ctx, g, name)
	missing := errors.Is(err, ErrNoChannel)
	if err != nil && !missing {
		return nil, false, err
	}

	channel, err := p.broker.FindOrCreate(ctx, g, name)
	if err != nil {
		return nil, false, err
	}

	return channel, missing, nil
}

// unseat takes one socket off one channel and drops the channel when it was the
// last one on it.
//
// The two steps are in this order because they are two questions:
// [Channel.Unsubscribe] is about the socket and announces [EventMemberRemoved]
// to whoever is left, and [pusher.drop] is about the channel. It is also the
// order that makes the second answerable -- a channel is only empty once the
// socket has gone.
//
// The delivery happens before the lock is taken and never under it, which is the
// rule this type shares with [channel].
func (p *pusher) unseat(ctx context.Context, conn *Connection, s seat) error {
	left := s.channel.Unsubscribe(ctx, conn)

	gone, err := p.drop(ctx, s)
	if gone {
		p.observer.ChannelRemoved(ctx, s.name)
	}

	return errors.Join(left, err)
}

// drop removes the channel the last subscriber just left, and answers whether it
// is really gone.
//
// The emptiness is asked first because [Broker.Remove] says an implementation
// calls it when the last subscriber leaves: a Broker that took the sentence at
// its word and deleted whatever it was handed would, without this, drop a
// channel that still has people on it every time anybody unsubscribed.
//
// And it is asked again afterwards, because Remove is a request and not a
// promise: [memoryBroker.Remove] keeps a channel that filled up again between
// the two calls, and announcing [Observer.ChannelRemoved] for a channel that is
// still there is a count that goes down while the thing it counts stays. This is
// relayedBroker.Remove's own check, made for the same reason one layer up.
func (p *pusher) drop(ctx context.Context, s seat) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(s.channel.Connections()) > 0 {
		return false, nil
	}
	if err := p.broker.Remove(ctx, s.grant, s.name); err != nil {
		return false, err
	}

	_, err := p.broker.Find(ctx, s.grant, s.name)

	return errors.Is(err, ErrNoChannel), nil
}

// take records that this socket is on this channel.
//
// A second subscription to the same channel replaces the first rather than
// adding to it, which is the membership [channel.Subscribe] keeps: one per
// socket per channel, so that a client resubscribing after a reconnect does not
// announce itself twice to people who never saw it leave.
func (p *pusher) take(id SocketID, s seat) {
	p.mu.Lock()
	defer p.mu.Unlock()

	held := p.seats[id]
	if held == nil {
		held = make(map[string]seat)
		p.seats[id] = held
	}
	held[s.name.String()] = s
}

// seatOf is this socket's membership of one channel, if it has one.
func (p *pusher) seatOf(id SocketID, name ChannelName) (seat, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	s, ok := p.seats[id][name.String()]

	return s, ok
}

// forgetOne drops one membership from the record and answers what it dropped.
//
// The record goes before the socket leaves the channel, and not after: the
// caller is about to deliver [EventMemberRemoved], and a membership still on the
// books during that delivery is one [pusher.Close] would leave a second time if
// the socket died in the middle of it.
func (p *pusher) forgetOne(id SocketID, name ChannelName) (seat, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	held := p.seats[id]
	s, ok := held[name.String()]
	if !ok {
		return seat{}, false
	}
	delete(held, name.String())
	if len(held) == 0 {
		// The socket's map goes when its last channel does, so a client that
		// subscribed once and left costs nothing for the rest of the connection.
		delete(p.seats, id)
	}

	return s, true
}

// forget drops everything this socket was on and answers all of it.
//
// The record is taken out in one step rather than iterated in place, so that
// what the caller then unsubscribes is a slice nobody else is writing to, and so
// that a socket closing twice -- which the server does not do, and which is one
// deferred call away from happening -- leaves its channels once.
func (p *pusher) forget(id SocketID) []seat {
	p.mu.Lock()
	defer p.mu.Unlock()

	held := p.seats[id]
	delete(p.seats, id)

	all := make([]seat, 0, len(held))
	for _, s := range held {
		all = append(all, s)
	}

	return all
}

// write encodes one frame and hands it to the socket.
//
// It is the only thing in this file that writes, so [Encode] is called in one
// place and the rule it enforces -- [ChannelName.Requested] goes out and
// [ChannelName.String] never does -- has one call site to hold here, as it has
// one in channels.go (RULE 9).
func (p *pusher) write(ctx context.Context, conn *Connection, f Frame) error {
	message, err := Encode(f)
	if err != nil {
		return fmt.Errorf("joaju: encoding %s for socket %s: %w", f.Event, conn.ID(), err)
	}
	if err := conn.Send(ctx, message); err != nil {
		return fmt.Errorf("joaju: sending %s to socket %s: %w", f.Event, conn.ID(), err)
	}

	return nil
}

// refuse tells the client that a frame was dropped, and keeps the socket.
//
// What the client is told is [ErrorFrame]'s to decide, and this is the reason it
// exists: every refusal in this file passes through one function, so no call
// site chooses how much of the cause to disclose. A [SubscriptionPolicy] names
// the subject and the channel in its refusal, and that sentence belongs in the
// caller's log -- which is where the returned error goes.
//
// The cause is returned even when the client was told, so that the two are the
// same event to whoever reads the log. A write that also fails is joined to it
// rather than replacing it: a socket that will not take the refusal is about to
// end anyway, and the first error is still why.
func (p *pusher) refuse(ctx context.Context, conn *Connection, cause error) error {
	if err := p.write(ctx, conn, ErrorFrame(cause)); err != nil {
		return errors.Join(cause, err)
	}

	return cause
}
