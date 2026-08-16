package joaju

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
)

// This file is the [Broker] implementation, and there is one of it.
//
// One channel's connections are already written -- they are the subscriber map
// inside [channel], in channels.go -- so what is left is the registry, and the
// whole of it is a map from a name to a [Channel].
//
// The map is flat and keyed by [ChannelName.String],
// "acme:private-orders.17": the tenant is in the key and it got there from a
// Grant, not from a path segment. Nesting it one level per tenant would leave
// the inner key free to be [ChannelName.Requested], and then two customers
// asking for "orders.17" would be two ways of spelling one key -- the single
// mistake this file cannot make, because the two channels it merged would be two
// customers reading each other's events.
//
// What is NOT here:
//
// Delivery. Nothing in this file writes to a socket, which is why the rule
// channels.go is built on -- nothing is sent with the lock held -- does not need
// restating as a discipline here. The one call this file makes into a [Channel]
// is a read, and [Channel] says the reads are not I/O.
//
// The decision about who may be seated. Reaching a channel and joining it are
// two operations, and [channel.Subscribe] makes the second one. What this file
// decides is the first: the Grant it was handed was issued by a
// [SubscriptionPolicy], and it is for the tenant that owns the name.
//
// The [Observer]. [Observer.ChannelCreated] and [Observer.ChannelRemoved] are
// announced by whoever holds an Observer, which is the [Server].
// [NewMemoryBroker] takes no argument and so has none to announce to: a registry
// that also announced would be a second place those two events come from, and
// they would disagree the first time one of the two paths was taken alone.

// NewMemoryBroker is the [Broker] a server runs on: the channels this process
// holds, in a map.
//
// Memory is in the name because it is the thing to know before running a second
// instance, not a hedge against a stored one arriving later. A channel is a set
// of open sockets and a socket is held by one process, so the channels of an
// instance cannot be anywhere but in it. What crosses instances is the events,
// over [Relay], and the metrics routes ask the fleet rather than a shared store.
//
// It takes no argument. There is nothing to configure about a map, and an option
// on it would be a second way to build the one registry this package has.
func NewMemoryBroker() Broker {
	return &memoryBroker{channels: make(map[string]Channel)}
}

// memoryBroker is the channels of one process, held under [ChannelName.String].
//
// It is safe for concurrent use, which [Broker] requires of an implementation:
// one connection is one goroutine, all of them subscribe through this one value,
// and a Go map written by two of them at once is a crash rather than a wrong
// answer.
//
// The lock is a sync.RWMutex over a plain map, which is what [channel] uses one
// level down, and the register is the same on purpose. A sync.Map would answer
// the reads without a lock and would cost this file the two writes that have to
// be indivisible: [memoryBroker.FindOrCreate] has to look and insert as one
// step, or one name means two channels; [memoryBroker.Remove] has to ask a
// channel whether it is empty and delete it as one step, or it deletes one that
// filled up in between. LoadOrStore is the first of those and there is no
// primitive for the second, so the lock would end up here anyway, next to a
// second kind of map.
type memoryBroker struct {
	mu       sync.RWMutex
	channels map[string]Channel
}

// Find is the channel held under the name, or [ErrNoChannel].
//
// The lookup is by [ChannelName.String] and never by [ChannelName.Requested].
// The second is the name the client sent, every customer sends the same ones,
// and a map keyed by it would hand acme's orders channel to globex -- with no
// error anywhere, because both asked for a channel that exists.
//
// The error names the requested half. It reaches a log line and not the client
// ([Server.fail] answers 404 with no detail), and the tenant is not news to the
// operator reading it.
func (b *memoryBroker) Find(_ context.Context, g auth.Grant, name ChannelName) (Channel, error) {
	if err := b.accepts(g, name); err != nil {
		return nil, err
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	held, ok := b.channels[name.String()]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoChannel, name.Requested())
	}

	return held, nil
}

// FindOrCreate is [memoryBroker.Find], creating the channel when it is not there
// yet.
//
// All of it happens under the write lock, [NewChannel] included. The shape that
// reads faster -- look under a read lock, insert under a write one -- is not
// written here: between the two locks another goroutine creates the same
// channel, so the insert has to look a second time, and an insert that forgets
// the second look replaces a channel that already has subscribers on it. They
// stay seated on a value nothing hands out any more, hear nothing from then on,
// and are told nothing. Building a channel is a map header and a struct; holding
// the lock across that costs less than one name meaning two channels.
//
// Creating one grants nothing. The Grant checked here says the caller may reach
// a channel of this tenant, and [Channel.Subscribe] asks the
// [SubscriptionPolicy] again before anybody is seated.
func (b *memoryBroker) FindOrCreate(_ context.Context, g auth.Grant, name ChannelName) (Channel, error) {
	if err := b.accepts(g, name); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	key := name.String()
	if held, ok := b.channels[key]; ok {
		return held, nil
	}

	made, err := NewChannel(name)
	if err != nil {
		return nil, err
	}
	b.channels[key] = made

	return made, nil
}

// Remove drops the channel, and drops it only when nobody is left on it.
//
// This is where a channel stops existing, and the emptiness is checked here
// rather than believed from the caller. [Broker.Remove] says an implementation
// calls it when the last subscriber leaves, and those are two operations: the
// caller unsubscribed a socket, saw the channel go empty and called this, and in
// between a subscription may have seated somebody. Dropping it then leaves that
// subscriber on a value nothing hands out any more -- the next
// [memoryBroker.FindOrCreate] builds a second channel under the same name, every
// publish goes to that one, and the socket on the first hears nothing and is
// told nothing. So the question and the delete are one critical section.
//
// The check is made here and not from inside the channel: [NewChannel] takes
// nothing but a name, so a channel does not know this type exists -- which is
// also what makes the nesting below safe. The channel's lock is taken under
// this one and never the other way round.
//
// Asking is [Channel.Connections], the only thing the interface offers for it,
// and it is enough: it is a read, [Channel] says the reads of a channel are not
// I/O, and the case this runs in is the empty one -- a channel that just lost
// its last subscriber copies no subscribers out.
//
// Removing a channel nobody made is not an error, for the reason
// [channel.Unsubscribe] gives about a socket that is not seated: whoever calls
// this is unwinding a subscription and does not know what has already unwound.
// A channel that filled up again is the same answer -- nothing was left in a
// state anyone has to be told about.
func (b *memoryBroker) Remove(_ context.Context, g auth.Grant, name ChannelName) error {
	if err := b.accepts(g, name); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	key := name.String()
	held, ok := b.channels[key]
	if !ok {
		return nil
	}
	if len(held.Connections()) > 0 {
		return nil
	}
	delete(b.channels, key)

	return nil
}

// All is every channel of the Grant's tenant, and never another tenant's.
//
// The filter is the method, not a detail of it. This map holds every tenant's
// channels, because one process serves them all, and the route behind this is
// GET /apps/{appId}/channels -- a customer listing their own. Returning the map
// is the shorter function and it publishes the channel names of every other
// customer to whoever asks first.
//
// Whose a channel is comes from [ChannelName.Tenant], read off the channel's own
// name. Not from the key: the key begins with the tenant, and cutting it back
// out would be parsing a tenant out of a string, which doc.go says this package
// deliberately has no constructor for.
//
// The order is [ChannelName.String], for the reason [channel.Connections] is
// ordered: a map is not, and a metrics route that answers the same channels in a
// different order on every request is one nobody can diff. It is sorted after
// the lock is released, because it is the only work here that grows with the
// size of the process.
func (b *memoryBroker) All(_ context.Context, g auth.Grant) ([]Channel, error) {
	tenant, err := b.tenantOf(g)
	if err != nil {
		return nil, err
	}

	b.mu.RLock()
	all := make([]Channel, 0, len(b.channels))
	for _, held := range b.channels {
		if held.Name().Tenant() != tenant {
			continue
		}
		all = append(all, held)
	}
	b.mu.RUnlock()

	slices.SortFunc(all, func(a, b Channel) int {
		return strings.Compare(a.Name().String(), b.Name().String())
	})

	return all, nil
}

// tenantOf is the check the Grant itself gets, and answers whose channels the
// caller is about to see.
//
// The action is broadcasting.ChannelJoin, which is what a [SubscriptionPolicy]
// issues and what server.go means where it says the Grant a policy issued is the
// only thing a Broker accepts. auth.Grant.Check refuses the zero Grant as well,
// and that is the one a caller outside hesape/auth can build by writing a struct
// literal: it reaches no channel here.
//
// A Grant carrying no tenant is refused rather than read as every tenant. It
// would list nothing today, because every channel held here was named from a
// Grant that had one, and "nothing" is the right answer for the wrong reason --
// there is no Grant without a tenant, and this says so instead of relying on
// the map to stay a certain shape.
func (b *memoryBroker) tenantOf(g auth.Grant) (string, error) {
	if err := g.Check(broadcasting.ChannelJoin); err != nil {
		return "", fmt.Errorf("joaju: reaching a channel: %w", err)
	}

	tenant := auth.Tenant(g)
	if tenant == "" {
		return "", fmt.Errorf("%w: the grant carries no tenant", ErrNoGrant)
	}

	return tenant, nil
}

// accepts is [memoryBroker.tenantOf] and the name, and is what the three methods
// that name a channel check before they touch the map.
//
// The tenant comparison is what [ErrWrongTenant] exists for, and it catches the
// one case [NewChannelName] cannot: a name built under one Grant and carried to
// another. Both values are valid and nothing but this line notices. It is
// [channel.Subscribe]'s second check made again, because reaching a channel and
// being seated on it are two operations and the read needs a policy too.
//
// The zero name is refused by name rather than left to the comparison below,
// which would also refuse it -- with a sentence that has a hole where the
// channel should be. It is the refusal [NewChannel] and [Relay.Leave] make.
func (b *memoryBroker) accepts(g auth.Grant, name ChannelName) error {
	tenant, err := b.tenantOf(g)
	if err != nil {
		return err
	}
	if name.IsZero() {
		return errors.New("joaju: the zero channel name is not a channel, and carries no tenant")
	}
	if tenant != name.Tenant() {
		return fmt.Errorf("%w: the grant is for %q and %s belongs to %q",
			ErrWrongTenant, tenant, name.Requested(), name.Tenant())
	}

	return nil
}
