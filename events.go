package joaju

import "context"

// Observer is told what the server did, after it did it.
//
// It exists so that an application can count, log or audit without the server
// knowing about counting, logging or auditing. There is no event dispatcher in
// this ecosystem, so the destination is a value somebody passes in.
//
// # Every method runs after the fact, and its return value is ignored
//
// None of these can refuse anything. A channel is created because somebody
// authorised a subscription; a message is sent because it was already
// broadcast. An observer that could veto would be a second authorisation path,
// and there is one -- the [SubscriptionPolicy].
//
// # It must not block
//
// The server calls these on the path that serves connections. An observer that
// talks to a database on MessageSent turns every broadcast into a round trip,
// and a thousand messages a second into a thousand of them. Count in memory,
// and let something else read the counter.
//
// Nothing here is called with a lock held, so an observer may call back into
// the server without deadlocking. That is deliberate and it is tested.
type Observer interface {
	// ChannelCreated is the first subscription to a channel that did not exist.
	ChannelCreated(ctx context.Context, name ChannelName)

	// ChannelRemoved is the last subscriber leaving.
	ChannelRemoved(ctx context.Context, name ChannelName)

	// ConnectionOpened is a socket that finished the upgrade and was
	// authorised. A refused upgrade never reaches here.
	ConnectionOpened(ctx context.Context, id SocketID, tenant string)

	// ConnectionClosed is a socket that went away, for any reason: the client
	// closed it, the pong did not arrive in time, or the server terminated it.
	//
	// There is one event for every way a socket can end, and a reason string
	// to tell them apart: the read deadline does the pruning, so a pruned
	// connection is not a separate kind of closure.
	ConnectionClosed(ctx context.Context, id SocketID, tenant, reason string)

	// MessageReceived is a frame that arrived from a client, before it is acted
	// on. The raw bytes, because that is what a diagnosis wants.
	MessageReceived(ctx context.Context, id SocketID, message []byte)

	// MessageSent is a frame written to a client.
	MessageSent(ctx context.Context, id SocketID, message []byte)
}

// Reasons a connection ended, as [Observer.ConnectionClosed] receives them.
const (
	// ReasonClient is the client hanging up, which is the ordinary case.
	ReasonClient = "client"
	// ReasonTimeout is nothing heard within PongTimeout.
	ReasonTimeout = "timeout"
	// ReasonTerminated is POST /apps/{id}/users/{userId}/terminate_connections.
	ReasonTerminated = "terminated"
	// ReasonShutdown is the server closing.
	ReasonShutdown = "shutdown"
	// ReasonLimit is the connection limit, refused after the upgrade.
	ReasonLimit = "limit"
)

// NopObserver ignores everything, and is what a server without one uses.
//
// It exists so that the server never checks for nil on a path it runs per
// message. The check would be free; the branch in six places would not be, and
// a nil that reaches one of them is a panic on a live connection.
type NopObserver struct{}

func (NopObserver) ChannelCreated(context.Context, ChannelName)                {}
func (NopObserver) ChannelRemoved(context.Context, ChannelName)                {}
func (NopObserver) ConnectionOpened(context.Context, SocketID, string)         {}
func (NopObserver) ConnectionClosed(context.Context, SocketID, string, string) {}
func (NopObserver) MessageReceived(context.Context, SocketID, []byte)          {}
func (NopObserver) MessageSent(context.Context, SocketID, []byte)              {}

var _ Observer = NopObserver{}
