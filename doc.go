// Package joaju is the WebSocket server: it holds open sockets, keeps them
// grouped into channels, and delivers a message to every socket in a group.
//
// This file declares the types the rest of the repository is written against.
// Nothing here opens a socket, parses a frame or serves an HTTP route; the
// packages that do those things take and return the values declared here.
//
// # The wire format
//
// The wire format is the Pusher protocol, which is why several names here are
// the protocol's rather than Go's: [Member] has user_id and user_info, the
// event names carry the "pusher:" and "pusher_internal:" prefixes, and a
// channel's kind is read off its name prefix. Those are bytes a browser client
// already knows how to read, and renaming them would mean shipping a client
// nobody else can talk to.
//
// One connection is one goroutine. There is no event loop to size, and the
// ceiling on concurrent sockets is the process's file descriptor limit.
//
// # The authorization shape, which is the point
//
// Two decisions happen, and each one is a Policy that issues an auth.Grant:
//
//	[Connect]                    opening the socket    -> [ConnectPolicy]
//	broadcasting.ChannelJoin     listening on one      -> [SubscriptionPolicy]
//
// They are two decisions and not one because they answer different questions.
// The first is whether this subject may hold a socket at all, and it is where
// an origin allowlist belongs -- see [Handshake]. The second is whether this
// subject may hear what goes out on one particular channel, and it is asked
// again for every channel, because subscribing is a read and no read reaches a
// channel without a policy behind it.
//
// A [Connection] cannot be built without a Grant, and a [ChannelName] cannot be
// built without one either. The tenant is enforced by the type and not by a
// review comment: it is read off the Grant by [NewChannelName], and the name
// the client sent is refused outright if it so much as contains the separator
// the tenant goes before. A client that could put "acme:" in front of a channel
// is a client choosing whose events it hears.
//
// There is deliberately no constructor that builds a [ChannelName] out of a
// name that already carries a tenant. The server that receives a relayed
// message over Redis pub/sub has such a name in hand, and the temptation is to
// parse the tenant back out of it -- which is the tenant coming off the wire.
// It compares the string against the [ChannelName.String] of the channels it
// already holds instead, and those were each built from a Grant.
//
// # What hesape already answers, and is not answered again here
//
// github.com/arandu-io/hesape/broadcasting owns the channel vocabulary shared
// with the SSE fanout, and this package imports it rather than restating it:
// broadcasting.PrivateChannelPrefix and broadcasting.PresenceChannelPrefix,
// broadcasting.TenantSeparator, broadcasting.RequestedChannel,
// broadcasting.TenantChannel and the broadcasting.ChannelJoin action are all
// used here as they stand. What is new is what SSE has no equivalent of: the
// three cache-channel prefixes, because SSE has no cache channel, and
// [MaxChannelNameLength] with [ChannelNameCharacters], because a name here is
// held for the life of a subscription and a name there is not held at all.
// Neither of those is a second opinion about what hesape already refuses --
// they are read after it, over the name it accepted.
//
// # What this package deliberately leaves out
//
// Multi-application is out of the first version: a Go binary is one process,
// and running one per application is both simpler to operate and free of the
// cross-application state the tenant rules exist to prevent.
//
// The protocol's private-encrypted- channels have no [ChannelType] of their
// own. End-to-end encrypted channels are a key distribution feature, not a
// channel kind: this server never holds the key and never reads a payload, so
// there is nothing about one it could report that private does not already say.
//
// The prefix is still read, and [ChannelName.Type] is where. What it is read
// for is the two properties this server does implement -- authorized, and
// replayed to whoever subscribes next -- so "private-encrypted-" is a
// [PrivateChannel] and "private-encrypted-cache-" is a [PrivateCacheChannel].
// Leaving it unread cost nothing on the first and a replay on the second, which
// is the shape of a prefix that is half known: the guarded half was right by
// accident, because it is the half "private-" already carried.
//
// [Channel] has no second broadcast method for an event this server was relayed
// rather than handed. Suppressing the re-cache of a replayed payload is the
// relay's business, and putting it here would give every implementation a
// second way to broadcast to explain.
//
// # The routes these types serve
//
// The nine routes of the Pusher HTTP API, which the HTTP layer of this
// repository answers with the values declared here:
//
//	GET  /app/{appKey}                                       the socket itself
//	POST /apps/{appId}/events                                publish one
//	POST /apps/{appId}/batch_events                          publish many
//	GET  /apps/{appId}/connections                           metrics
//	GET  /apps/{appId}/channels                              list
//	GET  /apps/{appId}/channels/{channel}                    one
//	GET  /apps/{appId}/channels/{channel}/users              presence members
//	POST /apps/{appId}/users/{userId}/terminate_connections  disconnect
//	GET  /up                                                 health
//
// Every one of the seven that reads or writes channel state needs a Grant, and
// [Broker] is why: there is no method on it that reaches a channel without one.
//
// # The transport
//
// Subpackage ws is this project's own implementation of RFC 6455, and it is why
// the dependency graph has one entry in it. It is not a fork and it borrows no
// code: the surface this server actually uses is ten symbols, so writing them
// costs less than carrying a library's client stack, proxy support and
// compression -- and less than tracking somebody else's security advisories
// forever.
//
// # The other end
//
// Subpackage client is the browser half: the JavaScript that speaks this
// protocol from a page, embedded and served rather than fetched from npm or a
// CDN. It is not routed by [Server] and the reason is in its own documentation
// -- the script has to be served from the origin the PAGE is on, and a socket
// server is frequently not that origin.
package joaju
