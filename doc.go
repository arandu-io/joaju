// Package joaju is the WebSocket server: it holds open sockets, keeps them
// grouped into channels, and delivers a message to every socket in a group.
//
// This file declares the types the rest of the repository is written against.
// Nothing here opens a socket, parses a frame or serves an HTTP route; the
// packages that do those things take and return the values declared here.
//
// # Where the shapes come from
//
// The structure is laravel/reverb's -- the route table, the channel kinds, the
// method names on a channel -- because a reader who knows Reverb should
// recognize this repository without reading it first (RULE 10). The
// implementation is not Reverb's and cannot be: Reverb is PHP on a ReactPHP
// event loop, whose default stream_select stops at 1024 connections. In Go a
// connection is a goroutine and the ceiling is the file descriptor limit, so
// there is no event loop to pick (ADR 0052).
//
// The wire format is the Pusher protocol, which is why several names here are
// Pusher's rather than Go's: [Member] has user_id and user_info, the event
// names carry the "pusher:" and "pusher_internal:" prefixes, and a channel's
// kind is read off its name prefix. Those are bytes a browser client already
// knows how to read, and renaming them would mean shipping a client nobody else
// can talk to.
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
// again for every channel, because subscribing is a read and RULE 17 opens no
// exception for reads.
//
// A [Connection] cannot be built without a Grant, and a [ChannelName] cannot be
// built without one either. That is RULE 14 as a type rather than a review
// comment: the tenant is read off the Grant by [NewChannelName], and the name
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
// with the SSE fanout, and this package imports it rather than restating it
// (RULE 9): broadcasting.PrivateChannelPrefix and
// broadcasting.PresenceChannelPrefix, broadcasting.TenantSeparator,
// broadcasting.RequestedChannel, broadcasting.TenantChannel and the
// broadcasting.ChannelJoin action are all used here as they stand. Only the
// three cache-channel prefixes are new, because SSE has no cache channel.
//
// # What Reverb has here and this package does not
//
// Reverb's Channel::findById is [Channel.Find]. Reverb needs both because its
// find() takes a Connection object and compares identity; here a connection is
// identified by its [SocketID] and one lookup covers both callers.
//
// Reverb's Channel::broadcastInternally is not on [Channel]. It exists there so
// that a CacheChannel replaying a payload it received from another server does
// not cache it a second time, and the distinction it draws is between an event
// this server was handed and an event it was relayed. That belongs to the relay
// and not to the channel interface, and putting it here would give every
// implementation a second broadcast method to explain (RULE 9).
//
// Reverb's ChannelManager::for($app) is not here either: multi-application is
// out of the first version, because a Go binary is one process and running one
// per application is both simpler to operate and free of the cross-application
// state RULE 14 exists to prevent (ADR 0052).
//
// Pusher's private-encrypted- channels have no [ChannelType]. Reverb has no
// class for them, and end-to-end encrypted channels are a key-distribution
// feature, not a channel kind.
//
// # The routes these types serve
//
// The nine routes of Reverb's Factory::pusherRoutes, which the HTTP layer of
// this repository answers with the values declared here:
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
// # The fork
//
// Subpackage websocket is a fork of gorilla/websocket, BSD-2-Clause, carried
// under THIRD_PARTY.md (ADR 0052). It is the only third-party code in the
// repository, and forking it rather than depending on it is what keeps the
// dependency graph at one entry.
package joaju
