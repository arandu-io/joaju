module github.com/arandu-io/joaju

go 1.26

// ws/ is this project's own WebSocket implementation of RFC 6455, and it is the
// reason the dependency list is as short as it is: the surface the server uses
// is ten symbols, and taking a library for them would bring a client stack, a
// buffer pool and permessage-deflate along with it.
//
// golang.org/x/net is what ws/proxy.go dials a SOCKS5 proxy with, and it is the
// one import here that is neither this project's nor the standard library's. It
// is confined to the client half of the transport: nothing the server does
// reaches it.
//
// The collection arrives when the server does: auth.Grant, because subscribing
// to a channel is a read and RULE 17 has no exception, and redis for the
// pub/sub that lets two instances agree on who is connected where.

require (
	github.com/arandu-io/hesape v0.9.0
	golang.org/x/net v0.26.0
)
