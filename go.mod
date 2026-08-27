module github.com/arandu-io/joaju

go 1.26

// ws/ is this project's own WebSocket implementation of RFC 6455, and it is the
// reason the dependency list is as short as it is: the surface the server uses
// is nineteen names -- twelve of the package and seven methods of Conn -- and
// taking a library for them would bring a client stack, a buffer pool and
// permessage-deflate along with it.
//
// The standard library is the whole of it. There was a second import,
// golang.org/x/net, for a SOCKS5 proxy dialer that only the client half used;
// it left with the file that called it, and nothing the server does needed it.
//
// The collection arrives when the server does: auth.Grant, because subscribing
// to a channel is a read and RULE 17 has no exception, and redis for the
// pub/sub that lets two instances agree on who is connected where.

require github.com/arandu-io/hesape v0.17.0
