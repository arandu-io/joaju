module github.com/arandu-io/joaju

go 1.26

// joaju takes no third-party dependency, and the ws subpackage is why.
//
// ws/ is this project's own RFC 6455, not a fork and not borrowed code
// (ADR 0052). The surface the server uses is ten symbols; a websocket library
// carries a client stack with proxy support, permessage-deflate and buffer
// pooling on top of them, and one of those -- SOCKS5, which lives in
// golang.org/x/net/proxy -- is the only third-party import this repository would
// otherwise have had.
//
// The collection arrives when the server does: auth.Grant, because subscribing
// to a channel is a read and RULE 17 has no exception, and redis for the
// pub/sub that lets two instances agree on who is connected where.

require github.com/arandu-io/hesape v0.3.0
