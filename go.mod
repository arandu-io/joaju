module github.com/arandu-io/joaju

go 1.26

// joaju takes no third-party dependency, and the fork is why.
//
// websocket/ is gorilla/websocket, forked rather than required (ADR 0052).
// Upstream reaches golang.org/x/net/proxy for SOCKS5 in one function; that
// branch is gone here, and with it the only third-party import this repository
// would have had. What the fork buys is exactly that kind of choice; what it
// costs is that every upstream security fix is ours to track, which is why it
// lives here and not in the framework core (ADR 0004).
//
// The collection arrives when the server does: auth.Grant, because subscribing
// to a channel is a read and RULE 17 has no exception, and redis for the
// pub/sub that lets two instances agree on who is connected where.

require github.com/arandu-io/hesape v0.3.0
