# Working in this repository

This is joaju, the WebSocket server for Arandu. It is a library first — `Server`
is an `http.Handler` an application mounts behind its own middleware — and a
process second, in `cmd/joaju`. The wire format is the Pusher protocol, so a
browser client that already speaks it speaks to this one.

Read `.agents/skills/` before writing code. Each skill is a procedure, and the
one you need is named by the situation you are in.

## The four gates

Nothing is finished until all four exit zero.

```sh
export GOWORK=off
gofmt -l $(find . -name '*.go' -not -path '*/testdata/*' -not -name '*.kyse.go')
go build ./...
go vet ./...
go test -race ./...
```

Both filters on `gofmt` are load-bearing, and they are the project's rather than
this repository's: `gofmt` is the only tool in the chain that ignores build
tags, and `testdata/` holds fixtures that are invalid on purpose. The command is
kept identical across repositories so that the one a contributor copies is the
one CI runs.

Three more before a pull request:

```sh
go vet  -tags 'integration e2e' ./...
go test -race -tags 'integration e2e' ./...
bash tests/test-layout-guard.sh
```

The tags are not decoration. Without them the two suites that need something
running — a RESP server for `redis/tests/Integration`, a JavaScript runtime for
`tests/E2E` — are not compiled at all, and a suite nothing built is a suite
nothing checked. Both skip and say so when there is nothing to talk to.

`redis/` carries its own `go.mod`, so every `./...` above stops at its
directory. Run the same commands again from inside it.

## The tree

| path | what it owns |
| --- | --- |
| `joaju.go`, `server.go`, `scaling.go`, `counter.go`, `events.go` | the socket, the registry, the limits, the relay, the counts. Knows no frame |
| `protocols/pusher/` | the wire format: frames, codecs, channels, the in-memory `Broker`, eight HTTP routes |
| `ws/` | this project's own RFC 6455. The reason the dependency graph has one entry in it |
| `client/` | the browser half: `joaju.js`, embedded, served by the application |
| `cmd/joaju/` | the same server as a process, configured by the environment alone |
| `redis/` | the bus for more than one instance, a module of its own |

`protocols/pusher` imports the root and the root does not import it. That is
what keeps the two apart: the server owns the socket and knows no frame, and a
second wire format would be a second subpackage rather than a branch in this
one.

## What does not exist here

Reaching for one of these is the fastest way to be wrong. None of them is
missing by accident.

| A model reaches for | What is here instead |
| --- | --- |
| a websocket library — gorilla, coder, nhooyr, gobwas | `ws/`, written against the RFC. The root `go.mod` has one `require` line and CI fails a second |
| Socket.IO, µWebSockets, a Node gateway | nothing. There is no Node in this tree, and `TestNoNodeAnywhereInThisRepository` walks the repository to say so |
| a browser client from npm or a CDN | `client/joaju.js`, embedded with `go:embed` and served from the application's own origin |
| middleware that authorizes the socket | a Policy that issues an `auth.Grant`. Middleware answers "is there a subject" and stops |
| a tenant parsed out of a channel name, a path or a header | `NewChannelName(grant, requested)`. The tenant is read off the Grant, and a requested name carrying the separator is refused outright |
| an exemption for reads — list, count, members, subscribe | none. Subscribing is a read, and there is no exception for reads |
| an allowed-origins list in configuration | `Handshake.Origin`, weighed by the `ConnectPolicy`. `ws.Upgrader` has no `CheckOrigin` field to widen |
| permessage-deflate, or any compression setting | the transport implements the extension and the server does not negotiate it |
| a second broadcast method for a relayed event | `Channel.Broadcast` and `Channel.BroadcastToAll`. Suppressing a re-cache is the relay's business and stays there |
| a second application multiplexed into one process | one server is one application. `AppID` and `AppKey` are single values, and another app is answered 404 |

## The two rules everything else follows from

**Two decisions, each issuing a Grant.** One says whether a subject may hold a
socket at all (`Connect`, decided by a `ConnectPolicy`). The other says whether
a subject may hear one channel (`broadcasting.ChannelJoin`, decided by a
`SubscriptionPolicy`), and it is asked again for every channel and on every
route that touches one. A `Connection` cannot be built without a Grant and a
`ChannelName` cannot be built without one either, so there is no path to a
channel that a policy did not open.

**The tenant comes from the Grant.** `auth.Tenant(g)`, read by
`NewChannelName`, and never from a path segment, a body, a query or a header.
The client supplies the name after the tenant and nothing else.

## Nothing writes to standard output

`cmd/joaju` puts its JSON log and its one fatal line on `os.Stderr` —
`cmd/joaju/main.go:74` and `cmd/joaju/main.go:60`. The only `fmt.Print` in the
repository is `tests/Specification/report/main.go:74`, where the printed text is
the tool's answer and standard output is where an answer goes.

## Writing code

Comments, identifiers, error messages, log lines, CLI output and test names are
in English. A test name is a sentence about what the code does:
`TestPusherAsksThePolicyAboutAPublicChannelToo`.

A doc comment documents its symbol and nothing beyond it — what the function
does, what it takes, what it returns, what it guarantees, and why a signature is
the shape it is, said in terms of the code. Cross-references to decisions,
sessions and versions belong in the decision record, not on `pkg.go.dev`.

Where a test goes is decided by one question: does it need an identifier the
package does not export? If yes it lives beside the code as
`<name>_internal_test.go`; if no it goes in the mirrored tree under the category
that describes what it does. `bash tests/test-layout-guard.sh` checks all of it.
`CONTRIBUTING.md` has the table and the sign-off requirement.
