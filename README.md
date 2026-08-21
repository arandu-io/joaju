<p align="center">
  <img src=".github/logo.png" alt="Arandu" width="140" height="140">
</p>

<h1 align="center">arandu-io/joaju</h1>

<p align="center">The WebSocket server for Arandu.</p>

<p align="center">
<a href="https://github.com/arandu-io/joaju/actions/workflows/ci.yml"><img src="https://github.com/arandu-io/joaju/actions/workflows/ci.yml/badge.svg" alt="Build Status"></a>
<a href="https://pkg.go.dev/github.com/arandu-io/joaju"><img src="https://pkg.go.dev/badge/github.com/arandu-io/joaju.svg" alt="Go Reference"></a>
<a href="https://github.com/arandu-io/joaju/tags"><img src="https://img.shields.io/github/v/tag/arandu-io/joaju?label=version" alt="Latest Version"></a>
<a href="LICENSE.md"><img src="https://img.shields.io/github/license/arandu-io/joaju" alt="License"></a>
</p>


## About the WebSocket server

The wire format is the Pusher protocol, so a browser client that already talks
to Pusher or to Reverb talks to this. Nine HTTP routes and six kinds of channel,
carrying the names Reverb uses, because a reader who knows Reverb should
recognize this repository without reading it first.

It is one library and two deployments. `Server` is an `http.Handler`, so an
Arandu application mounts it behind its own middleware and its own policies;
`cmd/joaju` is the same server as a process of its own, which is what a
container image runs.

## What it delivers

**Nothing in the dependency graph that is not ours or the standard library's.**
The root `go.mod` requires `github.com/arandu-io/hesape` and
`golang.org/x/net`, and a CI step fails the build on anything outside
`arandu-io` and `golang.org/x`. Subpackage `ws` is this project's own RFC 6455 —
not a fork, no borrowed code — and `golang.org/x/net` is what its client dials a
SOCKS5 proxy with. It is measured and not asserted: Autobahn TestSuite version
18, every case except `12.*` and `13.*` — permessage-deflate, the same pair
`laravel/reverb` excludes — **301 cases, 0 failures**. The report is
[`tests/Specification/REPORT.txt`](tests/Specification/REPORT.txt).

**No path to a channel that a policy did not open.** The server authenticates
nobody: it reads the subject the framework's middleware put on the request and
asks a Policy about it. Two decisions, each issuing a Grant — one for holding a
socket at all, one for reaching a channel, asked again for every channel and on
every route that touches one. Subscribing is a read, and there is no exception
for reads. A connection cannot be built without a Grant, a channel name cannot
either, and the tenant is read off the Grant rather than off the wire.

**The browser half is here too.** Subpackage `client` is the JavaScript that
speaks this protocol from a page — written here, embedded with `go:embed`,
served from the application's own origin under `script-src 'self'`. No npm, no
CDN, no build step, no `package.json` anywhere in the repository. It honours the
`activity_timeout` the server sends, reconnects with exponential backoff and
jitter and resubscribes what it held, and fetches authorization for private and
presence channels the way the protocol defines.

**Broadcast that crosses instances.** A `Relay` hands the fleet an event this
instance has already delivered, and the four metrics routes ask the others and
add up what answers in time; a bus it cannot reach degrades to this process
rather than failing a request. The bus is RESP pub/sub, in the `redis/`
submodule — its own `go.mod`, because the driver under it is third-party and Go
has no optional dependency.

9,114 lines of production code and 8,176 of test.

## Install

```sh
go get github.com/arandu-io/joaju
go get github.com/arandu-io/joaju/redis   # the bus, for more than one instance
```

The process is configured by the environment alone, and four variables have no
default it could invent:

```sh
JOAJU_APP_ID=app JOAJU_APP_KEY=key JOAJU_APP_SECRET=secret JOAJU_TENANT=acme \
  go run github.com/arandu-io/joaju/cmd/joaju@latest
```

The rest are in [`cmd/joaju/config.go`](cmd/joaju/config.go), each with what it
means and what leaving it out means.

## What is not here yet

- **Nothing exports the counts.** `Counter` keeps per-tenant totals and hands
  them to whoever asks; there is no OpenTelemetry exporter, which is what
  production is meant to read them through. `arandu-io/kyse` draws them with
  `StatCard` in the meantime.
- **The client is not routed by this server.** Subpackage `client` embeds the
  JavaScript that speaks this protocol from a page, and serving it is the
  application's: the script has to come from the origin the page is on, and a
  socket server is frequently not that origin.
- **The process runs alone.** `cmd/joaju` builds its server with no `Relay`, so
  a publish reaches the sockets it holds and the metrics routes answer for it
  alone. A second instance needs the library, a bus and the wiring by hand.
- **The process has no people.** It authenticates the application by its app
  secret and a browser not at all, so a private or presence channel there is
  reached by offering the Pusher subscription signature, which the policy
  recomputes. A mounted server decides in its own policy and ignores it.
- **No compression negotiated.** The transport implements permessage-deflate and
  the server does not turn it on, which is why `12.*` and `13.*` are excluded
  above. Also **no TLS**, held by a reverse proxy in front; **one application per
  process**; and no cross-origin socket, since nothing here sets the handshake's
  origin check and an origin allowlist only narrows it further.

## The rest of Arandu

[`arandu-io/hesape`](https://github.com/arandu-io/hesape) is where `auth.Grant`,
`auth.Policy` and the channel vocabulary this server shares with the SSE fanout
come from, and its `redis` module is the connection the bus borrows rather than
opening a second one. `framework` is process bootstrap and typed configuration,
`arandu` is the project skeleton that mounts this, and `aru` is the command
line.

## Learning Arandu

The API reference is generated from the doc comments and lives on
[pkg.go.dev](https://pkg.go.dev/github.com/arandu-io/framework). Every exported
symbol carries one, and that is deliberate: it is the documentation that cannot
drift from the code, because it sits in the same file.

The CLI documents itself. `aru help` lists every command, and each one explains
what it writes and what to do with it. `aru doctor` explains what it found and
what breaks, not which rule was violated.

A guide and a website do not exist yet, and that is a decision rather than a
gap: a guide written against an API that still moves is work done twice, and the
second time is worse — there is wrong documentation published. The site is the
next phase, and it will be an Arandu application.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Before opening a pull request, the three
commands at the top of that file have to pass, and CI runs exactly them.

## Security Vulnerabilities

Please review [our security policy](SECURITY.md) on how to report a
vulnerability. Never open a public issue for one.

## License

Open-sourced software licensed under the [MIT license](LICENSE.md).
