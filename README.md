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

```go
server, err := joaju.NewServer(cfg)
```

`Server` is an `http.Handler` and it authenticates nobody: it reads the subject
the framework's middleware put on the request and asks a Policy about it.
Subscribing to a channel is a read, and reads have no exception — a connection
cannot be built without a `Grant`, a channel name cannot either, and the tenant
is taken from the Grant rather than from the wire.

The transport is the `ws` subpackage: this project's own RFC 6455, not a fork
and with no borrowed code. It is why this repository takes no third-party
dependency at all.

It is written, and `go build`, `go vet` and `go test -race` are green. The
Autobahn conformance suite has not been run against it yet.

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
