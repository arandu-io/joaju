# Upgrade guide

What changed in a way that stops your code compiling, or changes what a running
server does, and what to write instead.

Additions are not listed. A new symbol breaks nothing, and a file that listed
every one of them would be a changelog nobody reads to find the two lines that
matter.

While the version starts with `v0.`, the API can break. Every release is
compared against the one before it by `apidiff` in CI, and an incompatible
change with no entry here fails the build.

---

## v0.8.0 — the HTTP API has a policy of its own

### `joaju.API.Connect` is gone, and the API asks an `OperationPolicy`

The HTTP API routes -- `events`, `batch_events`, `connections`, `channels`,
`channels/{channel}`, `channels/{channel}/users` and `terminate_connections` --
used to be authorized by the socket's `ConnectPolicy`, asked about
`joaju.Connect` with an empty `Handshake.Socket`. A policy written for sockets
("any signed-in user may connect") therefore let every signed-in user publish
events in the server's voice, close other people's sockets and read the
tenant's counts.

They now ask a new policy about a new action:

```go
// package joaju
const Operate auth.Action = "joaju.operate"

type Operation struct {
	Route  string // "POST /apps/{appId}/events": see pusher.RouteEvents and the rest
	Origin string
}

type OperationPolicy = auth.Policy[Operation]
```

`ServerConfig.Operate` is optional, and **nil refuses every API route with
403**. The socket route and `GET /up` are unaffected.

- A server that only serves sockets: nothing to do.
- A server whose application calls the API: write an `OperationPolicy` that
  allows the callers that should operate the server -- usually the
  application's backend, never an ordinary user -- and pass it as
  `ServerConfig.Operate`. A `ConnectPolicy` that branched on
  `Handshake.Socket == ""` to answer the API can drop that branch:
  `Handshake.Socket` is now always set, and the `ConnectPolicy` is asked by the
  socket route only.
- A `Protocol` of your own that read `API.Connect`: read `API.Operate` and ask
  it about `joaju.Operate`. It is nil when the server was built without one,
  and a route must refuse then.

`Server.Connections` and `Server.Terminate` (the `Registry`) now require a
Grant issued for `joaju.Operate`. A Grant issued for `joaju.Connect` is refused
with `auth.ErrForbidden`.

`cmd/joaju` keeps working unchanged for a caller that signs with the app
secret: its policy allows the signed application and nobody else.

### The POST routes require `Content-Type: application/json`

`events`, `batch_events` and `terminate_connections` answer **415** to a POST
whose `Content-Type` is not `application/json` (parameters such as
`; charset=utf-8` are accepted), before the body is read or anything is done.
Every Pusher server library already sends that type; a hand-written caller that
sent none, or `text/plain`, has to add the header. The reason is the cross-site
form: a browser posts a form or `text/plain` body to another origin without a
preflight and with the cookies attached.

### `cmd/joaju` checks the signature before it reads the body

The front door of the standalone server used to read up to `MaxBodySize` (1 MiB
by default) of every `/apps/...` request before it checked the signature, so
an anonymous caller could make it buffer a megabyte per request. It now
verifies the HMAC from the query alone -- `body_md5` is a signed query
parameter -- and reads the body only for a caller that proved the app secret,
then requires the body to match the signed `body_md5`.

Every Pusher server library signs and sends `body_md5` whenever there is a
body, and keeps working. What is refused now and was accepted before: a request
with a body whose query carries no `body_md5` (the old code recomputed the
digest and could accept a client that signed it without sending it), and a
`body_md5` that does not match the body that arrived, including one sent with
an empty body. Both answer 401.

### `cmd/joaju` bounds the whole HTTP request

The standalone server's `http.Server` set only `ReadHeaderTimeout`. It now also
sets `ReadTimeout` (30s), `WriteTimeout` (30s) and `IdleTimeout` (120s). An API
call whose body takes longer than 30 seconds to arrive, or whose answer takes
longer than 30 seconds to be read, is cut; an idle keep-alive connection is
closed after two minutes. Sockets are not affected: the server replaces both
deadlines on the hijacked connection before it reads or writes a frame, and
they keep `JOAJU_PONG_TIMEOUT` and `JOAJU_WRITE_TIMEOUT`.

### `cmd/joaju` meters every socket by default

`JOAJU_MAX_MESSAGES_PER_SECOND` left unset used to mean no limit. It now means
100 frames per second per socket (a token bucket whose burst is the limit, the
same number as the default channel limit, so a client resubscribing to every
channel it may hold is never refused). A frame past the limit is answered with
the rate-limit error and dropped; the socket stays. Set the variable to `0` for
the old behaviour, or raise it together with
`JOAJU_MAX_CHANNELS_PER_CONNECTION`. The library's `ServerConfig` is unchanged:
zero there is still no limit.

### `cmd/joaju` limits sockets per client address

The standalone server now refuses a socket with **429** when its client
address already holds `JOAJU_MAX_CONNECTIONS_PER_ADDRESS` sockets (256 by
default; a negative number turns the limit off). The library's per-tenant
limit was the only one, and with one tenant served to anonymous browsers one
address could take all of it.

The client address is the TCP peer, unless the peer is a trusted proxy, in
which case the `X-Forwarded-For` it appended is read from the right.
`JOAJU_TRUSTED_PROXIES` lists the trusted proxies as addresses or CIDR
prefixes; unset means loopback and the private ranges (`127.0.0.0/8`,
`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `::1`, `fc00::/7`), and
`none` trusts nobody.

Check this before upgrading a deployment behind a proxy:

- A reverse proxy on a private network or loopback that sets
  `X-Forwarded-For` (nginx, Traefik, Caddy by default): nothing to do.
- A proxy or CDN on a public address (for example Cloudflare in front of the
  process, or a load balancer with a public source address): list its ranges in
  `JOAJU_TRUSTED_PROXIES`, or every visitor counts as that proxy and the whole
  deployment gets 256 sockets.
- A TCP passthrough that does not set `X-Forwarded-For`: every visitor counts as
  the proxy; raise the limit or turn it off.

A refusal is logged as `joaju: a socket was refused by the per-address limit`
with the address and a hint naming `JOAJU_TRUSTED_PROXIES`.

### A cache channel does not cache client events

A `client-` event relayed on a `cache-`, `private-cache-` or
`presence-cache-` channel used to become the event the channel replays to the
next subscriber, so any subscriber allowed to send client events could replace
the state the server last published. Client events are still relayed, and are
no longer cached; the replay is the last event the server published. An event
named `client-...` published through the HTTP API is not cached either.

### `GET /apps/{appId}/channels` works in `cmd/joaju`

The standalone server's subscription policy refused the question the channel
list asks -- about no channel in particular -- so the route always answered
403. It now allows the signed application to list the tenant's channels, and
still refuses everybody else.
