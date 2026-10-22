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
