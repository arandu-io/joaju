---
name: joaju-browser-client
description: The browser half of joaju — the JavaScript that speaks this protocol from a page, how an application serves it, and the endpoint it calls to authorize a private or presence channel. Use when the request mentions "connect from the browser", "frontend websocket", "client script", "joaju.js", "pusher-js", "subscribe in the page", "listen for events in the browser", "the socket will not connect", "Content-Security-Policy", "script-src self", "refused to load the script", "reconnect", "activity timeout", "presence members in the page", or "broadcasting/auth". Covers why the script is served by the page's own origin rather than by the socket server, the two ways to mount it, the client API and its options, the authorization endpoint the application has to write, and the constraints the file is held to.
license: MIT
---

# The page half

A protocol only exists if something on the other end speaks it, and nothing in a
browser speaks this one. The usual browser half arrives through npm, which this
project does not have anywhere. So `client/joaju.js` is written here and served
by the binary, the way HTMX and Alpine already arrive. There is no
`package.json`, no lockfile, no bundler and no build step: the file that is
embedded is the file a browser runs.

## Serve it from the page's origin, not from the socket server

This is the part that is wrong by default. The pages this client runs on are
served under `script-src 'self'`, so the script has to come from **the origin
the page is on** — and a joaju server is frequently not that origin. A
`<script src>` pointing at `wss://socket.example.com` is refused by the browser
before a socket is ever attempted.

So it is not a tenth route on `Server`. A route there would work in the
deployment where joaju is mounted inside the application and be forbidden in the
deployment where it is not, which is one URL meaning two things — with the
broken one failing only where there is a separate socket server, that is, in
production.

Two ways to mount it, and both reach the same bytes:

```go
// Any Go server, and any test.
mux.HandleFunc(client.Path, client.Handler)

// An Arandu application, which already has one asset table.
view.RegisterAsset(client.Name, client.ContentType, client.Script())
```

`client.Path` is `/_arandu/joaju/` — the framework's reserved namespace, a
different leaf under it, with the trailing slash that makes it a subtree
pattern. The tag names `client.URL()`, which is
`/_arandu/joaju/<hash>/joaju.js` where the hash is the first twelve hex
characters of the SHA-256 of the bytes. That URL is cached for a year and is
safe to be: changing the script changes the URL. A URL that still names the
script but carries an older hash is served uncached, so a page holding a stale
reference is slow rather than broken. Anything else is 404 —
`TestTheCacheHeaderIsOnlyImmutableForTheContentAddressedURL`,
`TestTheHandlerServesNothingButTheScript`.

Use `client.URL()` absolute; a relative reference inherits the hash of whatever
document it sits in and would be re-downloaded on every page view, silently.

`client.Script()` hands back a copy. Do not reach for the embedded slice: a
caller that wrote into it would leave the served bytes disagreeing with the hash
every browser was told to cache for a year —
`TestScriptIsHandedOutAsACopy`, `TestTheURLCarriesTheHashOfWhatIsServed`.

## The API a page uses

```js
const joaju = new Joaju({ key: 'app-key' })
joaju.connect()
joaju.on('connected', ({ socketId }) => ...)

const orders = joaju.subscribe('private-orders.17')
orders.bind('OrderShipped', (data) => ...)
orders.trigger('client-typing', { who: 'ana' })
```

Everything a page needs is on those two objects: a connection, and a channel per
name. `Joaju` also has `disconnect()`, `unsubscribe(name)`, `channel(name)` and
`channels()`; `Channel` also has `unbind(event, callback)` and, on a presence
channel, `members` as a `Map` of `user_id -> user_info`.

The connection emits `connected`, `disconnected`, `failed`, `error` and `state`.
The state names — `initialized`, `connecting`, `connected`, `unavailable`,
`disconnected`, `failed` — are the ones a developer who has spoken this protocol
before already knows.

`subscribe` may be called before `connect` resolves. The channel is created
immediately and the subscription is sent at `connection_established`, so a page
can bind straight away. Channels are kept across a reconnect and resubscribed;
`unsubscribe` forgets one so that a reconnect does not bring it back.

`trigger` throws at the call site for the three things the server would refuse
later: a name not beginning `client-`, a channel that is not private or
presence, and a channel this connection is not subscribed to. The server refuses
all three again, and also refuses when the deployment relays no client events at
all, which the browser has no way to know.

## The options, and their defaults

| option | default | what it is |
| --- | --- | --- |
| `key` | required | the app key, which goes in the socket path |
| `url` | `''` | the socket server, no path — `wss://socket.example.com`. Empty means the page's own origin, which is the mounted deployment |
| `authEndpoint` | `/broadcasting/auth` | where a private or presence subscription is authorized |
| `authHeaders` | `null` | what goes on the authorization request on top of the content type — a CSRF token, usually |
| `activityTimeout` | `120000` | the fallback for a server that sends none of its own. The server's `activity_timeout` wins when it sends one |
| `pongTimeout` | `30000` | how long the answer to `pusher:ping` may take before the socket is treated as gone |
| `minReconnectDelay` | `1000` | the backoff floor |
| `maxReconnectDelay` | `30000` | the backoff ceiling |

The backoff doubles between the two, and the delay actually waited is drawn
uniformly from that range. The randomness is not optional: without it every
browser that was connected to an instance that died comes back at the same
instant.

The silence clock measures the **server's** silence, so nothing the client sends
resets it, and any frame at all cancels an outstanding ping.
`TestTheClientPingsOnTheServersActivityTimeout` and
`TestTheClientReconnectsWhenThePongDoesNotCome` are where that is checked.

## The endpoint the application has to write

The client does not authorize anything. For a guarded channel it asks the
application first, and only then sends `pusher:subscribe`:

```
POST /broadcasting/auth
Content-Type: application/x-www-form-urlencoded; charset=UTF-8
credentials: same-origin

socket_id=<id>&channel_name=private-orders.17
```

The answer is JSON with an `auth` string, and on a presence channel a
`channel_data` alongside it, which is forwarded verbatim into the subscribe
frame. A response that is not `ok`, or that carries no `auth` string, fails the
subscription and the channel emits `error`.

**Writing that endpoint is a policy decision, not a signature.** A mounted
server decides in its own `SubscriptionPolicy` and ignores what the client
offered; the signature exists for the deployment that has no front door. See
`joaju-channel-policy` before writing either half.

## What the file may not contain

It is held to `script-src 'self'` with no `unsafe-eval` and no `unsafe-inline`,
because a script that needed either could only be shipped by loosening the
policy — paying in security for a convenience, which is what embedding the file
was supposed to avoid. `TestTheScriptNeedsNoUnsafeContentSecurityPolicy` reads
the served bytes and fails on `eval(`, `new Function`, `document.write`,
`.innerHTML`, `.outerHTML`, `insertAdjacentHTML`, string-form `setTimeout`,
`import(`, and any literal `http://` or `https://`.

It runs as it is written: no build step, no bundler, no transpiler. It is a
classic script assigning one global, ES2017 throughout, because that is how the
other scripts on the page already arrive and a module would mean the application
writing modules of its own.

`TestNoNodeAnywhereInThisRepository` walks the whole repository for
`package.json`, any lockfile, `node_modules` and the usual bundler configs. Do
not add one, including for tooling.

## Running the browser tests

They are behind the `e2e` tag, and they need a JavaScript runtime — `node`,
`bun` or `deno`, in that order of preference. A runtime is a test dependency and
never a product one: a client nobody ever ran is a client nobody knows works.

```sh
export GOWORK=off
go test -race -tags e2e ./tests/E2E/...
```

With no runtime installed, everything there skips and says so, which is why a
fresh clone is green without one. `TestTheScriptIsSyntacticallyValid`,
`TestTheClientSpeaksTheProtocolEndToEnd` and
`TestTheClientReconnectsAndResubscribes` are the ones worth reading first.
