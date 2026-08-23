---
name: joaju-server
description: Stand up the joaju WebSocket server — mounted inside an Arandu (Go) application or run as its own process — and get an event to the browsers listening. Use when the request mentions "realtime", "real-time", "websocket", "web socket", "live updates", "push notification", "push to the browser", "broadcast", "pusher", "socket server", "presence channel", "notify connected users", "publish an event", "who is online", "scale sockets across instances", or "my publish reaches nobody". Also use when a ServerConfig, a Broker, a Protocol, a Relay, an API route under /apps/{appId}, or cmd/joaju is involved. Covers the nine routes, the required fields of ServerConfig, publishing over HTTP and in process, the metrics routes, and what a second instance needs.
license: MIT
---

# Standing up the server, and getting an event out of it

`joaju.Server` is an `http.Handler`. It answers one route — the socket — and
mounts whatever the `Protocol` answers over HTTP beside it. It authenticates
nobody: it reads the `auth.Subject` the framework's middleware put on the
request context and asks a Policy about it.

## The nine routes

One belongs to the server and eight belong to the wire format:

```
GET  /app/{appKey}                                       the socket
POST /apps/{appId}/events                                publish one
POST /apps/{appId}/batch_events                          publish many
GET  /apps/{appId}/connections                           metrics
GET  /apps/{appId}/channels                              list
GET  /apps/{appId}/channels/{channel}                    one
GET  /apps/{appId}/channels/{channel}/users              presence members
POST /apps/{appId}/users/{userId}/terminate_connections  disconnect
GET  /up                                                 health
```

The socket route is registered in `server.go:501`; the other eight are one
`mux.HandleFunc` each in `protocols/pusher/routes.go:83-90`. Count them there
rather than believing this list:

```sh
grep -c 'mux.HandleFunc' protocols/pusher/routes.go   # 8
```

Seven of the eight read or write channel state, and every one of the seven needs
a Grant, because there is no method on `Broker` that reaches a channel without
one. `GET /up` is the exception and reads nothing.

## Mounting it in an application

**1. Build the two policies first.** `NewServer` refuses a config missing one
rather than filling in something safe-looking, and a nil `ConnectPolicy` would
be a server that accepts every socket. Writing them is `joaju-channel-policy`.

**2. Build the server. The Broker and the SubscriptionPolicy go in twice, and
that is not a mistake** — the frame layer reaches channels too, so it is handed
the same two values the server holds:

```go
broker := pusher.NewMemoryBroker()
subscribe := ordersPolicy{}   // a joaju.SubscriptionPolicy

server, err := joaju.NewServer(joaju.ServerConfig{
	AppID:     cfg.AppID,
	AppKey:    cfg.AppKey,
	Broker:    broker,
	Connect:   connectPolicy{},
	Subscribe: subscribe,
	Protocol: pusher.NewPusher(broker, subscribe, pusher.PusherConfig{
		ActivityTimeout: 30 * time.Second,
	}),
	Log: log,
})
```

`ActivityTimeout` is what the client is told to ping at, and it has to sit
comfortably below the server's `PongTimeout`: a client pinging at the deadline
is pinging a socket already hung up on. Half the read deadline is the number
`cmd/joaju` uses.

**3. Mount it behind the middleware that puts a subject on the context.** That
middleware is the only thing in the ecosystem that calls `auth.WithSubject`, and
`server.go:635` is where the server reads what it left:

```go
authenticate := middleware.NewAuthenticate(factory, subjectFor)
mux.Handle("/", pipeline.Chain(server, authenticate.Using("web").Handle))
```

A socket that arrives with no subject is refused —
`TestServerRefusesASocketWithNoSubject`.

**4. Run the gates.**

```sh
export GOWORK=off
go build ./... && go vet ./... && go test -race ./...
```

## Getting an event out

Two ways in, and they are the same path: both build a `ChannelName` from a
Grant, ask the `SubscriptionPolicy` about it, and hand the `Event` to the
channel.

**Over HTTP**, which is what an application server does from a request handler:

```
POST /apps/{appId}/events
{"name": "OrderShipped", "channel": "private-orders.17", "data": "{\"id\":17}"}
```

`data` is a JSON string containing JSON. That is the protocol's shape in both
directions, and it is validated before the fanout rather than at the far end of
one — an invalid payload otherwise fails on somebody else's socket. Two names
are refused: anything starting `pusher:` or `pusher_internal:`, because those
are the server's own voice, and a client that could publish
`pusher_internal:member_added` could invent members.

A batch authorizes every element on its own. It is a convenience for the caller
and never a way to reach a channel one element could not have reached alone —
`TestServerAuthorizesEveryElementOfABatch`.

**In process**, when the publisher already holds the Grant:

```go
name, err := joaju.NewChannelName(connected, "private-orders.17")
grant, err := auth.Authorize(ctx, subscribe, connected.Subject(),
	broadcasting.ChannelJoin, joaju.Subscription{Channel: name})
channel, err := broker.Find(ctx, grant, name)
err = channel.Broadcast(ctx, joaju.Event{
	Name:    "OrderShipped",
	Channel: name,
	Data:    payload,
})
```

`Broadcast` skips `Event.Socket`; `BroadcastToAll` ignores that field and is
what the server itself sends on, where there is no sender to spare.

**A channel nobody is subscribed to here is not an error.** On a deployment of
more than one instance, the process that took the publish is usually not the one
holding the sockets. The local delivery is skipped and the relay runs anyway.

## The second instance

Without a `Relay`, a publish reaches the sockets this process holds and the four
metrics routes answer for this process alone. That is the true answer for a
deployment of one and the wrong answer for any other: two instances serving one
application hold half the sockets each, and neither can see the other half.

```go
conn := connections.Connect(connections.Config{Address: address, Prefix: app})
bus, err := redis.NewBus(conn)
relay, err := joaju.NewRelay(ctx, joaju.InstanceID(hostname), bus, log)
```

Then `Relay: relay` in the `ServerConfig`, and wrap the broker in
`joaju.RelayedBroker(broker, relay)`. That wrapper is the inbound half: it joins
the fleet's topic where a channel appears here and leaves it where the last
subscriber goes, so an instance receives the traffic of channels it is actually
holding and only those. Without it the instance publishes to the fleet and hears
nothing back — which from inside a browser is indistinguishable from a quiet
channel. The bus is RESP pub/sub and lives in the `redis/` submodule, which has
its own `go.mod` because the driver under it is third-party and Go has no
optional dependency.

A bus it cannot reach degrades to this process rather than failing a request —
`TestADegradedInstanceAnswersItsMetricsRoutesFromItsOwnState`. A metrics route
publishes its question, waits `MetricsTimeout` for the others, and answers with
what arrived: a partial answer is the answer, because a dashboard served late by
one instance being replaced is a dashboard that is down.

`cmd/joaju` builds its server with `Relay` nil, so the process as shipped runs
alone. A second instance needs the library, a bus and the wiring by hand.

## The process

Configured by the environment alone — no flag, no file, because two ways to say
the same thing is two ways to be wrong about what a running process is doing.
Four variables have no default it could invent:

```sh
export GOWORK=off
JOAJU_APP_ID=app JOAJU_APP_KEY=key JOAJU_APP_SECRET=secret JOAJU_TENANT=acme \
  JOAJU_ADDR=127.0.0.1:9200 go run ./cmd/joaju
```

From outside the repository the same line is
`go run github.com/arandu-io/joaju/cmd/joaju@latest`. Either way,
`curl http://127.0.0.1:9200/up` answers `OK` and the listening line goes to
standard error — nothing this process writes goes to standard output.

The rest of the variables are in `cmd/joaju/config.go`, each with what it means
and what leaving it out means. It serves HTTP; a reverse proxy in front holds
the certificate.

## The limits, and which way each one points

`MaxConnections` is per **tenant**. A global limit lets one customer's traffic
refuse another customer's connections, which is a denial of service one of them
did not cause and cannot see — `TestTheConnectionLimitIsPerTenantAndNotGlobal`.

`MaxMessagesPerSecond` is per **socket**, which is the opposite and the same
reasoning one layer in: a socket is the smallest thing a noisy client owns, so
metering it spends nothing of the tenant's other sockets. It is zero — off — by
default, because there is no rate that is right for traffic this server has not
seen. A frame past it is refused and dropped and the socket stays open.

## What is not here, and which kind of absence it is

The README has a section of these, and the distinction it draws is the one that
matters when you are about to add something.

**Not written yet.** These are gaps, and closing one is ordinary work — but it
is work with a design in front of it, so propose before you write.

- **Nothing exports the counts.** `Counter` keeps per-tenant totals and hands
  them to whoever asks. There is no OpenTelemetry exporter, which is what
  production is meant to read them through.
- **The process runs alone.** `cmd/joaju` builds its server with no `Relay`, so
  a second instance needs the library, a bus and the wiring by hand.

**Decided, and not a gap.** Adding one of these undoes a decision, so it is a
conversation and not a patch.

- **The client is not routed by this server.** It is served by the application,
  from the origin the page is on. See `joaju-browser-client` for why.
- **The process has no people.** A generic process has no policy about somebody
  else's users. It authenticates the application by its app secret and a browser
  not at all.
- **No compression negotiated.** The transport implements permessage-deflate and
  the server does not turn it on.
- **No TLS**, held by a reverse proxy in front. **One application per process**,
  because a Go binary is a process. **No cross-origin socket**, because nothing
  here widens the handshake's origin check.
