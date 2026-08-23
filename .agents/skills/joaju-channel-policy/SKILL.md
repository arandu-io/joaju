---
name: joaju-channel-policy
description: Authorization in the joaju WebSocket server — the two policies that decide who holds a socket and who hears a channel. Use when writing or changing a ConnectPolicy or a SubscriptionPolicy, when something asks for an auth.Grant, when a subscription is refused or is being allowed too easily, when a channel name will not build, or when the request mentions "who can subscribe", "private channel", "presence channel", "channel authorization", "authorize a socket", "subscription signature", "broadcasting/auth", "multi-tenant sockets", "tenant isolation", "ErrWrongTenant", "ErrNoGrant", "impersonate a member", or "let the reads through". Also use when tempted to skip the policy for a public channel, a channel list, a subscriber count or a presence member list — subscribing is a read, and there is no exception for reads.
license: MIT
---

# The two decisions, and why there is no third

Every path to a channel in this server goes through an `auth.Grant`, and a Grant
is only ever produced by a Policy. `Connection` cannot be built without one and
`ChannelName` cannot be built without one, so a handler that reaches a channel
without asking has nothing to pass.

```
Connect                    holding a socket at all   -> ConnectPolicy
broadcasting.ChannelJoin   hearing one channel       -> SubscriptionPolicy
```

They are two and not one because they answer different questions. Issuing one
Grant for both would mean the per-channel policy never runs — one constant
undoing every subscription decision.

The second is asked **again for every channel**, and on every route that touches
one. Subscribing is a read, and there is no exception for reads.

## The procedure

**1. Write the ConnectPolicy.** It answers two callers, told apart by
`Handshake.Socket`: a browser upgrading has one, an API caller has none, because
an API caller wants no socket opened.

```go
func (p connectPolicy) Can(_ context.Context, s auth.Subject, a auth.Action, h joaju.Handshake) error {
	if a != joaju.Connect {
		return fmt.Errorf("%s is not %s, which is the only action this policy answers", a, joaju.Connect)
	}
	if s.Tenant != p.tenant {
		return fmt.Errorf("this process serves the tenant %q and the subject carries %q", p.tenant, s.Tenant)
	}
	...
}
```

Check the action. A policy that answers any action it is handed is a policy that
will one day be asked the wrong question and say yes —
`TestConnectPolicyAnswersOnlyItsOwnAction`.

**2. Put the origin decision here, not in configuration.** `Handshake.Origin` is
the Origin header verbatim: what the browser claims, so it is evidence for a
policy and never a tenant. A policy can only narrow — `ws.Upgrader` already
refuses a handshake from another host and has no `CheckOrigin` field that would
widen it. See `TestConnectPolicyRefusesAnOriginItWasNotGiven` and
`TestServerRefusesASocketFromAnotherOrigin`.

**3. Write the SubscriptionPolicy.** By the time it runs, the tenant is settled:
`Subscription.Channel` is a `ChannelName` built from the Connect Grant. The
policy is asked whether this subject may hear this channel. It is never asked
whose channel it is, because that was not the client's to say.

```go
func (p subscriptionPolicy) Can(_ context.Context, s auth.Subject, a auth.Action, sub joaju.Subscription) error {
	if a != broadcasting.ChannelJoin {
		return fmt.Errorf("%s is not %s, which is the only action this policy answers", a, broadcasting.ChannelJoin)
	}
	if s.Tenant != p.tenant {
		return fmt.Errorf("this policy serves the tenant %q and the subject carries %q", p.tenant, s.Tenant)
	}
	if sub.Channel.IsZero() {
		return errors.New("the subscription named no channel")
	}
	if !sub.Channel.Type().Guarded() {
		return nil   // public and public cache: the tenant was the whole question
	}
	...
}
```

A Grant issued for `Connect` is not a Grant to listen, which is why the tenant
is compared again here: a subject that reached this policy by another path is
still refused. `cmd/joaju/policy.go` is the worked example.

**4. Compare the offered member against the subject.** `Subscription.Member`
came from the client. A policy that lets a subscriber name their own
`Member.UserID` is a policy that lets them impersonate one.

**5. Run the gates.**

```sh
export GOWORK=off
go build ./... && go vet ./... && go test -race ./...
```

## The six kinds of channel, and which need a decision

The kind is read off the name's prefix, never stored, because the client chooses
it by what it asks for. `ChannelName.Type` is the only place the prefixes are
compared. The constants are `joaju.go:192-207`:

```sh
sed -n '187,208p' joaju.go   # PublicChannel .. PresenceCacheChannel: six
```

| kind | prefix | `Guarded()` | `Cache()` | `Presence()` |
| --- | --- | --- | --- | --- |
| `PublicChannel` | none | no | no | no |
| `PrivateChannel` | `private-` | yes | no | no |
| `PresenceChannel` | `presence-` | yes | no | yes |
| `CacheChannel` | `cache-` | no | yes | no |
| `PrivateCacheChannel` | `private-cache-` | yes | yes | no |
| `PresenceCacheChannel` | `presence-cache-` | yes | yes | yes |

`private-encrypted-` has no kind of its own. End-to-end encryption is key
distribution, not a channel kind: this server never holds the key and never
reads a payload. The prefix is still read, so `private-encrypted-cache-` is a
`PrivateCacheChannel` — leaving it unread cost a replay on the cache half, which
is what a half-known prefix looks like.

**`Guarded()` says whether a policy may allow a subscription freely, not whether
one is consulted.** The policy is consulted on every kind. `Channel.Subscribe`
checks the Grant before it touches its state even on a public channel, and the
frame layer asks about a public channel too —
`TestPusherAsksThePolicyAboutAPublicChannelToo`,
`TestChannelSubscribeRefusesTheZeroGrant`.

A public channel is still one tenant's. "Public" means public within one
customer's namespace and never wider —
`TestPublicChannelIsStillOneTenants`.

## The reads that people try to exempt, and must not

Every one of these needs a Grant, and each is checked:

| the read | why it is a read of who is talking to whom |
| --- | --- |
| `pusher:subscribe` | listening to a channel is receiving its contents |
| `GET /apps/{appId}/channels` | which channels exist is who is online, by name |
| `GET /apps/{appId}/channels/{channel}` | a subscriber count |
| `GET /apps/{appId}/channels/{channel}/users` | the member list, by user id |
| `GET /apps/{appId}/connections` | a socket count for the tenant |
| publishing to a channel | reaching a channel is reaching a channel |

A dashboard that lists channels without a policy is a tenant boundary that holds
everywhere except the dashboard —
`TestServerRefusesTheChannelListWhenThePolicyDoes`,
`TestMemoryBrokerAllAnswersOnlyTheGrantsTenant`.

## The tenant

`NewChannelName(grant, requested)` is the only constructor, and there is
deliberately no second one that takes a name already carrying a tenant. It
refuses a `requested` that is empty or contains the tenant separator `:` — a
client that could put `acme:` in front of a channel is a client choosing whose
events it hears. It refuses a Grant carrying no tenant, or one that cannot be a
namespace. On top of that it enforces `MaxChannelNameLength` (164, prefix
counted) and `ChannelNameCharacters` (`_-=@,.;` besides letters and digits).

The name that goes back out on the wire is `ChannelName.Requested()`, never
`ChannelName.String()`. The client asked for `private-orders.17` and that is the
name it is answered about; the tenant is not its to see —
`TestChannelFrameNeverCarriesTheTenant`, `TestNoFrameCarriesTheTenant`.

The instance receiving a relayed message over the bus has a tenant-carrying name
in hand, and the temptation is to parse the tenant back out of it. That is the
tenant coming off the wire. It compares the string against the
`ChannelName.String()` of channels it already holds, and each of those was built
from a Grant.

## The errors, and what each one means

- **`ErrNoGrant`** — the zero Grant reached something that needs one.
- **`ErrWrongTenant`** — the Grant and the channel belong to different tenants.
  `NewChannelName` cannot produce this; a name carried over from another Grant
  can, which is why it is a distinct error: both values are valid and nothing
  but the comparison notices.
- **`ErrChannelName`** — the name is not one this protocol carries.
- **`Grant.Check`** refuses a Grant issued for another action —
  `TestChannelSubscribeRefusesAGrantIssuedForAnotherAction`.

## The subscription signature: evidence, never authority

`Subscription.Auth` carries the Pusher signature the client offered —
`"<app key>:<hex HMAC-SHA256>"` — and `Subscription.ChannelData` carries the
presence bytes exactly as they arrived. Nothing in the server reads either,
nothing derives a tenant from either, and holding one allows no channel. A
policy cannot weigh evidence it is never shown, which is the only reason they
are there.

**An application that mounts this server ignores them.** The subject on the
Grant arrived through the host application's own front door, so a signature that
could also allow a subscription would be a second mechanism for one decision.
The place with no front door is `cmd/joaju`, which authenticates a browser not
at all — so there is no first mechanism for the signature to compete with, and
`cmd/joaju/policy.go` is where it is recomputed.

If you do recompute it, three things are load-bearing:

- The signed string is `"<socket id>:<channel>"`, or
  `"<socket id>:<channel>:<channel data>"` on a presence channel.
- The channel in it is `ChannelName.Requested()` and never
  `ChannelName.String()`. The client signed the name it sent.
- The socket id is what makes the signature one connection's. A policy that
  leaves it out accepts a signature anybody who saw it can replay —
  `TestSubscriptionPolicyRefusesASignatureIssuedForAnotherSocket`.

Compare in constant time, and re-encode nothing: `ChannelData` is
`Subscription.Member` before it was read, and the two are both carried because a
policy compares fields while a signature covers bytes.

A signature offered on a **public** channel is not a mistake and is not checked.
Clients send one where the application's endpoint answered for a channel it did
not have to, and refusing it would refuse a subscription that needed no evidence
in the first place — `TestSubscriptionPolicyIgnoresASignatureOnAPublicChannel`.
A missing signature and a missing socket id are refused separately from a wrong
one, because "your auth endpoint is not wired" and "your secret is wrong" are
different problems for the person reading the error.

## What to do when it will not compile

You are missing a Grant, and the answer is never to remove the parameter.

- **In a route or a frame handler**: ask the policy first. `routes.enter` and
  `routes.reach` in `protocols/pusher/routes.go` are the two halves.
- **In a test**: build the subject and go through the policy, so the test proves
  the refusal as well as the success.
- **Anywhere else**: if none of those fits, the design is wrong rather than the
  compiler. Say so instead of working around it.
