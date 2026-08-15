package main

import (
	"context"
	"crypto/hmac"
	"fmt"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
	"github.com/arandu-io/joaju"
)

// connectPolicy decides who may be on this process at all, and is the first of
// the two decisions joaju makes.
//
// It answers two different callers, and [joaju.Handshake.Socket] is how they are
// told apart -- an API route asks the same question with no socket to open, which
// is [joaju.Server]'s own arrangement and not an invention here:
//
//	Socket set    a browser upgrading. The origin is checked.
//	Socket empty  the application, having signed with the app secret.
//
// The origin list belongs here rather than in configuration read by the
// transport, because refusing an origin is a decision about who may connect and
// this repository has one place where those are made. It can only narrow: the
// transport already refuses an Origin naming another host before the socket
// exists, and no value of this policy can allow one it refused.
type connectPolicy struct {
	// tenant is the one this process serves. A subject carrying another is a
	// wiring mistake, not a customer, because nothing here mints a subject for
	// anybody else.
	tenant string
	// origins is the allowlist, verbatim and complete. Empty means the
	// transport's same-origin default is the whole check.
	origins []string
}

// Can decides one handshake.
func (p connectPolicy) Can(_ context.Context, s auth.Subject, a auth.Action, h joaju.Handshake) error {
	if a != joaju.Connect {
		return fmt.Errorf("%s is not %s, which is the only action this policy answers", a, joaju.Connect)
	}
	if s.Tenant != p.tenant {
		return fmt.Errorf("this process serves the tenant %q and the subject carries %q", p.tenant, s.Tenant)
	}

	if h.Socket == "" {
		// An API route. What is being asked is whether this caller may act on
		// this server, and the answer is the credential it arrived with: the
		// signature was verified at the front door, and nothing else in this
		// process mints the role it left behind.
		if !s.HasRole(roleApplication) {
			return fmt.Errorf("the API routes answer the application, and %q did not sign with the app secret", s.ID)
		}

		return nil
	}

	if len(p.origins) == 0 {
		return nil
	}
	for _, allowed := range p.origins {
		if h.Origin == allowed {
			return nil
		}
	}

	return fmt.Errorf("the origin %q is not one this server accepts sockets from", h.Origin)
}

// subscriptionPolicy decides who may hear one channel, and is the second of the
// two decisions. It runs for every channel, on every subscription, and on every
// API route that names one, because subscribing is a read and RULE 17 opens no
// exception for reads.
//
// # What it decides on
//
// A channel that [joaju.ChannelType.Guarded] answers no for -- public and
// public cache -- needs no decision beyond the tenant, and the tenant is in
// every name and came from a Grant. Those it allows, whatever else the client
// sent with them.
//
// A private or presence channel is a decision about a person, and this process
// knows none: it authenticates the application by its app secret and a browser
// not at all. What it has instead is what the Pusher protocol puts in the
// subscribe frame -- an HMAC the application computed over
// "socket_id:channel", or "socket_id:channel:channel_data" on a presence
// channel, with the same shared secret. It reaches this policy as
// [joaju.Subscription.Auth], and this is what recomputes it.
//
// That is not a second way to allow a subscription, which is what
// [joaju.SubscribeRequest.Auth] rules out and still rules out for an
// application that has a front door of its own. This process is the case with
// no front door: nothing else in it identifies a browser, so there is no first
// mechanism for this to compete with. And it stays evidence rather than
// authority -- the signature allows nothing on its own, the tenant is the
// process's and comes off the Grant either way (RULE 14), and this policy is
// still what says yes.
//
// It is the same division Reverb reaches by the opposite route: there the socket
// server is always separate and the application always signs for it over HTTP;
// here an application that holds the server decides directly, and only a process
// standing alone falls back to the signature.
type subscriptionPolicy struct {
	// tenant is the one this process serves, checked again here: a Grant issued
	// for Connect is not a Grant to listen, and a subject that reached this
	// policy by another path is still refused.
	tenant string
	// appKey is what the half of a signature in front of the colon has to be,
	// and secret is what the half behind it has to be computed with. They are
	// the same two values [frontDoor] verifies the HTTP API with, because the
	// Pusher protocol has one shared secret and not two.
	appKey string
	secret []byte
}

// Can decides one subscription.
func (p subscriptionPolicy) Can(_ context.Context, s auth.Subject, a auth.Action, sub joaju.Subscription) error {
	if a != broadcasting.ChannelJoin {
		return fmt.Errorf("%s is not %s, which is the only action this policy answers", a, broadcasting.ChannelJoin)
	}
	if s.Tenant != p.tenant {
		return fmt.Errorf("this process serves the tenant %q and the subject carries %q", p.tenant, s.Tenant)
	}
	if sub.Channel.IsZero() {
		return fmt.Errorf("the subscription named no channel")
	}

	if !sub.Channel.Type().Guarded() {
		// A signature on a public channel is not a mistake and is not checked.
		// The clients send one where the application's endpoint answered for a
		// channel it did not have to, and refusing it would refuse a
		// subscription that needed no evidence in the first place.
		return nil
	}
	if s.HasRole(roleApplication) {
		// The application signed with the shared secret, so it is the party that
		// would have decided about everybody else. Refusing it its own channels
		// would leave the publish and metrics routes unable to reach the half of
		// the protocol they exist for.
		return nil
	}

	return p.verify(sub)
}

// verify recomputes the signature the client offered for a guarded channel.
//
// The refusals are three, and the first two are not ceremony. A subscription
// with no signature is the client that never asked its application, and saying
// so is what tells a developer their auth endpoint is not wired rather than
// that their secret is wrong. A subscription with no socket id is worse: the
// socket id is the whole of what binds a signature to one connection, so
// checking one without it would accept forever, from anybody, a signature
// issued once for somebody.
func (p subscriptionPolicy) verify(sub joaju.Subscription) error {
	if sub.Auth == "" {
		return fmt.Errorf(
			"a %s channel needs the signature the application issues for it, and the subscription to %s carried none",
			sub.Channel.Type(), sub.Channel.Requested())
	}
	if sub.Socket == "" {
		return fmt.Errorf(
			"the subscription to %s named no socket, and a signature not bound to one is a signature anybody who saw it can replay",
			sub.Channel.Requested())
	}

	// The app key is in front of the colon, so a signature computed for another
	// application is refused by the same comparison rather than by a separate
	// one that would answer earlier and say more.
	want := p.appKey + ":" + signature(p.secret, subscriptionSigningString(sub))
	// hmac.Equal and not ==, for the reason [frontDoor.verify] gives: comparing
	// a secret-derived value against one the caller chose is the comparison
	// whose duration says how many leading characters were right.
	if !hmac.Equal([]byte(want), []byte(sub.Auth)) {
		return fmt.Errorf("the signature offered for %s is not the one this server computed", sub.Channel.Requested())
	}

	return nil
}

// subscriptionSigningString builds the string a Pusher subscription signature is
// computed over: "<socket id>:<channel>", and with the presence data appended
// where the client sent some.
//
// The channel is [joaju.ChannelName.Requested] and never its String: the client
// signed the name it sent, and the tenant this process scoped it to is neither
// something it knows nor something it was allowed to choose (RULE 14).
//
// The presence data is appended as it arrived and is not re-encoded. It is
// already the exact bytes of the frame's channel_data -- see
// [joaju.Subscription.ChannelData] -- and a re-encoding that reordered a key or
// dropped a space would hash to something the application never signed.
func subscriptionSigningString(sub joaju.Subscription) string {
	signing := string(sub.Socket) + ":" + sub.Channel.Requested()
	if len(sub.ChannelData) > 0 {
		signing += ":" + string(sub.ChannelData)
	}

	return signing
}
