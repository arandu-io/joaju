package main

import (
	"context"
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
// # What it can decide, and what it deliberately does not
//
// A channel that [joaju.ChannelType.Guarded] answers no for -- public and
// public cache -- needs no decision beyond the tenant, and the tenant is in
// every name and came from a Grant. Those it allows.
//
// A private or presence channel is a decision about a person, and this process
// does not know any: it authenticates the application by its app secret and a
// browser not at all, so the only honest answer it has is a refusal. In the
// Pusher protocol the missing half arrives as the "auth" field of the subscribe
// frame -- an HMAC the application computed over "socket_id:channel" with the
// shared secret. This server does not read it, and that is a decision already
// taken and written down in [joaju.SubscribeRequest.Auth]: a signature that
// authorizes a channel without naming a tenant would be a second way to allow a
// subscription, and [joaju.Subscription] therefore carries no field for it. A
// policy cannot check evidence it is never shown.
//
// So the standalone process serves the public half of the protocol, and a
// deployment that needs private and presence channels mounts [joaju.Server]
// inside its application with a policy that knows its people. That is the same
// division Reverb has and reaches by the opposite route: there the socket server
// is separate and the application signs for it over HTTP; here the application
// can hold the server, so it can decide directly.
type subscriptionPolicy struct {
	// tenant is the one this process serves, checked again here: a Grant issued
	// for Connect is not a Grant to listen, and a subject that reached this
	// policy by another path is still refused.
	tenant string
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
		return nil
	}
	if s.HasRole(roleApplication) {
		// The application signed with the shared secret, so it is the party that
		// would have decided about everybody else. Refusing it its own channels
		// would leave the publish and metrics routes unable to reach the half of
		// the protocol they exist for.
		return nil
	}

	return fmt.Errorf(
		"a %s channel needs a decision about a person, and this process authenticates none: it accepts the application by its app secret and a browser as an anonymous reader. Serve %s by mounting joaju.Server inside the application, behind its own SubscriptionPolicy",
		sub.Channel.Type(), sub.Channel.Requested())
}
