package main

import (
	"context"
	"errors"
	"testing"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
	"github.com/arandu-io/joaju"
)

// browser is the subject the front door mints for a socket: hesape's declared
// anonymous reader, carrying the tenant the process was configured with.
func browser(tenant string) auth.Subject { return auth.Guest(tenant) }

// application is the subject the front door mints for a caller that signed with
// the app secret.
func application(tenant string) auth.Subject {
	return auth.Subject{ID: applicationSubjectPrefix + testAppKey, Tenant: tenant, Roles: []string{roleApplication}}
}

// named builds a [joaju.ChannelName], which cannot be written as a literal: its
// tenant comes off a Grant and from nowhere else.
func named(t *testing.T, tenant, requested string) joaju.ChannelName {
	t.Helper()

	name, err := joaju.NewChannelName(auth.SystemGrant(joaju.Connect, tenant), requested)
	if err != nil {
		t.Fatalf("NewChannelName(%q): %v", requested, err)
	}

	return name
}

func TestConnectPolicyAnswersTheSocketRouteAndTheAPIRoute(t *testing.T) {
	t.Parallel()

	policy := connectPolicy{tenant: testTenant}

	// A socket carries the id minted for it; an API caller asks the same
	// question with none, which is how the two are told apart.
	if _, err := auth.Authorize(context.Background(), policy, browser(testTenant), joaju.Connect,
		joaju.Handshake{Socket: "1.1", Origin: "https://app.example.com"}); err != nil {
		t.Errorf("a browser was refused with no origin list configured: %v", err)
	}
	if _, err := auth.Authorize(context.Background(), policy, application(testTenant), joaju.Connect,
		joaju.Handshake{}); err != nil {
		t.Errorf("the application was refused on an API route: %v", err)
	}
	// A caller that did not sign has no business on the API routes, whatever
	// tenant it carries.
	if _, err := auth.Authorize(context.Background(), policy, browser(testTenant), joaju.Connect,
		joaju.Handshake{}); !errors.Is(err, auth.ErrForbidden) {
		t.Errorf("error = %v, want auth.ErrForbidden: an unsigned caller reached an API route", err)
	}
}

func TestConnectPolicyRefusesAnotherTenant(t *testing.T) {
	t.Parallel()

	// Nothing in this process mints a subject for another tenant, so this is a
	// wiring mistake rather than a customer -- and it is refused where it would
	// otherwise become a Grant carrying somebody else's namespace (RULE 14).
	policy := connectPolicy{tenant: testTenant}

	if _, err := auth.Authorize(context.Background(), policy, browser("another"), joaju.Connect,
		joaju.Handshake{Socket: "1.1"}); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("error = %v, want auth.ErrForbidden", err)
	}
}

func TestConnectPolicyRefusesAnOriginItWasNotGiven(t *testing.T) {
	t.Parallel()

	policy := connectPolicy{tenant: testTenant, origins: []string{"https://app.example.com"}}

	for name, handshake := range map[string]joaju.Handshake{
		"an origin on the list":     {Socket: "1.1", Origin: "https://app.example.com"},
		"an origin off the list":    {Socket: "1.1", Origin: "https://evil.example.com"},
		"the same host, no scheme":  {Socket: "1.1", Origin: "app.example.com"},
		"another scheme, same host": {Socket: "1.1", Origin: "http://app.example.com"},
		"no origin at all":          {Socket: "1.1"},
	} {
		allowed := handshake.Origin == "https://app.example.com"

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := auth.Authorize(context.Background(), policy, browser(testTenant), joaju.Connect, handshake)
			switch {
			case allowed && err != nil:
				t.Fatalf("%q was refused: %v", handshake.Origin, err)
			case !allowed && !errors.Is(err, auth.ErrForbidden):
				t.Fatalf("%q was allowed, error = %v", handshake.Origin, err)
			}
		})
	}

	// The list narrows the socket route and says nothing about the API, whose
	// credential is the signature and which carries no Origin at all.
	if _, err := auth.Authorize(context.Background(), policy, application(testTenant), joaju.Connect,
		joaju.Handshake{}); err != nil {
		t.Errorf("an origin list refused a signed API call that carries no origin: %v", err)
	}
}

func TestConnectPolicyAnswersOnlyItsOwnAction(t *testing.T) {
	t.Parallel()

	// A Grant is issued for one action and joaju checks which. A policy that
	// answered any action would issue one for whatever it was asked about.
	policy := connectPolicy{tenant: testTenant}

	if _, err := auth.Authorize(context.Background(), policy, application(testTenant),
		broadcasting.ChannelJoin, joaju.Handshake{}); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("error = %v, want auth.ErrForbidden", err)
	}
}

func TestSubscriptionPolicyAllowsTheChannelsThatNeedNoDecision(t *testing.T) {
	t.Parallel()

	policy := subscriptionPolicy{tenant: testTenant}

	// Public and public cache: joaju.ChannelType.Guarded answers no for both,
	// and the tenant is already in the name and came off a Grant.
	for _, requested := range []string{"orders", "cache-orders"} {
		subscription := joaju.Subscription{Channel: named(t, testTenant, requested), Socket: "1.1"}
		if _, err := auth.Authorize(context.Background(), policy, browser(testTenant),
			broadcasting.ChannelJoin, subscription); err != nil {
			t.Errorf("a browser was refused %q: %v", requested, err)
		}
	}
}

func TestSubscriptionPolicyRefusesAGuardedChannelToABrowser(t *testing.T) {
	t.Parallel()

	// This is the decision this binary does not have, and the refusal is the
	// honest answer rather than a signature it cannot see: the "auth" field of
	// the subscribe frame never reaches a policy, because joaju.Subscription
	// deliberately carries no field for it.
	policy := subscriptionPolicy{tenant: testTenant}

	for _, requested := range []string{
		"private-orders.17",
		"presence-room.9",
		"private-cache-orders.17",
		"presence-cache-room.9",
	} {
		subscription := joaju.Subscription{Channel: named(t, testTenant, requested), Socket: "1.1"}
		if _, err := auth.Authorize(context.Background(), policy, browser(testTenant),
			broadcasting.ChannelJoin, subscription); !errors.Is(err, auth.ErrForbidden) {
			t.Errorf("%q was allowed to a browser this process cannot identify, error = %v", requested, err)
		}
	}
}

func TestSubscriptionPolicyAllowsTheApplicationEveryChannelOfItsOwnTenant(t *testing.T) {
	t.Parallel()

	// The application holds the secret, so it is the party that would have
	// decided about everybody else. Refusing it its own channels would leave the
	// publish and metrics routes unable to reach half the protocol.
	policy := subscriptionPolicy{tenant: testTenant}

	for _, requested := range []string{"orders", "private-orders.17", "presence-room.9"} {
		subscription := joaju.Subscription{Channel: named(t, testTenant, requested)}
		if _, err := auth.Authorize(context.Background(), policy, application(testTenant),
			broadcasting.ChannelJoin, subscription); err != nil {
			t.Errorf("the application was refused %q: %v", requested, err)
		}
	}
}

func TestSubscriptionPolicyRefusesAnotherTenantsChannel(t *testing.T) {
	t.Parallel()

	// The name was built under another tenant's Grant. Nothing in this process
	// produces one, and if something did, this is where it stops.
	policy := subscriptionPolicy{tenant: testTenant}
	subscription := joaju.Subscription{Channel: named(t, "another", "orders")}

	if _, err := auth.Authorize(context.Background(), policy, application("another"),
		broadcasting.ChannelJoin, subscription); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("error = %v, want auth.ErrForbidden", err)
	}
}

func TestSubscriptionPolicyAnswersOnlyItsOwnAction(t *testing.T) {
	t.Parallel()

	policy := subscriptionPolicy{tenant: testTenant}
	subscription := joaju.Subscription{Channel: named(t, testTenant, "orders")}

	if _, err := auth.Authorize(context.Background(), policy, application(testTenant),
		joaju.Connect, subscription); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("error = %v, want auth.ErrForbidden", err)
	}
}
