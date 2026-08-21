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

// subscriptions is the policy wired the way [newServer] wires it, with the app
// key a signature has to name and the secret it has to be computed with.
func subscriptions() subscriptionPolicy {
	return subscriptionPolicy{tenant: testTenant, appKey: testAppKey, secret: []byte(testAppSecret)}
}

// The three signatures below were not computed by this package.
//
// They came off the command line, under the same secret testAppSecret carries:
//
//	printf '%s' '1234.1234:private-foobar' | openssl dgst -sha256 -hmac '7ad3773142a6692b25b8'
//
// A test that signs with the code it is testing agrees with itself no matter
// where the colons went, so it proves the HMAC and nothing about the string it
// is taken over. These are the check on the string. They are also the Pusher
// protocol's own published examples, so a client signing by the specification
// and this server checking by it reach the same 64 characters.
const (
	// externalPrivateSignature is over "1234.1234:private-foobar".
	externalPrivateSignature = "58df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a4"
	// externalPresenceSignature is over
	// `1234.1234:presence-foobar:{"user_id":10,"user_info":{"name":"Mr. Pusher"}}`.
	externalPresenceSignature = "afaed3695da2ffd16931f457e338e6c9f2921fa133ce7dac49f529792be6304c"
	// externalOtherSocketSignature is over "1234.5678:private-foobar": a real
	// signature, issued by the application, for a different socket.
	externalOtherSocketSignature = "84f3ef7d76312ffd95cadfa1ef3df17c08315790d0e22536847e96ae7e46c504"
)

// presenceChannelData is the channel_data the presence signature covers, byte
// for byte as a client would put it in the frame.
const presenceChannelData = `{"user_id":10,"user_info":{"name":"Mr. Pusher"}}`

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
	// otherwise become a Grant carrying somebody else's namespace.
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

	policy := subscriptions()

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

// A signature on a channel that needs none is ignored rather than checked. The
// clients send one wherever their auth endpoint answered, and a public channel
// that refused a subscription over evidence it never wanted would be a public
// channel that is not public.
func TestSubscriptionPolicyIgnoresASignatureOnAPublicChannel(t *testing.T) {
	t.Parallel()

	policy := subscriptions()

	for _, offered := range []string{
		testAppKey + ":" + externalPrivateSignature,
		"nonsense",
		"another-key:" + externalPrivateSignature,
	} {
		subscription := joaju.Subscription{
			Channel: named(t, testTenant, "orders"),
			Socket:  "1234.1234",
			Auth:    offered,
		}
		if _, err := auth.Authorize(context.Background(), policy, browser(testTenant),
			broadcasting.ChannelJoin, subscription); err != nil {
			t.Errorf("a public channel was refused over the signature %q: %v", offered, err)
		}
	}
}

// The signature the application issued is the whole of what this process knows
// about a browser, and it is what opens a private channel to one.
func TestSubscriptionPolicyAllowsTheSignatureTheApplicationIssued(t *testing.T) {
	t.Parallel()

	policy := subscriptions()
	subscription := joaju.Subscription{
		Channel: named(t, testTenant, "private-foobar"),
		Socket:  "1234.1234",
		Auth:    testAppKey + ":" + externalPrivateSignature,
	}

	if _, err := auth.Authorize(context.Background(), policy, browser(testTenant),
		broadcasting.ChannelJoin, subscription); err != nil {
		t.Fatalf("a browser holding the application's signature was refused: %v", err)
	}
}

// The presence body is one part longer, and the part is the channel_data the
// client sent -- byte for byte, because the application signed those bytes.
func TestSubscriptionPolicyAllowsAPresenceSignatureOverTheChannelData(t *testing.T) {
	t.Parallel()

	policy := subscriptions()
	subscription := joaju.Subscription{
		Channel:     named(t, testTenant, "presence-foobar"),
		Socket:      "1234.1234",
		Member:      joaju.Member{UserID: "10", Info: []byte(`{"name":"Mr. Pusher"}`)},
		Auth:        testAppKey + ":" + externalPresenceSignature,
		ChannelData: []byte(presenceChannelData),
	}

	if _, err := auth.Authorize(context.Background(), policy, browser(testTenant),
		broadcasting.ChannelJoin, subscription); err != nil {
		t.Fatalf("a presence subscription carrying the signed channel_data was refused: %v", err)
	}

	// The same signature over presence data the client edited on the way. This
	// is the only thing standing between a signed membership of one person and
	// a channel joined as another.
	tampered := subscription
	tampered.ChannelData = []byte(`{"user_id":11,"user_info":{"name":"Mr. Pusher"}}`)
	if _, err := auth.Authorize(context.Background(), policy, browser(testTenant),
		broadcasting.ChannelJoin, tampered); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("channel_data the signature does not cover was allowed, error = %v", err)
	}

	// And the signed data with the third part left off, which is the same
	// signature offered for a shorter string.
	dropped := subscription
	dropped.ChannelData = nil
	if _, err := auth.Authorize(context.Background(), policy, browser(testTenant),
		broadcasting.ChannelJoin, dropped); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("a presence signature was allowed without the channel_data it covers, error = %v", err)
	}
}

// The replay the socket id exists to stop: a signature the application really
// issued, offered by the connection it was not issued for.
func TestSubscriptionPolicyRefusesASignatureIssuedForAnotherSocket(t *testing.T) {
	t.Parallel()

	policy := subscriptions()
	replayed := joaju.Subscription{
		Channel: named(t, testTenant, "private-foobar"),
		Socket:  "1234.1234",
		Auth:    testAppKey + ":" + externalOtherSocketSignature,
	}

	if _, err := auth.Authorize(context.Background(), policy, browser(testTenant),
		broadcasting.ChannelJoin, replayed); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("a signature issued for socket 1234.5678 opened socket 1234.1234, error = %v", err)
	}

	// On its own socket the same string is allowed, so what was refused above
	// was the replay and not a signature that was never valid.
	its := replayed
	its.Socket = "1234.5678"
	if _, err := auth.Authorize(context.Background(), policy, browser(testTenant),
		broadcasting.ChannelJoin, its); err != nil {
		t.Fatalf("the socket the signature was issued for was refused it: %v", err)
	}
}

func TestSubscriptionPolicyRefusesAGuardedChannelWithoutTheApplicationsSignature(t *testing.T) {
	t.Parallel()

	policy := subscriptions()
	valid := testAppKey + ":" + externalPrivateSignature

	for name, offered := range map[string]string{
		"none at all":            "",
		"the key alone":          testAppKey + ":",
		"another application":    "278d425bdf160c739804:" + externalPrivateSignature,
		"one character changed":  valid[:len(valid)-1] + "5",
		"no key in front of it":  externalPrivateSignature,
		"a signature of its own": testAppKey + ":" + externalPresenceSignature,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			subscription := joaju.Subscription{
				Channel: named(t, testTenant, "private-foobar"),
				Socket:  "1234.1234",
				Auth:    offered,
			}
			if _, err := auth.Authorize(context.Background(), policy, browser(testTenant),
				broadcasting.ChannelJoin, subscription); !errors.Is(err, auth.ErrForbidden) {
				t.Fatalf("%q opened a private channel, error = %v", offered, err)
			}
		})
	}
}

// A signature with no socket to belong to is one anybody who saw it can offer,
// so it is refused before it is computed.
func TestSubscriptionPolicyRefusesASignatureBoundToNoSocket(t *testing.T) {
	t.Parallel()

	policy := subscriptions()
	subscription := joaju.Subscription{
		Channel: named(t, testTenant, "private-foobar"),
		Auth:    testAppKey + ":" + signature([]byte(testAppSecret), ":private-foobar"),
	}

	if _, err := auth.Authorize(context.Background(), policy, browser(testTenant),
		broadcasting.ChannelJoin, subscription); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("a subscription naming no socket was allowed, error = %v", err)
	}
}

// Every guarded kind goes through the same check, and the compound prefixes are
// the ones a second copy of the prefix switch would get wrong.
func TestSubscriptionPolicyChecksEveryGuardedKindOfChannel(t *testing.T) {
	t.Parallel()

	policy := subscriptions()

	for _, requested := range []string{
		"private-orders.17",
		"presence-room.9",
		"private-cache-orders.17",
		"presence-cache-room.9",
	} {
		signed := joaju.Subscription{
			Channel: named(t, testTenant, requested),
			Socket:  "1234.1234",
			Auth:    testAppKey + ":" + signature([]byte(testAppSecret), "1234.1234:"+requested),
		}
		if _, err := auth.Authorize(context.Background(), policy, browser(testTenant),
			broadcasting.ChannelJoin, signed); err != nil {
			t.Errorf("%q was refused to a browser holding its signature: %v", requested, err)
		}

		unsigned := signed
		unsigned.Auth = ""
		if _, err := auth.Authorize(context.Background(), policy, browser(testTenant),
			broadcasting.ChannelJoin, unsigned); !errors.Is(err, auth.ErrForbidden) {
			t.Errorf("%q was allowed with no signature at all, error = %v", requested, err)
		}
	}
}

func TestSubscriptionPolicyAllowsTheApplicationEveryChannelOfItsOwnTenant(t *testing.T) {
	t.Parallel()

	// The application holds the secret, so it is the party that would have
	// decided about everybody else. Refusing it its own channels would leave the
	// publish and metrics routes unable to reach half the protocol.
	policy := subscriptions()

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
	policy := subscriptions()
	subscription := joaju.Subscription{Channel: named(t, "another", "orders")}

	if _, err := auth.Authorize(context.Background(), policy, application("another"),
		broadcasting.ChannelJoin, subscription); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("error = %v, want auth.ErrForbidden", err)
	}
}

func TestSubscriptionPolicyAnswersOnlyItsOwnAction(t *testing.T) {
	t.Parallel()

	policy := subscriptions()
	subscription := joaju.Subscription{Channel: named(t, testTenant, "orders")}

	if _, err := auth.Authorize(context.Background(), policy, application(testTenant),
		joaju.Connect, subscription); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("error = %v, want auth.ErrForbidden", err)
	}
}
