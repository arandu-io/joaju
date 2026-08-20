package joaju

import (
	"context"
	"strings"
	"testing"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
)

// FuzzNewChannelName feeds arbitrary names to the one constructor a client's
// channel string passes through.
//
// The name is the key a channel is held under, so a string that crosses this
// check is a string that reaches a map shared by every socket in the process,
// under whatever tenant the Grant carried. What is asserted is that the two
// halves of the published name stay separable, that the tenant is never the
// client's, and that the kind read off the prefix agrees with the prefix -- a
// guarded name classified public is a channel no [SubscriptionPolicy] guards.
func FuzzNewChannelName(f *testing.F) {
	f.Add("acme", "orders.17")
	f.Add("acme", "private-orders.17")
	f.Add("acme", "presence-room")
	f.Add("acme", "cache-quotes")
	f.Add("acme", "private-cache-quotes")
	f.Add("acme", "presence-cache-room")
	f.Add("acme", broadcasting.EncryptedPrivateChannelPrefix+"orders.17")
	f.Add("acme", EncryptedPrivateCacheChannelPrefix+"quotes")
	// The name that spells the separator, which is a client choosing whose
	// events it hears, and the tenant that spells it, which is one namespace
	// landing inside another.
	f.Add("acme", "globex:private-orders.17")
	f.Add("acme:globex", "private-orders.17")
	f.Add("", "private-orders.17")
	f.Add("acme", "")
	// Prefixes that only look like the guarded ones.
	f.Add("acme", "privateorders")
	f.Add("acme", "cache-private-orders")
	f.Add("acme", "presence-cache")
	f.Add("acme", "private-")
	f.Add("acme", strings.Repeat("a", 4096))

	f.Fuzz(func(t *testing.T, tenant, requested string) {
		g, err := auth.Authorize(context.Background(), connTestJoinPolicy{},
			auth.Subject{ID: "somebody", Tenant: tenant}, broadcasting.ChannelJoin, Subscription{})
		if err != nil {
			return
		}

		name, err := NewChannelName(g, requested)
		if err != nil {
			return
		}

		if name.IsZero() {
			t.Fatalf("naming %q gave the zero name, which is not a channel", requested)
		}
		if name.Requested() != requested {
			t.Fatalf("the client asked for %q and was given %q", requested, name.Requested())
		}
		if name.Tenant() != auth.Tenant(g) {
			t.Fatalf("the name carries tenant %q and the Grant carries %q", name.Tenant(), auth.Tenant(g))
		}
		if strings.Contains(name.Requested(), broadcasting.TenantSeparator) {
			t.Fatalf("%q was accepted, and the tenant comes from the Grant", name.Requested())
		}

		// The published name is what a [Broker] keys on and what the fleet
		// publishes to, so it has to split back into exactly the two halves it
		// was built from. A name that splits anywhere else is one customer
		// reaching another customer's channel by spelling it.
		published := name.String()
		if published != name.Tenant()+broadcasting.TenantSeparator+name.Requested() {
			t.Fatalf("the published name of %q is %q", requested, published)
		}
		gotTenant, gotChannel, found := broadcasting.CutTenant(published)
		if !found || gotTenant != name.Tenant() || gotChannel != name.Requested() {
			t.Fatalf("%q splits into %q and %q, want %q and %q",
				published, gotTenant, gotChannel, name.Tenant(), name.Requested())
		}

		// The kind is read off the prefix and nothing else records it, so the
		// two have to agree in both directions. The half that matters is the
		// second one: a name wearing a guarded prefix that reports itself public
		// is a subscription a policy may allow freely.
		guarded := strings.HasPrefix(requested, broadcasting.PrivateChannelPrefix) ||
			strings.HasPrefix(requested, broadcasting.PresenceChannelPrefix)
		if name.Type().Guarded() != guarded {
			t.Fatalf("%q is %s, and the prefix says guarded is %v", requested, name.Type(), guarded)
		}

		cache := strings.HasPrefix(requested, CacheChannelPrefix) ||
			strings.HasPrefix(requested, PrivateCacheChannelPrefix) ||
			strings.HasPrefix(requested, PresenceCacheChannelPrefix) ||
			strings.HasPrefix(requested, EncryptedPrivateCacheChannelPrefix)
		if name.Type().Cache() != cache {
			t.Fatalf("%q is %s, and the prefix says cache is %v", requested, name.Type(), cache)
		}

		presence := name.Type().Presence()
		if presence != strings.HasPrefix(requested, broadcasting.PresenceChannelPrefix) {
			t.Fatalf("%q is %s, and the prefix says presence is %v", requested, name.Type(), !presence)
		}
	})
}
