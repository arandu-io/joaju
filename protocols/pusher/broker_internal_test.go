package pusher

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/joaju"
)

// Everything in this file is named for the broker, because the package is one
// package and another file's helpers share the namespace. The Grants, the
// sockets and the names come from channels_internal_test.go, which already
// mints them through the policies that issue them.

// brokerTestCreate is the channel a client of tenant asks for, made through the
// broker.
func brokerTestCreate(t *testing.T, b joaju.Broker, tenant, requested string) joaju.Channel {
	t.Helper()

	held, err := b.FindOrCreate(context.Background(), channelTestJoinGrant(t, tenant, "maker"), channelTestName(t, tenant, requested))
	if err != nil {
		t.Fatalf("creating %s in %s: %v", requested, tenant, err)
	}

	return held
}

// brokerTestRequested is the requested names of a list of channels, which is
// what an assertion about [joaju.Broker.All] reads.
func brokerTestRequested(channels []joaju.Channel) []string {
	names := make([]string, 0, len(channels))
	for _, held := range channels {
		names = append(names, held.Name().Requested())
	}

	return names
}

func TestMemoryBrokerFindAnswersErrNoChannelForOneNobodyMade(t *testing.T) {
	b := NewMemoryBroker()

	_, err := b.Find(context.Background(), channelTestJoinGrant(t, "acme", "u1"), channelTestName(t, "acme", "orders.17"))
	if !errors.Is(err, joaju.ErrNoChannel) {
		t.Fatalf("Find() of a channel nobody made = %v, want ErrNoChannel", err)
	}
}

func TestMemoryBrokerFindOrCreateHandsBackTheChannelItMade(t *testing.T) {
	b := NewMemoryBroker()
	ctx := context.Background()
	g := channelTestJoinGrant(t, "acme", "u1")
	name := channelTestName(t, "acme", "private-orders.17")

	made, err := b.FindOrCreate(ctx, g, name)
	if err != nil {
		t.Fatalf("FindOrCreate() = %v", err)
	}
	if made.Name().String() != name.String() {
		t.Fatalf("the channel is held as %q and was asked for as %q", made.Name().String(), name.String())
	}
	if made.Name().Type() != joaju.PrivateChannel {
		t.Fatalf("%s was made a %s channel", name.Requested(), made.Name().Type())
	}

	again, err := b.FindOrCreate(ctx, g, name)
	if err != nil {
		t.Fatalf("FindOrCreate() a second time = %v", err)
	}
	if again != made {
		t.Fatal("the second subscriber of a channel was given a second channel: a publish reaches one of the two and the subscribers of the other hear nothing")
	}

	found, err := b.Find(ctx, g, name)
	if err != nil {
		t.Fatalf("Find() of the channel just created = %v", err)
	}
	if found != made {
		t.Fatal("Find() answered with a channel other than the one FindOrCreate() made")
	}
}

// The key is [joaju.ChannelName.String] and this is what that buys. Two customers ask
// for the same name all day; a registry keyed by [joaju.ChannelName.Requested] would
// merge them, and the merge is not an error anywhere -- both asked for a channel
// that exists.
func TestMemoryBrokerKeepsTwoTenantsAskingForTheSameNameApart(t *testing.T) {
	b := NewMemoryBroker()
	ctx := context.Background()

	mine, err := b.FindOrCreate(ctx, channelTestJoinGrant(t, "acme", "u1"), channelTestName(t, "acme", "private-orders.17"))
	if err != nil {
		t.Fatalf("creating private-orders.17 in acme: %v", err)
	}
	theirs, err := b.FindOrCreate(ctx, channelTestJoinGrant(t, "globex", "u1"), channelTestName(t, "globex", "private-orders.17"))
	if err != nil {
		t.Fatalf("creating private-orders.17 in globex: %v", err)
	}

	if mine == theirs {
		t.Fatalf("two customers asking for %q landed on one channel: every order event of one is delivered to the other", "private-orders.17")
	}
	if mine.Name().Tenant() != "acme" || theirs.Name().Tenant() != "globex" {
		t.Fatalf("the channels belong to %q and %q", mine.Name().Tenant(), theirs.Name().Tenant())
	}
}

// This is the refusal [joaju.ErrWrongTenant] exists for: a name built under one Grant
// and carried to another. Both values are valid, and the comparison in the
// broker is what notices.
func TestMemoryBrokerRefusesAGrantFromAnotherTenant(t *testing.T) {
	b := NewMemoryBroker()
	ctx := context.Background()

	held := brokerTestCreate(t, b, "acme", "private-orders.17")
	name := held.Name()
	theirs := channelTestJoinGrant(t, "globex", "u1")

	if _, err := b.Find(ctx, theirs, name); !errors.Is(err, joaju.ErrWrongTenant) {
		t.Fatalf("a client of globex was answered acme's %q: %v", name.Requested(), err)
	} else if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("the refusal does not read as forbidden, so a handler answers 500 instead of 403: %v", err)
	}
	if _, err := b.FindOrCreate(ctx, theirs, name); !errors.Is(err, joaju.ErrWrongTenant) {
		t.Fatalf("a client of globex reached acme's %q through FindOrCreate: %v", name.Requested(), err)
	}
	if err := b.Remove(ctx, theirs, name); !errors.Is(err, joaju.ErrWrongTenant) {
		t.Fatalf("a client of globex removed acme's %q: %v", name.Requested(), err)
	}

	// The refused Remove did not take the channel with it, and the refused
	// FindOrCreate did not leave a second one behind.
	still, err := b.Find(ctx, channelTestJoinGrant(t, "acme", "u1"), name)
	if err != nil {
		t.Fatalf("acme's own channel is gone after globex was refused: %v", err)
	}
	if still != held {
		t.Fatal("acme's channel was replaced by the one a refused FindOrCreate built")
	}
}

// The Grant that opened the socket is not the Grant that reaches a channel. If
// it were, no [joaju.SubscriptionPolicy] would ever run and
// every channel of the tenant would be readable by anyone allowed to connect.
func TestMemoryBrokerRefusesTheGrantThatOpenedTheSocket(t *testing.T) {
	b := NewMemoryBroker()
	ctx := context.Background()
	conn, _ := channelTestConnection(t, "acme", "u1")
	name := channelTestName(t, "acme", "orders.17")

	if _, err := b.FindOrCreate(ctx, conn.Grant(), name); err == nil {
		t.Fatal("the grant that opened the socket also reached a channel: no subscription policy was consulted")
	} else if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("refused, but not as an authorization failure: %v", err)
	}
	if _, err := b.Find(ctx, conn.Grant(), name); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("Find() with the connection's own grant = %v", err)
	}
	if _, err := b.All(ctx, conn.Grant()); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("All() with the connection's own grant = %v", err)
	}
	if err := b.Remove(ctx, conn.Grant(), name); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("Remove() with the connection's own grant = %v", err)
	}

	// The zero Grant is the one a caller outside hesape/auth can write as a
	// struct literal, and it reaches nothing either.
	if _, err := b.All(ctx, auth.Grant{}); !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("All() with the zero grant = %v", err)
	}
}

// The most dangerous method in the file: one process holds every customer's
// channels, and this is the list route.
func TestMemoryBrokerAllAnswersOnlyTheGrantsTenant(t *testing.T) {
	b := NewMemoryBroker()

	brokerTestCreate(t, b, "acme", "orders.17")
	brokerTestCreate(t, b, "acme", "private-orders.18")
	brokerTestCreate(t, b, "globex", "orders.17")
	brokerTestCreate(t, b, "globex", "presence-lobby")

	all, err := b.All(context.Background(), channelTestJoinGrant(t, "acme", "u1"))
	if err != nil {
		t.Fatalf("All() = %v", err)
	}

	for _, held := range all {
		if held.Name().Tenant() != "acme" {
			t.Fatalf("a client of acme was answered %q of %s: the list route publishes one customer's channel names to another", held.Name().Requested(), held.Name().Tenant())
		}
	}
	// Sorted by [joaju.ChannelName.String], which inside one tenant is the requested
	// name: a route that answers in a different order every time is one nobody
	// can diff.
	want := []string{"orders.17", "private-orders.18"}
	if got := brokerTestRequested(all); !slices.Equal(got, want) {
		t.Fatalf("All() = %v, want %v", got, want)
	}
}

func TestMemoryBrokerRemoveOfAChannelNobodyMadeIsNotAnError(t *testing.T) {
	b := NewMemoryBroker()

	if err := b.Remove(context.Background(), channelTestJoinGrant(t, "acme", "u1"), channelTestName(t, "acme", "orders.17")); err != nil {
		t.Fatalf("removing a channel nobody made = %v, and whoever unwinds a subscription does not know what has already unwound", err)
	}
}

// Where a channel stops existing, and the one case that must not be believed
// from the caller: it emptied, then somebody subscribed, then Remove arrived.
func TestMemoryBrokerRemovesAChannelOnlyWhenItIsEmpty(t *testing.T) {
	b := NewMemoryBroker()
	ctx := context.Background()
	g := channelTestJoinGrant(t, "acme", "u1")
	name := channelTestName(t, "acme", "orders.17")

	held, err := b.FindOrCreate(ctx, g, name)
	if err != nil {
		t.Fatalf("FindOrCreate() = %v", err)
	}
	conn, _ := channelTestConnection(t, "acme", "u1")
	if err := held.Subscribe(ctx, g, conn, joaju.Member{}); err != nil {
		t.Fatalf("subscribing to %s: %v", name.Requested(), err)
	}

	if err := b.Remove(ctx, g, name); err != nil {
		t.Fatalf("Remove() of a channel with a subscriber on it = %v", err)
	}
	still, err := b.Find(ctx, g, name)
	if err != nil {
		t.Fatalf("a channel with a subscriber on it was dropped: the next subscriber builds a second channel under the same name, and the socket seated on the first hears nothing and is told nothing (%v)", err)
	}
	if still != held {
		t.Fatal("the channel was replaced rather than kept")
	}

	// The last subscriber leaves, which is what [joaju.Observer.ChannelRemoved] means.
	if err := held.Unsubscribe(ctx, conn); err != nil {
		t.Fatalf("unsubscribing from %s: %v", name.Requested(), err)
	}
	if err := b.Remove(ctx, g, name); err != nil {
		t.Fatalf("Remove() of an empty channel = %v", err)
	}
	if _, err := b.Find(ctx, g, name); !errors.Is(err, joaju.ErrNoChannel) {
		t.Fatalf("the last subscriber left, the channel was removed, and it is still held: %v", err)
	}

	// And it is made again by the next subscriber, rather than staying gone.
	made, err := b.FindOrCreate(ctx, g, name)
	if err != nil {
		t.Fatalf("FindOrCreate() after the channel was removed = %v", err)
	}
	if made == held {
		t.Fatal("the removed channel was handed out again")
	}
}

// One name is one channel, however many goroutines ask for it at once. Two
// channels under one name is a broadcast that reaches half the subscribers, and
// nothing reports it.
func TestMemoryBrokerHandsOneChannelToEverySubscriberOfIt(t *testing.T) {
	b := NewMemoryBroker()
	name := channelTestName(t, "acme", "orders.17")

	// The Grants are minted here rather than inside the goroutines, because the
	// helper fails the test and only the test goroutine may do that.
	grants := make([]auth.Grant, 0, 8)
	for i := range 8 {
		grants = append(grants, channelTestJoinGrant(t, "acme", fmt.Sprintf("u%d", i)))
	}

	var mu sync.Mutex
	seen := make(map[joaju.Channel]int)

	var wg sync.WaitGroup
	for _, g := range grants {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for range 50 {
				held, err := b.FindOrCreate(context.Background(), g, name)
				if err != nil {
					t.Errorf("finding or creating %s: %v", name.Requested(), err)

					return
				}
				mu.Lock()
				seen[held]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != 1 {
		t.Fatalf("%d channels were handed out under %q: a publish reaches the subscribers of one of them and the rest hear nothing", len(seen), name.Requested())
	}
}

func TestMemoryBrokerIsSafeForConcurrentUse(t *testing.T) {
	b := NewMemoryBroker()

	// The sockets, the Grants and the names are minted here rather than inside
	// the goroutines, for the reason above. Two tenants, because the tenant
	// filter is what this has to keep right while the map is being written.
	type actor struct {
		tenant string
		grant  auth.Grant
		conn   *joaju.Connection
		shared joaju.ChannelName
		own    joaju.ChannelName
	}
	actors := make([]actor, 0, 8)
	for i := range 8 {
		tenant := "acme"
		if i%2 == 1 {
			tenant = "globex"
		}
		user := fmt.Sprintf("u%d", i)
		conn, _ := channelTestConnection(t, tenant, user)
		actors = append(actors, actor{
			tenant: tenant,
			grant:  channelTestJoinGrant(t, tenant, user),
			conn:   conn,
			shared: channelTestName(t, tenant, "orders.17"),
			own:    channelTestName(t, tenant, "orders."+user),
		})
	}

	var wg sync.WaitGroup
	for _, a := range actors {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ctx := context.Background()
			for range 25 {
				shared, err := b.FindOrCreate(ctx, a.grant, a.shared)
				if err != nil {
					t.Errorf("finding or creating %s of %s: %v", a.shared.Requested(), a.tenant, err)

					return
				}
				own, err := b.FindOrCreate(ctx, a.grant, a.own)
				if err != nil {
					t.Errorf("finding or creating %s of %s: %v", a.own.Requested(), a.tenant, err)

					return
				}
				if err := shared.Subscribe(ctx, a.grant, a.conn, joaju.Member{}); err != nil {
					t.Errorf("subscribing to %s of %s: %v", a.shared.Requested(), a.tenant, err)

					return
				}
				if err := own.Subscribe(ctx, a.grant, a.conn, joaju.Member{}); err != nil {
					t.Errorf("subscribing to %s of %s: %v", a.own.Requested(), a.tenant, err)

					return
				}

				// Nobody else ever names this one, so it is the lookup that can
				// be asserted while the map is being written by everyone else.
				found, err := b.Find(ctx, a.grant, a.own)
				if err != nil {
					t.Errorf("finding %s of %s: %v", a.own.Requested(), a.tenant, err)

					return
				}
				if found != own {
					t.Errorf("%s of %s was handed out twice", a.own.Requested(), a.tenant)

					return
				}

				all, err := b.All(ctx, a.grant)
				if err != nil {
					t.Errorf("listing the channels of %s: %v", a.tenant, err)

					return
				}
				for _, held := range all {
					if held.Name().Tenant() != a.tenant {
						t.Errorf("a client of %s was answered %q of %s", a.tenant, held.Name().Requested(), held.Name().Tenant())

						return
					}
				}

				if err := shared.Unsubscribe(ctx, a.conn); err != nil {
					t.Errorf("unsubscribing from %s of %s: %v", a.shared.Requested(), a.tenant, err)

					return
				}
				if err := own.Unsubscribe(ctx, a.conn); err != nil {
					t.Errorf("unsubscribing from %s of %s: %v", a.own.Requested(), a.tenant, err)

					return
				}
				if err := b.Remove(ctx, a.grant, a.shared); err != nil {
					t.Errorf("removing %s of %s: %v", a.shared.Requested(), a.tenant, err)

					return
				}
				if err := b.Remove(ctx, a.grant, a.own); err != nil {
					t.Errorf("removing %s of %s: %v", a.own.Requested(), a.tenant, err)

					return
				}
			}
		}()
	}
	wg.Wait()

	// Every subscriber left and every channel was removed after it, so nothing
	// is held: a channel left behind here is one that outlives its subscribers,
	// and the process fills up with them.
	for _, tenant := range []string{"acme", "globex"} {
		all, err := b.All(context.Background(), channelTestJoinGrant(t, tenant, "u1"))
		if err != nil {
			t.Fatalf("listing the channels of %s: %v", tenant, err)
		}
		if len(all) != 0 {
			t.Fatalf("%d channel(s) of %s were left behind: %v", len(all), tenant, brokerTestRequested(all))
		}
	}
}
