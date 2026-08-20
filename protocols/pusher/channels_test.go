package pusher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
	"github.com/arandu-io/joaju"
)

// Everything in this file is named for the channel, because the package is one
// package and another file's helpers share the namespace.

// channelTestSink is a [joaju.Sink] that keeps what was written to it.
type channelTestSink struct {
	mu         sync.Mutex
	messages   [][]byte
	terminated bool
	err        error
}

func (s *channelTestSink) Send(_ context.Context, message []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err != nil {
		return s.err
	}
	s.messages = append(s.messages, append([]byte(nil), message...))

	return nil
}

func (s *channelTestSink) Terminate(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.terminated = true

	return nil
}

// channelTestFrame is one decoded frame, as a client would read it.
type channelTestFrame struct {
	Event   string `json:"event"`
	Channel string `json:"channel"`
	Data    string `json:"data,omitempty"`
}

// frames decodes everything written to the sink.
func (s *channelTestSink) frames(t *testing.T) []channelTestFrame {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	frames := make([]channelTestFrame, 0, len(s.messages))
	for _, message := range s.messages {
		var frame channelTestFrame
		if err := json.Unmarshal(message, &frame); err != nil {
			t.Fatalf("the channel wrote something that is not a frame: %v (%s)", err, message)
		}
		frames = append(frames, frame)
	}

	return frames
}

// channelTestConnectPolicy allows every handshake, so that a test can hold a
// connection. Whether a socket may open is not what this file is about.
type channelTestConnectPolicy struct{}

func (channelTestConnectPolicy) Can(_ context.Context, _ auth.Subject, _ auth.Action, _ joaju.Handshake) error {
	return nil
}

// channelTestJoinPolicy allows every subscription, which is the case worth
// testing: the tenant checks in [channel.Subscribe] must refuse subscriptions a
// policy said yes to, because that is the only refusal a policy cannot make.
type channelTestJoinPolicy struct{}

func (channelTestJoinPolicy) Can(_ context.Context, _ auth.Subject, _ auth.Action, _ joaju.Subscription) error {
	return nil
}

// channelTestJoinGrant is the Grant a [joaju.SubscriptionPolicy] issues.
func channelTestJoinGrant(t *testing.T, tenant, user string) auth.Grant {
	t.Helper()

	g, err := auth.Authorize(context.Background(), channelTestJoinPolicy{},
		auth.Subject{ID: user, Tenant: tenant}, broadcasting.ChannelJoin, joaju.Subscription{})
	if err != nil {
		t.Fatalf("authorizing %s of %s to join: %v", user, tenant, err)
	}

	return g
}

// channelTestConnection is a connection held by user of tenant.
func channelTestConnection(t *testing.T, tenant, user string) (*joaju.Connection, *channelTestSink) {
	t.Helper()

	id := joaju.SocketID(fmt.Sprintf("%s.%s", tenant, user))
	g, err := auth.Authorize(context.Background(), channelTestConnectPolicy{},
		auth.Subject{ID: user, Tenant: tenant}, joaju.Connect, joaju.Handshake{Socket: id})
	if err != nil {
		t.Fatalf("authorizing the handshake of %s of %s: %v", user, tenant, err)
	}

	sink := &channelTestSink{}
	conn, err := joaju.NewConnection(g, id, sink)
	if err != nil {
		t.Fatalf("opening the socket of %s of %s: %v", user, tenant, err)
	}

	return conn, sink
}

// channelTestName is the name of a channel in tenant, as asked for by a client
// of tenant.
func channelTestName(t *testing.T, tenant, requested string) joaju.ChannelName {
	t.Helper()

	name, err := joaju.NewChannelName(channelTestJoinGrant(t, tenant, "namer"), requested)
	if err != nil {
		t.Fatalf("naming %s in %s: %v", requested, tenant, err)
	}

	return name
}

// channelTestChannel is a channel of the kind requested names, in tenant.
func channelTestChannel(t *testing.T, tenant, requested string) joaju.Channel {
	t.Helper()

	c, err := NewChannel(channelTestName(t, tenant, requested))
	if err != nil {
		t.Fatalf("creating %s in %s: %v", requested, tenant, err)
	}

	return c
}

// channelTestSubscribe seats a connection and fails the test if it is refused.
func channelTestSubscribe(t *testing.T, c joaju.Channel, tenant, user string, member joaju.Member) (*joaju.Connection, *channelTestSink) {
	t.Helper()

	conn, sink := channelTestConnection(t, tenant, user)
	if err := c.Subscribe(context.Background(), channelTestJoinGrant(t, tenant, user), conn, member); err != nil {
		t.Fatalf("subscribing %s of %s to %s: %v", user, tenant, c.Name().Requested(), err)
	}

	return conn, sink
}

func TestNewChannelRefusesTheZeroName(t *testing.T) {
	if _, err := NewChannel(joaju.ChannelName{}); err == nil {
		t.Fatal("a channel was created from the zero name, which carries no tenant: every subscriber of it would be in the same channel as every other customer's")
	}
}

// This is the refusal the repository exists for: a client that names another
// customer's channel.
//
// It is refused twice over, and both are proved here. A client cannot spell a
// tenant -- the name it sends never reaches the tenant half of a
// [joaju.ChannelName] -- and a name built under one Grant cannot be subscribed to
// with another.
func TestChannelRefusesAClientThatNamesAnotherTenant(t *testing.T) {
	g := channelTestJoinGrant(t, "globex", "u1")

	if _, err := joaju.NewChannelName(g, "acme:private-orders.17"); err == nil {
		t.Fatal("a client of globex named the channel acme:private-orders.17 and it was accepted: the client chose whose events it hears, and every order event of acme goes to it")
	} else if !errors.Is(err, broadcasting.ErrTenantInChannelName) {
		t.Fatalf("refused for the wrong reason: %v", err)
	}

	// The same client asking for the same channel without spelling the tenant
	// gets its own, in its own tenant, which is a different channel.
	mine, err := joaju.NewChannelName(g, "private-orders.17")
	if err != nil {
		t.Fatalf("naming private-orders.17 in globex: %v", err)
	}
	theirs := channelTestName(t, "acme", "private-orders.17")
	if mine.String() == theirs.String() {
		t.Fatalf("two tenants asking for %q landed on the same channel %q: one customer's orders are published to the other", "private-orders.17", mine.String())
	}
}

// The encrypted prefix, which the server has no key for and still has to read.
//
// The encryption is the subscribers' business, but the two properties the
// prefix is wrapped around are this server's: a name it does not recognize is
// authorized by accident -- "private-encrypted-" begins with "private-" -- and
// replayed to nobody, because "private-encrypted-cache-" does not begin with
// "private-cache-".
func TestChannelNameTypeReadsTheEncryptedPrefix(t *testing.T) {
	for _, one := range []struct {
		requested string
		want      joaju.ChannelType
	}{
		{broadcasting.EncryptedPrivateChannelPrefix + "orders.17", joaju.PrivateChannel},
		{joaju.EncryptedPrivateCacheChannelPrefix + "quotes", joaju.PrivateCacheChannel},
		// The prefixes it must not swallow.
		{joaju.PrivateCacheChannelPrefix + "quotes", joaju.PrivateCacheChannel},
		{joaju.PresenceCacheChannelPrefix + "room", joaju.PresenceCacheChannel},
		{broadcasting.PrivateChannelPrefix + "orders.17", joaju.PrivateChannel},
		{joaju.CacheChannelPrefix + "quotes", joaju.CacheChannel},
	} {
		name := channelTestName(t, "acme", one.requested)
		if got := name.Type(); got != one.want {
			t.Fatalf("%q is a %s channel, want %s", one.requested, got, one.want)
		}
	}

	// The two the encrypted prefix would have lost, said as the properties the
	// rest of the package reads rather than as the kind.
	encrypted := channelTestName(t, "acme", joaju.EncryptedPrivateCacheChannelPrefix+"quotes")
	if !encrypted.Type().Guarded() {
		t.Fatalf("%q is unguarded, so a policy may allow it freely", encrypted.Requested())
	}
	if !encrypted.Type().Cache() {
		t.Fatalf("%q does not replay, so a client that subscribes after the event waits for the next one", encrypted.Requested())
	}
}

// The protocol's ceiling on a name, which is what stands between one authorized
// socket and a channel the size of the message limit.
func TestNewChannelNameRefusesANameLongerThanTheProtocolCarries(t *testing.T) {
	g := channelTestJoinGrant(t, "acme", "u1")

	longest := strings.Repeat("a", joaju.MaxChannelNameLength)
	if _, err := joaju.NewChannelName(g, longest); err != nil {
		t.Fatalf("naming a channel of %d characters, which is the most the protocol carries: %v", joaju.MaxChannelNameLength, err)
	}

	// One character past it, and every length up to what a frame can hold.
	for _, n := range []int{joaju.MaxChannelNameLength + 1, 1024, 10 << 10} {
		_, err := joaju.NewChannelName(g, strings.Repeat("a", n))
		if err == nil {
			t.Fatalf("a channel name of %d characters was accepted, and %d is the most the protocol carries: the only ceiling left is the message limit, and the name is held for as long as the subscription is", n, joaju.MaxChannelNameLength)
		}
		if !errors.Is(err, joaju.ErrChannelName) {
			t.Fatalf("a name of %d characters was refused for the wrong reason: %v", n, err)
		}
	}
}

// The set a name is spelled from, which is what keeps a channel name from
// meaning something to whatever it is written into.
func TestNewChannelNameRefusesACharacterTheProtocolHasNoRoomFor(t *testing.T) {
	g := channelTestJoinGrant(t, "acme", "u1")

	spelled := "private-cache-Orders_17=v2@acme,eu.west;1-2"
	if _, err := joaju.NewChannelName(g, spelled); err != nil {
		t.Fatalf("naming %q, which is letters, digits and %q: %v", spelled, joaju.ChannelNameCharacters, err)
	}

	for _, requested := range []string{
		"private-#internal",  // reserved by the protocol for itself
		"orders 17",          // a space, which a metrics label splits on
		"orders\n17",         // a newline, which a log line ends on
		"orders/17",          // a separator, which a URL path cuts on
		"orders*",            // a glob, which a subscriber pattern reads
		"private-\x00orders", // a terminator
		"private-étage",      // a letter outside the set
		"private-orders​",    // a space that does not look like one
	} {
		_, err := joaju.NewChannelName(g, requested)
		if err == nil {
			t.Fatalf("the channel name %q was accepted, and it is not spelled from the letters, the digits and %q", requested, joaju.ChannelNameCharacters)
		}
		if !errors.Is(err, joaju.ErrChannelName) {
			t.Fatalf("%q was refused for the wrong reason: %v", requested, err)
		}
	}
}

func TestChannelSubscribeRefusesAGrantFromAnotherTenant(t *testing.T) {
	c := channelTestChannel(t, "acme", "private-orders.17")
	conn, sink := channelTestConnection(t, "globex", "u1")

	err := c.Subscribe(context.Background(), channelTestJoinGrant(t, "globex", "u1"), conn, joaju.Member{})
	if err == nil {
		t.Fatalf("a subscriber of globex was seated on %q, which belongs to acme: every event acme publishes on that channel is now delivered to another customer", c.Name().String())
	}
	if !errors.Is(err, joaju.ErrWrongTenant) {
		t.Fatalf("refused, but not as a tenant mismatch: %v", err)
	}
	if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("the refusal does not read as forbidden, so a handler answers 500 instead of 403: %v", err)
	}
	if c.Subscribed(conn) {
		t.Fatal("the subscription was refused and the socket is on the channel anyway")
	}

	if err := c.BroadcastToAll(context.Background(), joaju.Event{
		Name:    "order.paid",
		Channel: c.Name(),
		Data:    json.RawMessage(`{"total":100}`),
	}); err != nil {
		t.Fatalf("broadcasting on %s: %v", c.Name().Requested(), err)
	}
	if got := sink.frames(t); len(got) != 0 {
		t.Fatalf("a socket of globex received %d frame(s) published on an acme channel: %v", len(got), got)
	}
}

// The Grant may be the channel's tenant and the socket somebody else's. This is
// the line that stops one customer's Grant from seating another customer's
// socket.
func TestChannelSubscribeRefusesAConnectionFromAnotherTenant(t *testing.T) {
	c := channelTestChannel(t, "acme", "private-orders.17")
	conn, _ := channelTestConnection(t, "globex", "u1")

	err := c.Subscribe(context.Background(), channelTestJoinGrant(t, "acme", "u1"), conn, joaju.Member{})
	if err == nil {
		t.Fatalf("a socket belonging to globex was seated on %q with a grant of acme: the grant was right and the listener was another customer", c.Name().String())
	}
	if !errors.Is(err, joaju.ErrWrongTenant) {
		t.Fatalf("refused, but not as a tenant mismatch: %v", err)
	}
}

func TestChannelSubscribeRefusesAGrantIssuedForAnotherAction(t *testing.T) {
	c := channelTestChannel(t, "acme", "private-orders.17")
	conn, _ := channelTestConnection(t, "acme", "u1")

	// The Grant the socket itself holds: same subject, same tenant, issued for
	// Connect. Reusing it would mean no subscription policy ever ran.
	err := c.Subscribe(context.Background(), conn.Grant(), conn, joaju.Member{})
	if err == nil {
		t.Fatal("the grant that opened the socket also subscribed it: no subscription policy was consulted, and every channel of the tenant is reachable by anyone who may connect")
	}
	if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("refused, but not as an authorization failure: %v", err)
	}
}

func TestChannelSubscribeRefusesTheZeroGrant(t *testing.T) {
	c := channelTestChannel(t, "acme", "private-orders.17")
	conn, _ := channelTestConnection(t, "acme", "u1")

	if err := c.Subscribe(context.Background(), auth.Grant{}, conn, joaju.Member{}); err == nil {
		t.Fatal("a subscription with no grant at all was accepted")
	}
}

func TestChannelSubscribeRefusesAGrantForAnotherSubject(t *testing.T) {
	c := channelTestChannel(t, "acme", "private-orders.17")
	conn, _ := channelTestConnection(t, "acme", "u1")

	err := c.Subscribe(context.Background(), channelTestJoinGrant(t, "acme", "u2"), conn, joaju.Member{})
	if err == nil {
		t.Fatal("the policy answered about u2 and u1's socket was seated: the decision was made about somebody else")
	}
	if !errors.Is(err, auth.ErrForbidden) {
		t.Fatalf("refused, but not as an authorization failure: %v", err)
	}
}

// A public channel is public inside one tenant and never wider, which is what
// [joaju.PublicChannel] promises. A channel with no prefix takes the same two tenant
// checks as a private one.
func TestPublicChannelIsStillOneTenants(t *testing.T) {
	c := channelTestChannel(t, "acme", "status")
	if got := c.Name().Type(); got != joaju.PublicChannel {
		t.Fatalf("status is a %s channel, expected public", got)
	}

	conn, _ := channelTestConnection(t, "globex", "u1")
	err := c.Subscribe(context.Background(), channelTestJoinGrant(t, "globex", "u1"), conn, joaju.Member{})
	if !errors.Is(err, joaju.ErrWrongTenant) {
		t.Fatalf("a socket of globex reached acme's public channel: %v", err)
	}
}

func TestChannelBroadcastSkipsTheSender(t *testing.T) {
	c := channelTestChannel(t, "acme", "orders.17")
	sender, senderSink := channelTestSubscribe(t, c, "acme", "u1", joaju.Member{})
	_, otherSink := channelTestSubscribe(t, c, "acme", "u2", joaju.Member{})

	e := joaju.Event{
		Name:    "order.paid",
		Channel: c.Name(),
		Data:    json.RawMessage(`{"total":100}`),
		Socket:  sender.ID(),
	}
	if err := c.Broadcast(context.Background(), e); err != nil {
		t.Fatalf("broadcasting: %v", err)
	}

	if got := senderSink.frames(t); len(got) != 0 {
		t.Fatalf("the sender received its own event back: %v", got)
	}
	got := otherSink.frames(t)
	if len(got) != 1 {
		t.Fatalf("the other subscriber received %d frames, expected 1: %v", len(got), got)
	}
	if got[0].Event != "order.paid" {
		t.Fatalf("the frame carries %q, expected order.paid", got[0].Event)
	}
	if got[0].Data != `{"total":100}` {
		t.Fatalf("the data field is %q, expected the payload as a JSON string", got[0].Data)
	}
}

func TestChannelBroadcastToAllReachesTheSender(t *testing.T) {
	c := channelTestChannel(t, "acme", "orders.17")
	sender, senderSink := channelTestSubscribe(t, c, "acme", "u1", joaju.Member{})

	err := c.BroadcastToAll(context.Background(), joaju.Event{
		Name:    "order.paid",
		Channel: c.Name(),
		Data:    json.RawMessage(`{"total":100}`),
		Socket:  sender.ID(),
	})
	if err != nil {
		t.Fatalf("broadcasting to all: %v", err)
	}

	if got := senderSink.frames(t); len(got) != 1 {
		t.Fatalf("the sender received %d frames, expected 1: %v", len(got), got)
	}
}

func TestChannelBroadcastRefusesAnEventOfAnotherTenant(t *testing.T) {
	c := channelTestChannel(t, "acme", "orders.17")
	_, sink := channelTestSubscribe(t, c, "acme", "u1", joaju.Member{})

	err := c.Broadcast(context.Background(), joaju.Event{
		Name:    "order.paid",
		Channel: channelTestName(t, "globex", "orders.17"),
		Data:    json.RawMessage(`{"total":100}`),
	})
	if err == nil {
		t.Fatal("an event published by globex was delivered on acme's channel of the same name: the two customers share a channel as soon as the tenant is dropped from the lookup")
	}
	if !errors.Is(err, joaju.ErrWrongTenant) {
		t.Fatalf("refused, but not as a tenant mismatch: %v", err)
	}
	if got := sink.frames(t); len(got) != 0 {
		t.Fatalf("the subscriber received another tenant's event anyway: %v", got)
	}
}

func TestChannelBroadcastRefusesAnEventOfAnotherChannel(t *testing.T) {
	c := channelTestChannel(t, "acme", "orders.17")

	err := c.Broadcast(context.Background(), joaju.Event{
		Name:    "order.paid",
		Channel: channelTestName(t, "acme", "orders.18"),
	})
	if err == nil {
		t.Fatal("an event for orders.18 was delivered on orders.17")
	}
}

// The tenant is in the key the channel is held under and never in the bytes.
func TestChannelFrameNeverCarriesTheTenant(t *testing.T) {
	c := channelTestChannel(t, "acme", "private-orders.17")
	_, sink := channelTestSubscribe(t, c, "acme", "u1", joaju.Member{})

	if err := c.BroadcastToAll(context.Background(), joaju.Event{
		Name:    "order.paid",
		Channel: c.Name(),
		Data:    json.RawMessage(`{"total":100}`),
	}); err != nil {
		t.Fatalf("broadcasting: %v", err)
	}

	got := sink.frames(t)
	if len(got) != 1 {
		t.Fatalf("expected one frame, got %d: %v", len(got), got)
	}
	if got[0].Channel != "private-orders.17" {
		t.Fatalf("the frame names the channel %q, and the client asked for private-orders.17", got[0].Channel)
	}
	if strings.Contains(got[0].Channel, broadcasting.TenantSeparator) {
		t.Fatalf("the tenant went out on the wire in %q: a client that reads it learns whose namespace it is in, and a client that echoes it back has named one", got[0].Channel)
	}
}

func TestPresenceChannelAnnouncesArrivalAndDeparture(t *testing.T) {
	c := channelTestChannel(t, "acme", "presence-orders.17")
	_, first := channelTestSubscribe(t, c, "acme", "u1", joaju.Member{UserID: "u1", Info: json.RawMessage(`{"name":"Ana"}`)})
	second, secondSink := channelTestSubscribe(t, c, "acme", "u2", joaju.Member{UserID: "u2"})

	got := first.frames(t)
	if len(got) != 1 {
		t.Fatalf("the first member received %d frames, expected the arrival of the second: %v", len(got), got)
	}
	if got[0].Event != joaju.EventMemberAdded {
		t.Fatalf("the frame is %q, expected %q", got[0].Event, joaju.EventMemberAdded)
	}
	if !strings.Contains(got[0].Data, `"user_id":"u2"`) {
		t.Fatalf("the arrival does not name the member: %q", got[0].Data)
	}
	if len(secondSink.frames(t)) != 0 {
		t.Fatalf("the arriving member was told about its own arrival: %v", secondSink.frames(t))
	}

	if err := c.Unsubscribe(context.Background(), second); err != nil {
		t.Fatalf("unsubscribing: %v", err)
	}
	got = first.frames(t)
	if len(got) != 2 {
		t.Fatalf("the first member received %d frames, expected the departure of the second too: %v", len(got), got)
	}
	if got[1].Event != joaju.EventMemberRemoved {
		t.Fatalf("the second frame is %q, expected %q", got[1].Event, joaju.EventMemberRemoved)
	}
}

func TestPresenceChannelRefusesASubscriptionWithNoMember(t *testing.T) {
	c := channelTestChannel(t, "acme", "presence-orders.17")
	conn, _ := channelTestConnection(t, "acme", "u1")

	if err := c.Subscribe(context.Background(), channelTestJoinGrant(t, "acme", "u1"), conn, joaju.Member{}); err == nil {
		t.Fatal("a presence channel seated a member with no id: the member list has a hole in it and nobody can be told who left")
	}
}

// A presence channel's member list is one tenant's, because the channel is.
// Two customers may ask for the same name and neither one appears in the
// other's list.
func TestPresenceMembersDoNotCrossTenants(t *testing.T) {
	acme := channelTestChannel(t, "acme", "presence-orders.17")
	globex := channelTestChannel(t, "globex", "presence-orders.17")

	channelTestSubscribe(t, acme, "acme", "ana", joaju.Member{UserID: "ana"})
	channelTestSubscribe(t, globex, "globex", "bruno", joaju.Member{UserID: "bruno"})

	for _, tt := range []struct {
		tenant  string
		channel joaju.Channel
		member  string
		other   string
	}{
		{"acme", acme, "ana", "bruno"},
		{"globex", globex, "bruno", "ana"},
	} {
		ids := channelTestPresenceIDs(t, tt.channel)
		if len(ids) != 1 || ids[0] != tt.member {
			t.Fatalf("%s's member list is %v, expected only %q", tt.tenant, ids, tt.member)
		}
		if channelTestContains(ids, tt.other) {
			t.Fatalf("%q appears in the member list of %s's %q: a subject id of one customer is disclosed to another", tt.other, tt.tenant, tt.channel.Name().Requested())
		}
		if len(tt.channel.Connections()) != 1 {
			t.Fatalf("%s's channel holds %d subscribers, expected 1", tt.tenant, len(tt.channel.Connections()))
		}
	}
}

// One person with two tabs is one member: they arrive once, and they have not
// left while a tab is still open.
func TestPresenceChannelCountsPeopleAndNotSockets(t *testing.T) {
	c := channelTestChannel(t, "acme", "presence-orders.17")
	_, watcher := channelTestSubscribe(t, c, "acme", "ana", joaju.Member{UserID: "ana"})

	// A second socket for the same person, which the helper cannot mint because
	// it derives the socket id from the user.
	g, err := auth.Authorize(context.Background(), channelTestConnectPolicy{},
		auth.Subject{ID: "bruno", Tenant: "acme"}, joaju.Connect, joaju.Handshake{Socket: "acme.bruno.2"})
	if err != nil {
		t.Fatalf("authorizing the second handshake: %v", err)
	}
	secondTab, err := joaju.NewConnection(g, "acme.bruno.2", &channelTestSink{})
	if err != nil {
		t.Fatalf("opening the second socket: %v", err)
	}

	firstTab, _ := channelTestSubscribe(t, c, "acme", "bruno", joaju.Member{UserID: "bruno"})
	if err := c.Subscribe(context.Background(), channelTestJoinGrant(t, "acme", "bruno"), secondTab, joaju.Member{UserID: "bruno"}); err != nil {
		t.Fatalf("subscribing bruno's second tab: %v", err)
	}

	if ids := channelTestPresenceIDs(t, c); len(ids) != 2 {
		t.Fatalf("the member list is %v, expected two people", ids)
	}
	arrivals := 0
	for _, frame := range watcher.frames(t) {
		if frame.Event == joaju.EventMemberAdded {
			arrivals++
		}
	}
	if arrivals != 1 {
		t.Fatalf("bruno arrived %d times with two tabs open, expected once", arrivals)
	}

	if err := c.Unsubscribe(context.Background(), firstTab); err != nil {
		t.Fatalf("unsubscribing the first tab: %v", err)
	}
	for _, frame := range watcher.frames(t) {
		if frame.Event == joaju.EventMemberRemoved {
			t.Fatal("bruno was announced as gone while a second tab of his is still on the channel")
		}
	}
	if err := c.Unsubscribe(context.Background(), secondTab); err != nil {
		t.Fatalf("unsubscribing the second tab: %v", err)
	}
	departures := 0
	for _, frame := range watcher.frames(t) {
		if frame.Event == joaju.EventMemberRemoved {
			departures++
		}
	}
	if departures != 1 {
		t.Fatalf("bruno left %d times, expected once", departures)
	}
}

func TestChannelDataIsEmptyOffAPresenceChannel(t *testing.T) {
	for _, requested := range []string{"orders.17", "private-orders.17", "cache-orders.17", "private-cache-orders.17"} {
		c := channelTestChannel(t, "acme", requested)
		if got := c.Data(); len(got) != 0 {
			t.Fatalf("%s published %v about its subscribers, and only a presence channel publishes anything", requested, got)
		}
	}
}

func TestPresenceChannelDataCarriesTheMemberInfo(t *testing.T) {
	c := channelTestChannel(t, "acme", "presence-orders.17")
	channelTestSubscribe(t, c, "acme", "ana", joaju.Member{UserID: "ana", Info: json.RawMessage(`{"name":"Ana"}`)})
	channelTestSubscribe(t, c, "acme", "bruno", joaju.Member{UserID: "bruno"})

	presence, ok := c.Data()["presence"].(map[string]any)
	if !ok {
		t.Fatalf("the data of a presence channel is %v, expected a presence block", c.Data())
	}
	if presence["count"] != 2 {
		t.Fatalf("count is %v, expected 2", presence["count"])
	}
	hash, ok := presence["hash"].(map[string]json.RawMessage)
	if !ok {
		t.Fatalf("hash is %T, expected a map of member info", presence["hash"])
	}
	if string(hash["ana"]) != `{"name":"Ana"}` {
		t.Fatalf("ana's info is %s", hash["ana"])
	}
	if string(hash["bruno"]) != "{}" {
		t.Fatalf("a member with no info is %s, expected an empty object", hash["bruno"])
	}
}

func TestCacheChannelReplaysItsLastEvent(t *testing.T) {
	c := channelTestChannel(t, "acme", "cache-orders.17")
	if !c.Name().Type().Cache() {
		t.Fatalf("cache-orders.17 is a %s channel", c.Name().Type())
	}

	// The first subscriber arrives before anything has been broadcast, so it is
	// told there is nothing to replay and that frame is discounted below.
	_, earlySink := channelTestSubscribe(t, c, "acme", "u1", joaju.Member{})
	if got := earlySink.frames(t); len(got) != 1 || got[0].Event != EventCacheMiss {
		t.Fatalf("the first subscriber of an empty cache channel received %v, expected a cache miss", got)
	}
	for _, total := range []string{`{"total":100}`, `{"total":200}`} {
		if err := c.BroadcastToAll(context.Background(), joaju.Event{
			Name:    "order.paid",
			Channel: c.Name(),
			Data:    json.RawMessage(total),
		}); err != nil {
			t.Fatalf("broadcasting: %v", err)
		}
	}
	if len(earlySink.frames(t)) != 3 {
		t.Fatalf("the subscriber that was there received %d frames, expected the cache miss and both events", len(earlySink.frames(t)))
	}

	_, lateSink := channelTestSubscribe(t, c, "acme", "u2", joaju.Member{})
	got := lateSink.frames(t)
	if len(got) != 1 {
		t.Fatalf("the late subscriber received %d frames, expected the last event replayed: %v", len(got), got)
	}
	if got[0].Data != `{"total":200}` {
		t.Fatalf("the replay carries %q, expected the last event and not an earlier one", got[0].Data)
	}
	if len(earlySink.frames(t)) != 3 {
		t.Fatal("the replay went to everyone: a subscriber that had already seen the event saw it twice")
	}
}

func TestCacheChannelMissesBeforeTheFirstEvent(t *testing.T) {
	c := channelTestChannel(t, "acme", "cache-orders.17")
	_, sink := channelTestSubscribe(t, c, "acme", "u1", joaju.Member{})

	got := sink.frames(t)
	if len(got) != 1 {
		t.Fatalf("a channel that has carried nothing sent %v", got)
	}
	if got[0].Event != EventCacheMiss {
		t.Fatalf("the frame is %q, expected %q so the client stops waiting for a replay that is not coming", got[0].Event, EventCacheMiss)
	}
	if got[0].Data != "" {
		t.Fatalf("the cache miss carries %q, and there is nothing to carry", got[0].Data)
	}
}

// A channel that is not a cache channel says nothing about a cache.
func TestPlainChannelSendsNoCacheMiss(t *testing.T) {
	c := channelTestChannel(t, "acme", "private-orders.17")
	_, sink := channelTestSubscribe(t, c, "acme", "u1", joaju.Member{})

	if got := sink.frames(t); len(got) != 0 {
		t.Fatalf("subscribing to a channel that caches nothing produced %v", got)
	}
}

func TestCacheChannelDoesNotCacheProtocolEvents(t *testing.T) {
	c := channelTestChannel(t, "acme", "presence-cache-orders.17")
	if !c.Name().Type().Cache() || !c.Name().Type().Presence() {
		t.Fatalf("presence-cache-orders.17 is a %s channel", c.Name().Type())
	}

	channelTestSubscribe(t, c, "acme", "ana", joaju.Member{UserID: "ana"})
	// The arrival of the second member is a pusher_internal: frame on the
	// channel. Replaying it to a third would announce an arrival that happened
	// before they were listening.
	channelTestSubscribe(t, c, "acme", "bruno", joaju.Member{UserID: "bruno"})

	_, thirdSink := channelTestSubscribe(t, c, "acme", "carla", joaju.Member{UserID: "carla"})
	got := thirdSink.frames(t)
	if len(got) != 1 {
		t.Fatalf("the third member received %v, expected the cache miss alone", got)
	}
	if got[0].Event != EventCacheMiss {
		t.Fatalf("the channel replayed %q to a new subscriber: it is protocol traffic, not the state the channel is caching", got[0].Event)
	}
}

func TestNonPresenceChannelDiscardsMemberData(t *testing.T) {
	c := channelTestChannel(t, "acme", "private-orders.17")
	conn, _ := channelTestSubscribe(t, c, "acme", "u1", joaju.Member{UserID: "u1", Info: json.RawMessage(`{"name":"Ana"}`)})

	s, ok := c.Find(conn.ID())
	if !ok {
		t.Fatal("the subscriber is not on the channel it subscribed to")
	}
	if s.Member.UserID != "" || len(s.Member.Info) != 0 {
		t.Fatalf("a channel that publishes nothing about its subscribers kept %v", s.Member)
	}
}

func TestChannelUnsubscribeIsIdempotent(t *testing.T) {
	c := channelTestChannel(t, "acme", "orders.17")
	conn, _ := channelTestSubscribe(t, c, "acme", "u1", joaju.Member{})

	if err := c.Unsubscribe(context.Background(), conn); err != nil {
		t.Fatalf("unsubscribing: %v", err)
	}
	if c.Subscribed(conn) {
		t.Fatal("the socket is still on the channel it left")
	}
	if err := c.Unsubscribe(context.Background(), conn); err != nil {
		t.Fatalf("unsubscribing a socket that had already left: %v", err)
	}
	if len(c.Connections()) != 0 {
		t.Fatalf("the channel holds %d subscribers after the only one left", len(c.Connections()))
	}
}

func TestChannelSeatsASocketOnce(t *testing.T) {
	c := channelTestChannel(t, "acme", "presence-orders.17")
	conn, _ := channelTestSubscribe(t, c, "acme", "ana", joaju.Member{UserID: "ana"})
	_, watcher := channelTestSubscribe(t, c, "acme", "bruno", joaju.Member{UserID: "bruno"})

	if err := c.Subscribe(context.Background(), channelTestJoinGrant(t, "acme", "ana"), conn, joaju.Member{UserID: "ana"}); err != nil {
		t.Fatalf("resubscribing: %v", err)
	}
	if len(c.Connections()) != 2 {
		t.Fatalf("the channel holds %d subscribers after one of them subscribed twice, expected 2", len(c.Connections()))
	}
	for _, frame := range watcher.frames(t) {
		if frame.Event == joaju.EventMemberAdded && strings.Contains(frame.Data, `"user_id":"ana"`) {
			t.Fatal("a socket that resubscribed announced its arrival to people who never saw it leave")
		}
	}
}

func TestChannelDeliveryFailureDoesNotStopTheOthers(t *testing.T) {
	c := channelTestChannel(t, "acme", "orders.17")
	_, broken := channelTestSubscribe(t, c, "acme", "u1", joaju.Member{})
	_, healthy := channelTestSubscribe(t, c, "acme", "u2", joaju.Member{})

	broken.mu.Lock()
	broken.err = errors.New("the socket is closed")
	broken.mu.Unlock()

	err := c.BroadcastToAll(context.Background(), joaju.Event{Name: "order.paid", Channel: c.Name()})
	if err == nil {
		t.Fatal("a socket refused the write and the broadcast reported success")
	}
	if len(healthy.frames(t)) != 1 {
		t.Fatal("one dead socket stopped the delivery to the others")
	}
}

func TestChannelIsSafeForConcurrentUse(t *testing.T) {
	c := channelTestChannel(t, "acme", "presence-orders.17")

	// The sockets and the Grants are minted here rather than inside the
	// goroutines, because the helpers fail the test and only the test goroutine
	// may do that.
	type actor struct {
		user  string
		conn  *joaju.Connection
		grant auth.Grant
	}
	actors := make([]actor, 0, 8)
	for i := range 8 {
		user := fmt.Sprintf("u%d", i)
		conn, _ := channelTestConnection(t, "acme", user)
		actors = append(actors, actor{user: user, conn: conn, grant: channelTestJoinGrant(t, "acme", user)})
	}

	var wg sync.WaitGroup
	for _, a := range actors {
		user, conn, g := a.user, a.conn, a.grant
		wg.Add(1)
		go func() {
			defer wg.Done()

			for range 20 {
				if err := c.Subscribe(context.Background(), g, conn, joaju.Member{UserID: user}); err != nil {
					t.Errorf("subscribing %s: %v", user, err)

					return
				}
				_ = c.Data()
				_ = c.Connections()
				if err := c.BroadcastToAll(context.Background(), joaju.Event{Name: "order.paid", Channel: c.Name()}); err != nil {
					t.Errorf("broadcasting: %v", err)

					return
				}
				if err := c.Unsubscribe(context.Background(), conn); err != nil {
					t.Errorf("unsubscribing %s: %v", user, err)

					return
				}
			}
		}()
	}
	wg.Wait()

	if len(c.Connections()) != 0 {
		t.Fatalf("%d subscribers were left behind", len(c.Connections()))
	}
}

// channelTestPresenceIDs is the member ids out of [joaju.Channel.Data].
func channelTestPresenceIDs(t *testing.T, c joaju.Channel) []string {
	t.Helper()

	presence, ok := c.Data()["presence"].(map[string]any)
	if !ok {
		t.Fatalf("%s published no presence block", c.Name().Requested())
	}
	ids, ok := presence["ids"].([]string)
	if !ok {
		t.Fatalf("the ids of %s are %T", c.Name().Requested(), presence["ids"])
	}

	return ids
}

// channelTestContains keeps the assertion above readable.
func channelTestContains(haystack []string, needle string) bool {
	for _, have := range haystack {
		if have == needle {
			return true
		}
	}

	return false
}
