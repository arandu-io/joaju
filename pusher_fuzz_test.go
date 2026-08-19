package joaju

import (
	"bytes"
	"context"
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
)

// fuzzTenant is the customer every Grant in these targets belongs to.
//
// A client cannot spell broadcasting.TenantSeparator in a channel name at all,
// so this string followed by it is a marker: a frame carrying it is a frame
// naming the namespace its receiver was scoped to.
const fuzzTenant = "acme"

// fuzzAllocationSlack is how much one decode may allocate on top of the bytes it
// was handed.
//
// A parser holding n bytes cannot produce a document larger than n, so anything
// far above n is memory the sender's shape bought without sending the bytes for
// it. The slack covers the scanner, the frame and whatever the runtime charges
// to the goroutine while it works.
const fuzzAllocationSlack = 8 << 20

// fuzzNests reports whether the input is mostly structural openers, which is the
// shape that makes a recursive decoder allocate per level rather than per byte.
//
// The allocation invariant is checked on those inputs alone: reading the memory
// statistics stops the world, and a document that spends its bytes on content
// cannot cost more than its length.
func fuzzNests(data []byte) bool {
	if len(data) < 256 {
		return false
	}

	openers := bytes.Count(data, []byte{'['}) + bytes.Count(data, []byte{'{'})

	return openers*2 >= len(data)
}

// fuzzJoinGrant is the Grant a [SubscriptionPolicy] issues, for one subject of
// [fuzzTenant].
func fuzzJoinGrant(t testing.TB, user string) auth.Grant {
	t.Helper()

	g, err := auth.Authorize(context.Background(), channelTestJoinPolicy{},
		auth.Subject{ID: user, Tenant: fuzzTenant}, broadcasting.ChannelJoin, Subscription{})
	if err != nil {
		t.Fatalf("authorizing %s to join: %v", user, err)
	}

	return g
}

// FuzzDecode feeds arbitrary bytes to the frame codec.
//
// [Decode] is where a client's payload stops being bytes and becomes a
// structure, and the socket that handed it over has authenticated nothing about
// the contents. What is asserted is what it promises: it returns rather than
// panicking, an accepted frame names an event, it produces no more bytes than it
// was given, its data is always something [Encode] can write back out, and what
// it accepted survives a round trip unchanged.
func FuzzDecode(f *testing.F) {
	f.Add([]byte(`{"event":"pusher:ping"}`))
	f.Add([]byte(`{"event":"pusher:subscribe","data":{"channel":"private-orders.17"}}`))
	f.Add([]byte(`{"event":"client-typing","data":"{\"at\":1}","channel":"presence-room"}`))
	// The data field in each of the shapes the protocol's ambiguity allows: an
	// object, a string containing an object, a string containing a scalar, and a
	// string containing something that is not JSON at all.
	f.Add([]byte(`{"event":"x","data":"123"}`))
	f.Add([]byte(`{"event":"x","data":"not json"}`))
	f.Add([]byte(`{"event":"x","data":"\"{\\\"a\\\":1}\""}`))
	f.Add([]byte(`{"event":"x","data":null}`))
	f.Add([]byte(`{"event":"x","data":{"a" : 1}}`))
	// A user_id a client wrote for itself, which no path may believe.
	f.Add([]byte(`{"event":"client-x","channel":"presence-room","user_id":"somebody-else"}`))
	// The frame with no event name, which json.Unmarshal alone would accept.
	f.Add([]byte(`{}`))
	// Escapes that shrink, including a lone surrogate.
	f.Add([]byte(`{"event":"A\ud800","channel":"💡"}`))
	// Nesting, which is the one input whose cost is not its length.
	f.Add([]byte(`{"event":"x","data":` + strings.Repeat("[", 200) + strings.Repeat("]", 200) + `}`))
	f.Add(append(bytes.Repeat([]byte{'['}, 512), bytes.Repeat([]byte{']'}, 512)...))

	f.Fuzz(func(t *testing.T, data []byte) {
		if fuzzNests(data) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, _ = Decode(data)
			runtime.ReadMemStats(&after)

			if grown := after.TotalAlloc - before.TotalAlloc; grown > uint64(len(data))+fuzzAllocationSlack {
				t.Fatalf("decoding %d bytes allocated %d bytes", len(data), grown)
			}
		}

		first, err := Decode(data)
		if err != nil {
			return
		}

		if first.Event == "" {
			t.Fatal("an accepted frame names no event")
		}
		// Nothing the decoder produces is longer than what it read. A field that
		// grew came from the shape of the document rather than from its bytes,
		// and the frame is held for the life of the call that reads it.
		if size := len(first.Event) + len(first.Channel) + len(first.UserID) + len(first.Data); size > len(data) {
			t.Fatalf("a frame of %d bytes came out of %d bytes of input", size, len(data))
		}
		if len(first.Data) > 0 && !json.Valid(first.Data) {
			t.Fatalf("an accepted frame carries data that is not JSON: %q", first.Data)
		}

		// Every accepted frame can be written back. The decoder leaves the data
		// field as JSON whatever the client wrapped it in, so there is no message
		// a client can send that this server then fails to re-encode.
		encoded, err := Encode(first)
		if err != nil {
			t.Fatalf("encoding a frame that was just decoded = %v", err)
		}

		// One encoding compacts the data, so the fixed point is reached after it
		// rather than at the frame the client sent. From there the codec must
		// agree with itself: what it accepts, re-encoded, decodes the same.
		second, err := Decode(encoded)
		if err != nil {
			t.Fatalf("rereading a frame that was just written = %v", err)
		}
		again, err := Encode(second)
		if err != nil {
			t.Fatalf("encoding a frame that survived a round trip = %v", err)
		}
		third, err := Decode(again)
		if err != nil {
			t.Fatalf("rereading a frame on the second round trip = %v", err)
		}

		if second.Event != third.Event || second.Channel != third.Channel ||
			second.UserID != third.UserID || !bytes.Equal(second.Data, third.Data) {
			t.Fatalf("the round trip turned %q %q %q %q into %q %q %q %q",
				second.Event, second.Channel, second.UserID, second.Data,
				third.Event, third.Channel, third.UserID, third.Data)
		}
	})
}

// FuzzClientMaySend asserts the closed list of what a socket is allowed to say.
//
// The list is short and the consequence of widening it is not: a client able to
// send a "pusher_internal:" event is a client able to invent the members of a
// presence channel it is on. So the assertion is stated the other way round --
// nothing outside the four protocol events and the client- namespace may pass.
func FuzzClientMaySend(f *testing.F) {
	f.Add(EventSubscribe)
	f.Add(EventMemberAdded)
	f.Add(ClientEventPrefix + "typing")
	f.Add(ProtocolPrefix)
	f.Add(InternalPrefix + "subscription_succeeded")
	f.Add("orders.updated")
	f.Add("")
	// Case and whitespace around a name that would otherwise pass, which is
	// where a prefix test written with a fold or a trim would let one through.
	f.Add(" " + EventSubscribe)
	f.Add(strings.ToUpper(ClientEventPrefix) + "typing")

	f.Fuzz(func(t *testing.T, event string) {
		allowed := Frame{Event: event}.ClientMaySend() == nil
		if !allowed {
			return
		}

		switch event {
		case EventSubscribe, EventUnsubscribe, EventPing, EventPong:
			return
		}
		if !strings.HasPrefix(event, ClientEventPrefix) {
			t.Fatalf("a client may send %q", event)
		}
		// A client event name that also wears one of the server's namespaces
		// would be relayed to every other browser on the channel under a name
		// they read as the server's.
		if strings.HasPrefix(event, ProtocolPrefix) || strings.HasPrefix(event, InternalPrefix) {
			t.Fatalf("a client may send %q, which is in a reserved namespace", event)
		}
	})
}

// FuzzSubscribeRequest feeds arbitrary bytes to the data of a pusher:subscribe
// frame, and reads the member out of what comes back.
//
// This is the payload that decides which channel a socket reaches and who it
// claims to be on it, and every byte of it is the client's. What is asserted is
// that it cannot be made to panic, that an accepted request names a channel,
// that nothing it produces is larger than what it read, and that the presence
// data it hands a policy is always something that can be encoded again -- a
// member's user_info is written back out to every other subscriber of the
// channel.
func FuzzSubscribeRequest(f *testing.F) {
	f.Add([]byte(`{"channel":"private-orders.17"}`))
	f.Add([]byte(`{"channel":"presence-room","auth":"key:0123","channel_data":"{\"user_id\":\"u1\"}"}`))
	f.Add([]byte(`{"channel":"presence-room","channel_data":{"user_id":7,"user_info":{"name":"Ana"}}}`))
	// A user_id in each shape the clients send, and two they should not.
	f.Add([]byte(`{"channel":"presence-room","channel_data":{"user_id":007}}`))
	f.Add([]byte(`{"channel":"presence-room","channel_data":{"user_id":1e400}}`))
	f.Add([]byte(`{"channel":"presence-room","channel_data":{"user_id":true}}`))
	f.Add([]byte(`{"channel":"presence-room","channel_data":{"user_id":null,"user_info":null}}`))
	f.Add([]byte(`{"channel":"presence-room","channel_data":{"user_id":{"a":1}}}`))
	// user_info arriving as a string containing JSON, which is the second layer
	// of the protocol's double encoding.
	f.Add([]byte(`{"channel":"presence-room","channel_data":{"user_id":"u1","user_info":"{\"n\":1}"}}`))
	f.Add([]byte(`{"channel":""}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(nil))

	f.Fuzz(func(t *testing.T, data []byte) {
		frame := Frame{Event: EventSubscribe, Data: json.RawMessage(data)}

		request, err := frame.Subscribe()
		if err != nil {
			return
		}

		if request.Channel == "" {
			t.Fatal("an accepted subscription names no channel")
		}
		// The size is asserted over the data a frame can actually carry. A JSON
		// decoder widens every byte it cannot read into U+FFFD, three bytes out
		// for one in, and [Decode] refuses a message carrying such a byte -- so
		// the data that reaches here off a socket is always UTF-8, and a caller
		// reaching this function directly is the only way to the other case.
		if size := len(request.Channel) + len(request.Auth) + len(request.ChannelData); utf8.Valid(data) && size > len(data) {
			t.Fatalf("a request of %d bytes came out of %d bytes of input", size, len(data))
		}
		if len(request.ChannelData) > 0 && !json.Valid(request.ChannelData) {
			t.Fatalf("an accepted subscription carries channel_data that is not JSON: %q", request.ChannelData)
		}

		member, err := request.Member()
		if err != nil {
			return
		}

		// Guarded on the input being UTF-8 for the reason the size above it is,
		// and it is the same widening seen one field further in: a user_id of
		// bytes no decoder can read comes back three bytes out for one in.
		if size := len(member.UserID) + len(member.Info); utf8.Valid(data) && size > len(data) {
			t.Fatalf("a member of %d bytes came out of %d bytes of input", size, len(data))
		}
		if len(member.Info) > 0 && !json.Valid(member.Info) {
			t.Fatalf("an accepted member carries user_info that is not JSON: %q", member.Info)
		}
		// The member is announced to the rest of a presence channel and listed in
		// the confirmation every later subscriber reads, so a member that cannot
		// be encoded is a channel nobody can join.
		if _, err := json.Marshal(member); err != nil {
			t.Fatalf("encoding an accepted member = %v", err)
		}

		// An unsubscription reads the same field out of the same shape, so the
		// two decoders must agree about which channel was named.
		leaving, err := Frame{Event: EventUnsubscribe, Data: json.RawMessage(data)}.Unsubscribe()
		if err != nil {
			t.Fatalf("unsubscribing from data a subscription accepted = %v", err)
		}
		if leaving != request.Channel {
			t.Fatalf("subscribing named %q and unsubscribing named %q", request.Channel, leaving)
		}
	})
}

// FuzzClientEventsAccept is the gate a browser's frame passes before every other
// browser on the channel receives it.
//
// It is the only path in this repository where a client is the source of what
// other clients are told, so the assertions are its four refusals and the one
// field it must never take from the sender: the user_id on the relayed event is
// the channel's record of who took the seat, and a frame that named somebody
// else must not change it.
func FuzzClientEventsAccept(f *testing.F) {
	f.Add(ClientEventPrefix+"typing", "presence-room", "presence-room", []byte(`{"at":1}`), "impostor", "u1", true, true)
	f.Add(ClientEventPrefix+"typing", "presence-room", "presence-room", []byte(`{}`), "", "u1", true, false)
	f.Add(ClientEventPrefix+"typing", "orders", "orders", []byte(`{}`), "", "u1", true, true)
	f.Add(ClientEventPrefix+"typing", "private-orders", "private-orders", []byte(`{}`), "", "", true, true)
	f.Add(ClientEventPrefix+"typing", "private-orders", "private-orders", []byte(`{}`), "", "u1", false, true)
	f.Add(EventSubscribe, "private-orders", "private-orders", []byte(`{}`), "", "u1", true, true)
	// The frame naming one channel while the caller resolved another, which is
	// the mistake that would relay a message into a channel nobody asked about.
	f.Add(ClientEventPrefix+"typing", "private-a", "private-b", []byte(`{}`), "", "u1", true, true)
	f.Add(ClientEventPrefix+"typing", "", "private-orders", []byte(`{}`), "", "u1", true, true)
	f.Add(ClientEventPrefix, "cache-x", "cache-x", []byte(`null`), "", "u1", true, true)
	f.Add(ClientEventPrefix+"x", "private-encrypted-a", "private-encrypted-a", []byte(`1`), "", "u1", true, true)

	f.Fuzz(func(t *testing.T, event, named, resolved string, data []byte, claimed, seated string, on, subscribed bool) {
		// The zero name is a legitimate input: it is what a failed resolution
		// hands the caller, and Accept has to refuse it rather than read it.
		channel, err := NewChannelName(fuzzJoinGrant(t, "caller"), resolved)
		if err != nil {
			channel = ChannelName{}
		}

		events := ClientEventsOff
		if on {
			events = ClientEventsOn
		}
		frame := Frame{Event: event, Channel: named, Data: json.RawMessage(data), UserID: claimed}

		got, err := events.Accept(frame, channel, "7.1", Member{UserID: seated}, subscribed)
		if err != nil {
			return
		}

		if !on {
			t.Fatal("a client event was relayed by a server that does not relay them")
		}
		if !subscribed {
			t.Fatalf("a client event on %q was relayed for a socket that is not on it", named)
		}
		if !strings.HasPrefix(event, ClientEventPrefix) {
			t.Fatalf("%q was relayed as a client event", event)
		}
		if !channel.Type().Guarded() {
			t.Fatalf("a client event was relayed on %q, which no policy guards", channel.Requested())
		}
		if channel.Requested() != named {
			t.Fatalf("a client event named %q and was relayed on %q", named, channel.Requested())
		}

		// The sender is the seat the channel is holding, and the frame's own
		// user_id is never read. A relayed event that carried the claim would let
		// one browser publish to a channel as another person on it.
		if got.UserID != seated {
			t.Fatalf("a client event claiming %q was relayed as %q, and the seat says %q", claimed, got.UserID, seated)
		}
		if got.Socket != "7.1" {
			t.Fatalf("a client event was relayed as coming from socket %q", got.Socket)
		}
		if got.Name != event {
			t.Fatalf("a client event named %q was relayed as %q", event, got.Name)
		}
		if got.Channel.Tenant() != fuzzTenant {
			t.Fatalf("a client event was relayed on tenant %q", got.Channel.Tenant())
		}
		if !bytes.Equal(got.Data, frame.Data) {
			t.Fatalf("the payload %q was relayed as %q", frame.Data, got.Data)
		}
	})
}
