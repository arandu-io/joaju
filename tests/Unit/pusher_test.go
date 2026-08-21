package unit

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/joaju"
	"github.com/arandu-io/joaju/protocols/pusher"
	"github.com/arandu-io/joaju/tests"
)

func TestEncodeWritesDataAsAStringContainingJSON(t *testing.T) {
	f := pusher.Frame{
		Event:   "orders.updated",
		Channel: "private-orders.17",
		Data:    json.RawMessage(`{ "id" : 17 }`),
	}

	want := `{"event":"orders.updated","data":"{\"id\":17}","channel":"private-orders.17"}`
	if got := tests.Encode(t, f); got != want {
		t.Errorf("Encode() = %s, want %s", got, want)
	}
}

func TestEncodeOmitsTheFieldsWithNoValue(t *testing.T) {
	if got, want := tests.Encode(t, pusher.Pong()), `{"event":"pusher:pong"}`; got != want {
		t.Errorf("Encode(Pong()) = %s, want %s", got, want)
	}
}

func TestEncodeRefusesAFrameWithNoEventName(t *testing.T) {
	if _, err := pusher.Encode(pusher.Frame{Channel: "orders"}); err == nil {
		t.Fatal("Encode() of a frame with no event name = nil, want an error")
	}
}

func TestEncodeRefusesDataThatIsNotJSON(t *testing.T) {
	f := pusher.Frame{Event: "orders.updated", Data: json.RawMessage(`{`)}
	if _, err := pusher.Encode(f); err == nil {
		t.Fatal("Encode() of a frame with broken data = nil, want an error")
	}
}

func TestDecodeReadsDataInEitherShape(t *testing.T) {
	cases := []struct {
		name    string
		message string
		want    string
	}{
		{
			name:    "an object, which is how a subscription arrives",
			message: `{"event":"pusher:subscribe","data":{"channel":"orders"}}`,
			want:    `{"channel":"orders"}`,
		},
		{
			name:    "a string containing JSON, which is how a client event arrives",
			message: `{"event":"client-typing","data":"{\"at\":1}"}`,
			want:    `{"at":1}`,
		},
		{
			name:    "a string that is not JSON, which stays the string it is",
			message: `{"event":"client-typing","data":"hello"}`,
			want:    `"hello"`,
		},
		{
			name:    "no data at all",
			message: `{"event":"pusher:ping"}`,
			want:    "",
		},
		{
			name:    "a null, which is not data either",
			message: `{"event":"pusher:ping","data":null}`,
			want:    "",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, err := pusher.Decode([]byte(c.message))
			if err != nil {
				t.Fatalf("Decode(%s) = %v", c.message, err)
			}
			if got := string(f.Data); got != c.want {
				t.Errorf("Data = %s, want %s", got, c.want)
			}
		})
	}
}

func TestDecodeRefusesAMessageItCannotRead(t *testing.T) {
	cases := map[string]string{
		"not JSON":       `{`,
		"no event name":  `{"channel":"orders"}`,
		"an empty event": `{"event":""}`,
	}

	for name, message := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := pusher.Decode([]byte(message))
			if !errors.Is(err, pusher.ErrInvalidMessage) {
				t.Fatalf("Decode(%s) = %v, want ErrInvalidMessage", message, err)
			}
		})
	}
}

// TestDecodeRefusesAMessageThatIsNotUTF8 covers the bytes a JSON decoder would
// have read anyway, by replacing each of them.
//
// The message is well-formed JSON in every other respect, so nothing but the
// encoding refuses it.
func TestDecodeRefusesAMessageThatIsNotUTF8(t *testing.T) {
	cases := map[string]string{
		"in the event name":   "{\"event\":\"\xac\xac\"}",
		"in a channel name":   "{\"event\":\"pusher:subscribe\",\"channel\":\"private-\xeb\"}",
		"in the data":         "{\"event\":\"client-typing\",\"data\":\"\xff\"}",
		"a truncated rune":    "{\"event\":\"\xf0\x9f\x92\"}",
		"a bare continuation": "{\"event\":\"a\x80b\"}",
	}

	for name, message := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := pusher.Decode([]byte(message))
			if !errors.Is(err, pusher.ErrInvalidMessage) {
				t.Fatalf("Decode(%q) = %v, want ErrInvalidMessage", message, err)
			}
		})
	}
}

// TestDecodeKeepsTwoChannelNamesApart is what the encoding check is for.
//
// A JSON decoder maps every byte it cannot read onto one replacement rune, so
// two clients asking for two different channels would be asking for the same
// one. The name is the key a channel is held under, and sharing it is sharing
// the events on it.
func TestDecodeKeepsTwoChannelNamesApart(t *testing.T) {
	one := "{\"event\":\"pusher:subscribe\",\"data\":{\"channel\":\"private-\xac\"}}"
	other := "{\"event\":\"pusher:subscribe\",\"data\":{\"channel\":\"private-\xeb\"}}"

	first, firstErr := pusher.Decode([]byte(one))
	second, secondErr := pusher.Decode([]byte(other))
	if firstErr == nil || secondErr == nil {
		t.Fatalf("Decode() accepted %+v and %+v, and the two channels differ only in a byte neither is", first, second)
	}
}

func TestFrameRoundTrips(t *testing.T) {
	want := pusher.Frame{
		Event:   "client-typing",
		Channel: "presence-room.1",
		Data:    json.RawMessage(`{"at":1}`),
		UserID:  "7",
	}

	got, err := pusher.Decode([]byte(tests.Encode(t, want)))
	if err != nil {
		t.Fatalf("Decode() = %v", err)
	}
	if got.Event != want.Event || got.Channel != want.Channel || got.UserID != want.UserID {
		t.Errorf("Decode(Encode(f)) = %+v, want %+v", got, want)
	}
	if string(got.Data) != string(want.Data) {
		t.Errorf("Data = %s, want %s", got.Data, want.Data)
	}
}

func TestConnectionEstablishedCarriesTheSocketID(t *testing.T) {
	got := tests.Encode(t, pusher.ConnectionEstablished("7.1", 30*time.Second))
	want := `{"event":"pusher:connection_established","data":"{\"socket_id\":\"7.1\",\"activity_timeout\":30}"}`
	if got != want {
		t.Errorf("Encode(ConnectionEstablished()) = %s, want %s", got, want)
	}
}

func TestConnectionEstablishedOmitsATimeoutItWasNotGiven(t *testing.T) {
	got := tests.Encode(t, pusher.ConnectionEstablished("7.1", 0))
	want := `{"event":"pusher:connection_established","data":"{\"socket_id\":\"7.1\"}"}`
	if got != want {
		t.Errorf("Encode(ConnectionEstablished()) = %s, want %s", got, want)
	}
}

func TestConnectionEstablishedNeverAsksForATimeoutOfZeroSeconds(t *testing.T) {
	f := pusher.ConnectionEstablished("7.1", 100*time.Millisecond)
	if got := string(f.Data); !strings.Contains(got, `"activity_timeout":1`) {
		t.Errorf("Data = %s, want an activity_timeout of 1", got)
	}
}

func TestSubscriptionSucceededAlwaysCarriesAnObject(t *testing.T) {
	f, err := pusher.SubscriptionSucceeded(tests.ChannelName(t, "orders"), nil)
	if err != nil {
		t.Fatalf("SubscriptionSucceeded() = %v", err)
	}

	want := `{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"orders"}`
	if got := tests.Encode(t, f); got != want {
		t.Errorf("Encode() = %s, want %s", got, want)
	}
}

func TestMemberAddedCarriesTheWholeMember(t *testing.T) {
	name := tests.ChannelName(t, "presence-room.1")
	member := joaju.Member{UserID: "7", Info: json.RawMessage(`{"name":"Ana"}`)}

	f, err := pusher.MemberAdded(name, member)
	if err != nil {
		t.Fatalf("MemberAdded() = %v", err)
	}

	want := `{"event":"pusher_internal:member_added","data":"{\"user_id\":\"7\",\"user_info\":{\"name\":\"Ana\"}}","channel":"presence-room.1"}`
	if got := tests.Encode(t, f); got != want {
		t.Errorf("Encode() = %s, want %s", got, want)
	}
}

func TestMemberRemovedCarriesOnlyTheUserID(t *testing.T) {
	name := tests.ChannelName(t, "presence-room.1")
	member := joaju.Member{UserID: "7", Info: json.RawMessage(`{"name":"Ana"}`)}

	f, err := pusher.MemberRemoved(name, member)
	if err != nil {
		t.Fatalf("MemberRemoved() = %v", err)
	}

	want := `{"event":"pusher_internal:member_removed","data":"{\"user_id\":\"7\"}","channel":"presence-room.1"}`
	if got := tests.Encode(t, f); got != want {
		t.Errorf("Encode() = %s, want %s", got, want)
	}
}

func TestCacheMissNamesTheChannelAndCarriesNothing(t *testing.T) {
	f, err := pusher.CacheMiss(tests.ChannelName(t, "cache-prices"))
	if err != nil {
		t.Fatalf("CacheMiss() = %v", err)
	}

	want := `{"event":"pusher:cache_miss","channel":"cache-prices"}`
	if got := tests.Encode(t, f); got != want {
		t.Errorf("Encode() = %s, want %s", got, want)
	}
}

// TestNoFrameCarriesTheTenant is the tenant rule on the way out: the client
// asked about a name with no tenant in it and is answered about the same name.
// Every
// constructor that names a channel is here, because the mistake is one line in
// any one of them.
func TestNoFrameCarriesTheTenant(t *testing.T) {
	name := tests.ChannelName(t, "presence-room.1")
	if !strings.HasPrefix(name.String(), tests.Tenant+":") {
		t.Fatalf("String() = %s, want it to carry the tenant", name.String())
	}

	subscribed, err := pusher.SubscriptionSucceeded(name, map[string]any{"presence": map[string]any{"count": 1}})
	if err != nil {
		t.Fatalf("SubscriptionSucceeded() = %v", err)
	}
	added, err := pusher.MemberAdded(name, joaju.Member{UserID: "7"})
	if err != nil {
		t.Fatalf("MemberAdded() = %v", err)
	}
	removed, err := pusher.MemberRemoved(name, joaju.Member{UserID: "7"})
	if err != nil {
		t.Fatalf("MemberRemoved() = %v", err)
	}
	missed, err := pusher.CacheMiss(name)
	if err != nil {
		t.Fatalf("CacheMiss() = %v", err)
	}
	carried, err := pusher.EventFrame(joaju.Event{Name: "orders.updated", Channel: name, Data: json.RawMessage(`{"id":17}`)})
	if err != nil {
		t.Fatalf("EventFrame() = %v", err)
	}

	for _, f := range []pusher.Frame{subscribed, added, removed, missed, carried} {
		if f.Channel != name.Requested() {
			t.Errorf("%s carried the channel %s, want %s", f.Event, f.Channel, name.Requested())
		}
		if got := tests.Encode(t, f); strings.Contains(got, tests.Tenant) {
			t.Errorf("%s reached the wire carrying the tenant: %s", f.Event, got)
		}
	}
}

func TestEventFrameRefusesAnEventWithNoChannel(t *testing.T) {
	if _, err := pusher.EventFrame(joaju.Event{Name: "orders.updated"}); err == nil {
		t.Fatal("EventFrame() with no channel = nil, want an error")
	}
}

func TestClientMaySendOnlyAClosedList(t *testing.T) {
	allowed := []string{
		joaju.EventSubscribe,
		joaju.EventUnsubscribe,
		joaju.EventPing,
		joaju.EventPong,
		"client-typing",
	}
	for _, event := range allowed {
		if err := (pusher.Frame{Event: event}).ClientMaySend(); err != nil {
			t.Errorf("ClientMaySend(%s) = %v, want nil", event, err)
		}
	}

	refused := []string{
		joaju.EventConnectionEstablished,
		joaju.EventError,
		pusher.EventCacheMiss,
		joaju.EventSubscriptionSucceeded,
		joaju.EventMemberAdded,
		joaju.EventMemberRemoved,
		"orders.updated",
	}
	for _, event := range refused {
		err := (pusher.Frame{Event: event}).ClientMaySend()
		if !errors.Is(err, pusher.ErrInvalidMessage) {
			t.Errorf("ClientMaySend(%s) = %v, want ErrInvalidMessage", event, err)
		}
	}
}

func TestSubscribeReadsWhatTheClientAskedFor(t *testing.T) {
	message := `{"event":"pusher:subscribe","data":{"channel":"presence-room.1","auth":"key:signature","channel_data":"{\"user_id\":7,\"user_info\":{\"name\":\"Ana\"}}"}}`

	f, err := pusher.Decode([]byte(message))
	if err != nil {
		t.Fatalf("Decode() = %v", err)
	}
	r, err := f.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}
	if r.Channel != "presence-room.1" {
		t.Errorf("Channel = %s, want presence-room.1", r.Channel)
	}
	if r.Auth != "key:signature" {
		t.Errorf("Auth = %s, want key:signature", r.Auth)
	}

	member, err := r.Member()
	if err != nil {
		t.Fatalf("Member() = %v", err)
	}
	if member.UserID != "7" {
		t.Errorf("UserID = %s, want 7 -- a numeric user_id is read as it was written", member.UserID)
	}
	if string(member.Info) != `{"name":"Ana"}` {
		t.Errorf("Info = %s, want {\"name\":\"Ana\"}", member.Info)
	}
}

func TestSubscribeWithNoPresenceDataHasNoMember(t *testing.T) {
	f, err := pusher.Decode([]byte(`{"event":"pusher:subscribe","data":{"channel":"orders"}}`))
	if err != nil {
		t.Fatalf("Decode() = %v", err)
	}
	r, err := f.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe() = %v", err)
	}

	member, err := r.Member()
	if err != nil {
		t.Fatalf("Member() = %v", err)
	}
	if member.UserID != "" || member.Info != nil {
		t.Errorf("Member() = %+v, want the zero value", member)
	}
}

func TestSubscribeRefusesARequestWithNoChannel(t *testing.T) {
	f := pusher.Frame{Event: joaju.EventSubscribe, Data: json.RawMessage(`{}`)}
	if _, err := f.Subscribe(); !errors.Is(err, pusher.ErrInvalidMessage) {
		t.Fatalf("Subscribe() = %v, want ErrInvalidMessage", err)
	}
}

func TestSubscribeRefusesAnotherEvent(t *testing.T) {
	f := pusher.Frame{Event: joaju.EventUnsubscribe, Data: json.RawMessage(`{"channel":"orders"}`)}
	if _, err := f.Subscribe(); !errors.Is(err, pusher.ErrInvalidMessage) {
		t.Fatalf("Subscribe() = %v, want ErrInvalidMessage", err)
	}
}

func TestUnsubscribeReadsTheChannel(t *testing.T) {
	f, err := pusher.Decode([]byte(`{"event":"pusher:unsubscribe","data":{"channel":"orders"}}`))
	if err != nil {
		t.Fatalf("Decode() = %v", err)
	}

	channel, err := f.Unsubscribe()
	if err != nil {
		t.Fatalf("Unsubscribe() = %v", err)
	}
	if channel != "orders" {
		t.Errorf("Unsubscribe() = %s, want orders", channel)
	}
}

func TestClientEventsAreOffInTheZeroValue(t *testing.T) {
	var events pusher.ClientEvents

	f := pusher.Frame{Event: "client-typing", Channel: "private-room.1"}
	_, err := events.Accept(f, tests.ChannelName(t, "private-room.1"), "7.1", joaju.Member{}, true)
	if !errors.Is(err, pusher.ErrClientEventsDisabled) {
		t.Fatalf("Accept() = %v, want ErrClientEventsDisabled", err)
	}
}

func TestClientEventsRefuseAPublicChannel(t *testing.T) {
	f := pusher.Frame{Event: "client-typing", Channel: "room.1"}
	_, err := pusher.ClientEventsOn.Accept(f, tests.ChannelName(t, "room.1"), "7.1", joaju.Member{}, true)
	if !errors.Is(err, pusher.ErrClientEventChannel) {
		t.Fatalf("Accept() = %v, want ErrClientEventChannel", err)
	}
}

func TestClientEventsRefuseSomebodyWhoIsNotOnTheChannel(t *testing.T) {
	f := pusher.Frame{Event: "client-typing", Channel: "private-room.1"}
	_, err := pusher.ClientEventsOn.Accept(f, tests.ChannelName(t, "private-room.1"), "7.1", joaju.Member{}, false)
	if !errors.Is(err, pusher.ErrNotSubscribed) {
		t.Fatalf("Accept() = %v, want ErrNotSubscribed", err)
	}
}

func TestClientEventsRefuseAFrameThatIsNotOne(t *testing.T) {
	f := pusher.Frame{Event: joaju.EventMemberAdded, Channel: "presence-room.1"}
	_, err := pusher.ClientEventsOn.Accept(f, tests.ChannelName(t, "presence-room.1"), "7.1", joaju.Member{}, true)
	if !errors.Is(err, pusher.ErrInvalidMessage) {
		t.Fatalf("Accept() = %v, want ErrInvalidMessage", err)
	}
}

func TestClientEventsRefuseAChannelResolvedFromAnotherName(t *testing.T) {
	f := pusher.Frame{Event: "client-typing", Channel: "private-room.1"}
	_, err := pusher.ClientEventsOn.Accept(f, tests.ChannelName(t, "private-room.2"), "7.1", joaju.Member{}, true)
	if !errors.Is(err, pusher.ErrInvalidMessage) {
		t.Fatalf("Accept() = %v, want ErrInvalidMessage", err)
	}
}

func TestClientEventsAcceptASubscriberOfAGuardedChannel(t *testing.T) {
	name := tests.ChannelName(t, "private-room.1")
	f := pusher.Frame{Event: "client-typing", Channel: "private-room.1", Data: json.RawMessage(`{"at":1}`)}

	// A private channel seats no member, so there is nobody to name and the
	// relayed frame says so by leaving the field out.
	e, err := pusher.ClientEventsOn.Accept(f, name, "7.1", joaju.Member{}, true)
	if err != nil {
		t.Fatalf("Accept() = %v", err)
	}
	if e.Name != "client-typing" {
		t.Errorf("Name = %s, want client-typing", e.Name)
	}
	if e.Channel != name {
		t.Errorf("Channel = %v, want %v", e.Channel, name)
	}
	if e.Socket != "7.1" {
		t.Errorf("Socket = %s, want 7.1 -- the sender is skipped by the relay", e.Socket)
	}
	if string(e.Data) != `{"at":1}` {
		t.Errorf("Data = %s, want {\"at\":1}", e.Data)
	}
	if e.UserID != "" {
		t.Errorf("UserID = %s, want none: a private channel has no member to name", e.UserID)
	}

	relayed, err := pusher.EventFrame(e)
	if err != nil {
		t.Fatalf("EventFrame() = %v", err)
	}

	want := `{"event":"client-typing","data":"{\"at\":1}","channel":"private-room.1"}`
	if got := tests.Encode(t, relayed); got != want {
		t.Errorf("Encode() = %s, want %s -- a user_id of \"\" is a key a client has to be taught to expect", got, want)
	}
}

// The user_id on a relayed client event is the channel's record of who took the
// seat, and a frame that claims another one does not get to say so. This is what
// makes "somebody is typing" name a person rather than a stranger's byte.
func TestClientEventsCarryTheSeatedMemberAndNotTheUserIDTheFrameClaims(t *testing.T) {
	name := tests.ChannelName(t, "presence-room.1")
	f := pusher.Frame{
		Event:   "client-typing",
		Channel: "presence-room.1",
		Data:    json.RawMessage(`{"at":1}`),
		UserID:  "somebody-else",
	}

	e, err := pusher.ClientEventsOn.Accept(f, name, "7.1", joaju.Member{UserID: "7", Info: json.RawMessage(`{"name":"Ana"}`)}, true)
	if err != nil {
		t.Fatalf("Accept() = %v", err)
	}
	if e.UserID != "7" {
		t.Fatalf("UserID = %s, want 7 -- the member the channel seated, not the one the frame named", e.UserID)
	}

	relayed, err := pusher.EventFrame(e)
	if err != nil {
		t.Fatalf("EventFrame() = %v", err)
	}

	// The user_id is top level and beside the channel, which is where a Pusher
	// client reads it. [Member.Info] is not in it: the receivers were given it
	// when the member arrived.
	want := `{"event":"client-typing","data":"{\"at\":1}","channel":"presence-room.1","user_id":"7"}`
	if got := tests.Encode(t, relayed); got != want {
		t.Errorf("Encode() = %s, want %s", got, want)
	}
}

func TestErrorFrameKeepsTheCodeAndDropsTheCause(t *testing.T) {
	err := errors.New("channel private-orders.17 denied for subject u_31: not the owner")

	got := tests.Encode(t, pusher.ErrorFrame(err))
	want := `{"event":"pusher:error","data":"{\"code\":4200,\"message\":\"Invalid message format\"}"}`
	if got != want {
		t.Errorf("ErrorFrame() = %s, want %s", got, want)
	}
	if strings.Contains(got, "u_31") {
		t.Errorf("ErrorFrame() disclosed the cause: %s", got)
	}
}

func TestErrorFrameMapsARefusedGrantToUnauthorized(t *testing.T) {
	// The zero Grant refuses everything, which is the shape every policy
	// refusal arrives in.
	var g auth.Grant
	err := g.Check(joaju.Connect)
	if err == nil {
		t.Fatal("the zero Grant allowed Connect")
	}

	f := pusher.ErrorFrame(err)
	if got, want := string(f.Data), `{"code":4009,"message":"Connection is unauthorized"}`; got != want {
		t.Errorf("ErrorFrame() data = %s, want %s", got, want)
	}
}

func TestErrorFrameKeepsAProtocolErrorFoundAtAnyDepth(t *testing.T) {
	_, err := pusher.Decode([]byte(`{`))

	f := pusher.ErrorFrame(err)
	if got, want := string(f.Data), `{"code":4200,"message":"Invalid message format"}`; got != want {
		t.Errorf("ErrorFrame() data = %s, want %s", got, want)
	}
}

func TestProtocolErrorFrameIsTheOneThePusherClientsRead(t *testing.T) {
	got := tests.Encode(t, pusher.ErrOverQuota.Frame())
	want := `{"event":"pusher:error","data":"{\"code\":4004,\"message\":\"Application is over connection quota\"}"}`
	if got != want {
		t.Errorf("Frame() = %s, want %s", got, want)
	}
}
