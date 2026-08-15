package joaju_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
	"github.com/arandu-io/joaju"
)

// tenant is the customer every channel in this file belongs to. It is a string
// no channel name here contains, so a test can assert that it did not reach the
// wire by looking for it.
const tenant = "acme"

// channelName is the only way a test gets a ChannelName, and it goes through
// the Grant like everything else.
func channelName(t *testing.T, requested string) joaju.ChannelName {
	t.Helper()

	g := auth.SystemGrant(broadcasting.ChannelJoin, tenant)
	name, err := joaju.NewChannelName(g, requested)
	if err != nil {
		t.Fatalf("NewChannelName(%q) = %v", requested, err)
	}

	return name
}

func encode(t *testing.T, f joaju.Frame) string {
	t.Helper()

	b, err := joaju.Encode(f)
	if err != nil {
		t.Fatalf("Encode(%v) = %v", f, err)
	}

	return string(b)
}

func TestEncodeWritesDataAsAStringContainingJSON(t *testing.T) {
	f := joaju.Frame{
		Event:   "orders.updated",
		Channel: "private-orders.17",
		Data:    json.RawMessage(`{ "id" : 17 }`),
	}

	want := `{"event":"orders.updated","data":"{\"id\":17}","channel":"private-orders.17"}`
	if got := encode(t, f); got != want {
		t.Errorf("Encode() = %s, want %s", got, want)
	}
}

func TestEncodeOmitsTheFieldsWithNoValue(t *testing.T) {
	if got, want := encode(t, joaju.Pong()), `{"event":"pusher:pong"}`; got != want {
		t.Errorf("Encode(Pong()) = %s, want %s", got, want)
	}
}

func TestEncodeRefusesAFrameWithNoEventName(t *testing.T) {
	if _, err := joaju.Encode(joaju.Frame{Channel: "orders"}); err == nil {
		t.Fatal("Encode() of a frame with no event name = nil, want an error")
	}
}

func TestEncodeRefusesDataThatIsNotJSON(t *testing.T) {
	f := joaju.Frame{Event: "orders.updated", Data: json.RawMessage(`{`)}
	if _, err := joaju.Encode(f); err == nil {
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
			f, err := joaju.Decode([]byte(c.message))
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
			_, err := joaju.Decode([]byte(message))
			if !errors.Is(err, joaju.ErrInvalidMessage) {
				t.Fatalf("Decode(%s) = %v, want ErrInvalidMessage", message, err)
			}
		})
	}
}

func TestFrameRoundTrips(t *testing.T) {
	want := joaju.Frame{
		Event:   "client-typing",
		Channel: "presence-room.1",
		Data:    json.RawMessage(`{"at":1}`),
		UserID:  "7",
	}

	got, err := joaju.Decode([]byte(encode(t, want)))
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
	got := encode(t, joaju.ConnectionEstablished("7.1", 30*time.Second))
	want := `{"event":"pusher:connection_established","data":"{\"socket_id\":\"7.1\",\"activity_timeout\":30}"}`
	if got != want {
		t.Errorf("Encode(ConnectionEstablished()) = %s, want %s", got, want)
	}
}

func TestConnectionEstablishedOmitsATimeoutItWasNotGiven(t *testing.T) {
	got := encode(t, joaju.ConnectionEstablished("7.1", 0))
	want := `{"event":"pusher:connection_established","data":"{\"socket_id\":\"7.1\"}"}`
	if got != want {
		t.Errorf("Encode(ConnectionEstablished()) = %s, want %s", got, want)
	}
}

func TestConnectionEstablishedNeverAsksForATimeoutOfZeroSeconds(t *testing.T) {
	f := joaju.ConnectionEstablished("7.1", 100*time.Millisecond)
	if got := string(f.Data); !strings.Contains(got, `"activity_timeout":1`) {
		t.Errorf("Data = %s, want an activity_timeout of 1", got)
	}
}

func TestSubscriptionSucceededAlwaysCarriesAnObject(t *testing.T) {
	f, err := joaju.SubscriptionSucceeded(channelName(t, "orders"), nil)
	if err != nil {
		t.Fatalf("SubscriptionSucceeded() = %v", err)
	}

	want := `{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"orders"}`
	if got := encode(t, f); got != want {
		t.Errorf("Encode() = %s, want %s", got, want)
	}
}

func TestMemberAddedCarriesTheWholeMember(t *testing.T) {
	name := channelName(t, "presence-room.1")
	member := joaju.Member{UserID: "7", Info: json.RawMessage(`{"name":"Ana"}`)}

	f, err := joaju.MemberAdded(name, member)
	if err != nil {
		t.Fatalf("MemberAdded() = %v", err)
	}

	want := `{"event":"pusher_internal:member_added","data":"{\"user_id\":\"7\",\"user_info\":{\"name\":\"Ana\"}}","channel":"presence-room.1"}`
	if got := encode(t, f); got != want {
		t.Errorf("Encode() = %s, want %s", got, want)
	}
}

func TestMemberRemovedCarriesOnlyTheUserID(t *testing.T) {
	name := channelName(t, "presence-room.1")
	member := joaju.Member{UserID: "7", Info: json.RawMessage(`{"name":"Ana"}`)}

	f, err := joaju.MemberRemoved(name, member)
	if err != nil {
		t.Fatalf("MemberRemoved() = %v", err)
	}

	want := `{"event":"pusher_internal:member_removed","data":"{\"user_id\":\"7\"}","channel":"presence-room.1"}`
	if got := encode(t, f); got != want {
		t.Errorf("Encode() = %s, want %s", got, want)
	}
}

func TestCacheMissNamesTheChannelAndCarriesNothing(t *testing.T) {
	f, err := joaju.CacheMiss(channelName(t, "cache-prices"))
	if err != nil {
		t.Fatalf("CacheMiss() = %v", err)
	}

	want := `{"event":"pusher:cache_miss","channel":"cache-prices"}`
	if got := encode(t, f); got != want {
		t.Errorf("Encode() = %s, want %s", got, want)
	}
}

// TestNoFrameCarriesTheTenant is RULE 14 on the way out: the client asked about
// a name with no tenant in it and is answered about the same name. Every
// constructor that names a channel is here, because the mistake is one line in
// any one of them.
func TestNoFrameCarriesTheTenant(t *testing.T) {
	name := channelName(t, "presence-room.1")
	if !strings.HasPrefix(name.String(), tenant+":") {
		t.Fatalf("String() = %s, want it to carry the tenant", name.String())
	}

	subscribed, err := joaju.SubscriptionSucceeded(name, map[string]any{"presence": map[string]any{"count": 1}})
	if err != nil {
		t.Fatalf("SubscriptionSucceeded() = %v", err)
	}
	added, err := joaju.MemberAdded(name, joaju.Member{UserID: "7"})
	if err != nil {
		t.Fatalf("MemberAdded() = %v", err)
	}
	removed, err := joaju.MemberRemoved(name, joaju.Member{UserID: "7"})
	if err != nil {
		t.Fatalf("MemberRemoved() = %v", err)
	}
	missed, err := joaju.CacheMiss(name)
	if err != nil {
		t.Fatalf("CacheMiss() = %v", err)
	}
	carried, err := joaju.EventFrame(joaju.Event{Name: "orders.updated", Channel: name, Data: json.RawMessage(`{"id":17}`)})
	if err != nil {
		t.Fatalf("EventFrame() = %v", err)
	}

	for _, f := range []joaju.Frame{subscribed, added, removed, missed, carried} {
		if f.Channel != name.Requested() {
			t.Errorf("%s carried the channel %s, want %s", f.Event, f.Channel, name.Requested())
		}
		if got := encode(t, f); strings.Contains(got, tenant) {
			t.Errorf("%s reached the wire carrying the tenant: %s", f.Event, got)
		}
	}
}

func TestEventFrameRefusesAnEventWithNoChannel(t *testing.T) {
	if _, err := joaju.EventFrame(joaju.Event{Name: "orders.updated"}); err == nil {
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
		if err := (joaju.Frame{Event: event}).ClientMaySend(); err != nil {
			t.Errorf("ClientMaySend(%s) = %v, want nil", event, err)
		}
	}

	refused := []string{
		joaju.EventConnectionEstablished,
		joaju.EventError,
		joaju.EventCacheMiss,
		joaju.EventSubscriptionSucceeded,
		joaju.EventMemberAdded,
		joaju.EventMemberRemoved,
		"orders.updated",
	}
	for _, event := range refused {
		err := (joaju.Frame{Event: event}).ClientMaySend()
		if !errors.Is(err, joaju.ErrInvalidMessage) {
			t.Errorf("ClientMaySend(%s) = %v, want ErrInvalidMessage", event, err)
		}
	}
}

func TestSubscribeReadsWhatTheClientAskedFor(t *testing.T) {
	message := `{"event":"pusher:subscribe","data":{"channel":"presence-room.1","auth":"key:signature","channel_data":"{\"user_id\":7,\"user_info\":{\"name\":\"Ana\"}}"}}`

	f, err := joaju.Decode([]byte(message))
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
	f, err := joaju.Decode([]byte(`{"event":"pusher:subscribe","data":{"channel":"orders"}}`))
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
	f := joaju.Frame{Event: joaju.EventSubscribe, Data: json.RawMessage(`{}`)}
	if _, err := f.Subscribe(); !errors.Is(err, joaju.ErrInvalidMessage) {
		t.Fatalf("Subscribe() = %v, want ErrInvalidMessage", err)
	}
}

func TestSubscribeRefusesAnotherEvent(t *testing.T) {
	f := joaju.Frame{Event: joaju.EventUnsubscribe, Data: json.RawMessage(`{"channel":"orders"}`)}
	if _, err := f.Subscribe(); !errors.Is(err, joaju.ErrInvalidMessage) {
		t.Fatalf("Subscribe() = %v, want ErrInvalidMessage", err)
	}
}

func TestUnsubscribeReadsTheChannel(t *testing.T) {
	f, err := joaju.Decode([]byte(`{"event":"pusher:unsubscribe","data":{"channel":"orders"}}`))
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
	var events joaju.ClientEvents

	f := joaju.Frame{Event: "client-typing", Channel: "private-room.1"}
	_, err := events.Accept(f, channelName(t, "private-room.1"), "7.1", true)
	if !errors.Is(err, joaju.ErrClientEventsDisabled) {
		t.Fatalf("Accept() = %v, want ErrClientEventsDisabled", err)
	}
}

func TestClientEventsRefuseAPublicChannel(t *testing.T) {
	f := joaju.Frame{Event: "client-typing", Channel: "room.1"}
	_, err := joaju.ClientEventsOn.Accept(f, channelName(t, "room.1"), "7.1", true)
	if !errors.Is(err, joaju.ErrClientEventChannel) {
		t.Fatalf("Accept() = %v, want ErrClientEventChannel", err)
	}
}

func TestClientEventsRefuseSomebodyWhoIsNotOnTheChannel(t *testing.T) {
	f := joaju.Frame{Event: "client-typing", Channel: "private-room.1"}
	_, err := joaju.ClientEventsOn.Accept(f, channelName(t, "private-room.1"), "7.1", false)
	if !errors.Is(err, joaju.ErrNotSubscribed) {
		t.Fatalf("Accept() = %v, want ErrNotSubscribed", err)
	}
}

func TestClientEventsRefuseAFrameThatIsNotOne(t *testing.T) {
	f := joaju.Frame{Event: joaju.EventMemberAdded, Channel: "presence-room.1"}
	_, err := joaju.ClientEventsOn.Accept(f, channelName(t, "presence-room.1"), "7.1", true)
	if !errors.Is(err, joaju.ErrInvalidMessage) {
		t.Fatalf("Accept() = %v, want ErrInvalidMessage", err)
	}
}

func TestClientEventsRefuseAChannelResolvedFromAnotherName(t *testing.T) {
	f := joaju.Frame{Event: "client-typing", Channel: "private-room.1"}
	_, err := joaju.ClientEventsOn.Accept(f, channelName(t, "private-room.2"), "7.1", true)
	if !errors.Is(err, joaju.ErrInvalidMessage) {
		t.Fatalf("Accept() = %v, want ErrInvalidMessage", err)
	}
}

func TestClientEventsAcceptASubscriberOfAGuardedChannel(t *testing.T) {
	name := channelName(t, "private-room.1")
	f := joaju.Frame{Event: "client-typing", Channel: "private-room.1", Data: json.RawMessage(`{"at":1}`)}

	e, err := joaju.ClientEventsOn.Accept(f, name, "7.1", true)
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
}

func TestErrorFrameKeepsTheCodeAndDropsTheCause(t *testing.T) {
	err := errors.New("channel private-orders.17 denied for subject u_31: not the owner")

	got := encode(t, joaju.ErrorFrame(err))
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

	f := joaju.ErrorFrame(err)
	if got, want := string(f.Data), `{"code":4009,"message":"Connection is unauthorized"}`; got != want {
		t.Errorf("ErrorFrame() data = %s, want %s", got, want)
	}
}

func TestErrorFrameKeepsAProtocolErrorFoundAtAnyDepth(t *testing.T) {
	_, err := joaju.Decode([]byte(`{`))

	f := joaju.ErrorFrame(err)
	if got, want := string(f.Data), `{"code":4200,"message":"Invalid message format"}`; got != want {
		t.Errorf("ErrorFrame() data = %s, want %s", got, want)
	}
}

func TestProtocolErrorFrameIsTheOneThePusherClientsRead(t *testing.T) {
	got := encode(t, joaju.ErrOverQuota.Frame())
	want := `{"event":"pusher:error","data":"{\"code\":4004,\"message\":\"Application is over connection quota\"}"}`
	if got != want {
		t.Errorf("Frame() = %s, want %s", got, want)
	}
}
