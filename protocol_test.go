package joaju_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/joaju"
	"github.com/arandu-io/joaju/ws"
)

// Everything in this file runs against a real [joaju.Server] over a real socket,
// with [joaju.NewMemoryBroker] behind it. A Protocol tested through doubles
// proves that its own switch works; what is worth proving is that a browser
// sending the bytes a browser sends is answered with the bytes a Pusher client
// reads, which is the whole of this type's job.
//
// The names are prefixed for the reason channels_test.go gives: the package is
// one package and another file's helpers share the namespace.

// protocolPolicy is the [joaju.SubscriptionPolicy], and it records what it was
// asked about -- which is how a test asserts that a subscription reached a
// policy at all, and that the policy saw the member the client claimed to be.
type protocolPolicy struct {
	// deny is consulted for every subscription. Nil allows everything.
	deny func(joaju.Subscription) error

	mu    sync.Mutex
	asked []joaju.Subscription
}

func (p *protocolPolicy) Can(_ context.Context, _ auth.Subject, _ auth.Action, s joaju.Subscription) error {
	p.mu.Lock()
	p.asked = append(p.asked, s)
	p.mu.Unlock()

	if p.deny == nil {
		return nil
	}

	return p.deny(s)
}

func (p *protocolPolicy) seen() []joaju.Subscription {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]joaju.Subscription(nil), p.asked...)
}

// protocolObserver keeps the two events the protocol announces.
type protocolObserver struct {
	joaju.NopObserver

	mu      sync.Mutex
	created []string
	removed []string
}

func (o *protocolObserver) ChannelCreated(_ context.Context, name joaju.ChannelName) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.created = append(o.created, name.String())
}

func (o *protocolObserver) ChannelRemoved(_ context.Context, name joaju.ChannelName) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.removed = append(o.removed, name.String())
}

func (o *protocolObserver) counts() (created, removed []string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([]string(nil), o.created...), append([]string(nil), o.removed...)
}

// waitForRemoved waits for a channel to be announced as gone, which happens on
// the reader goroutine of a socket that has closed.
func (o *protocolObserver) waitForRemoved(t *testing.T, name string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, removed := o.counts()
		for _, one := range removed {
			if one == name {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}

	_, removed := o.counts()
	t.Fatalf("the observer was told about %v, want %s among them", removed, name)
}

// protocolFixture is one server speaking the Pusher protocol, and the two values
// a test looks into.
type protocolFixture struct {
	*serverFixture

	policy   *protocolPolicy
	observer *protocolObserver
}

// protocolKeepingBroker keeps what it is asked to remove.
//
// It is a Broker doing what [joaju.Broker.Remove] allows: the call is a request
// and not a promise, and [joaju.NewMemoryBroker] itself refuses it for a channel
// that filled up again. Here it always refuses, so that the case can be asserted
// without racing a subscription against an unsubscription.
type protocolKeepingBroker struct{ joaju.Broker }

func (protocolKeepingBroker) Remove(context.Context, auth.Grant, joaju.ChannelName) error {
	return nil
}

// newProtocolFixture wires the protocol the way an application does: one Broker
// and one Observer, handed to both halves. The server owns the socket and
// announces what happens to it; the protocol owns the frames and announces what
// happens to a channel.
//
// The Broker is the in-memory one unless a test hands over another, because what
// this file is about is the frames a real one produces.
func newProtocolFixture(t *testing.T, cfg joaju.PusherConfig, over ...joaju.Broker) *protocolFixture {
	t.Helper()

	policy := &protocolPolicy{}
	observer := &protocolObserver{}
	cfg.Observer = observer

	var broker joaju.Broker = joaju.NewMemoryBroker()
	if len(over) == 1 {
		broker = over[0]
	}

	return &protocolFixture{
		serverFixture: newServerFixture(t, joaju.ServerConfig{
			Broker:    broker,
			Subscribe: policy,
			Protocol:  joaju.NewPusher(broker, policy, cfg),
			Observer:  observer,
		}),
		policy:   policy,
		observer: observer,
	}
}

// waitForConnections waits until the server holds this many sockets.
//
// It is how a test acts on a socket having gone: closing one from this side
// answers immediately, and what follows happens on the reader goroutine the
// server holds it with.
func (f *protocolFixture) waitForConnections(t *testing.T, n int) {
	t.Helper()

	want := `{"connections":` + strconv.Itoa(n) + `}`
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, body := f.get(t, "/apps/"+serverAppID+"/connections")
		if strings.TrimSpace(string(body)) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the server holds %s, want %s", strings.TrimSpace(string(body)), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// open dials the socket and reads pusher:connection_established off it, so that
// what a test reads afterwards is an answer to something it sent.
func (f *protocolFixture) open(t *testing.T) (*ws.Conn, joaju.SocketID) {
	t.Helper()

	conn, _, err := f.dial(t, "http://"+f.host(t))
	if err != nil {
		t.Fatalf("dialling = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	established := protocolNext(t, conn)
	if established.Event != joaju.EventConnectionEstablished {
		t.Fatalf("the first frame was %s, want %s", established.Event, joaju.EventConnectionEstablished)
	}

	var payload struct {
		SocketID joaju.SocketID `json:"socket_id"`
	}
	if err := json.Unmarshal(established.Data, &payload); err != nil {
		t.Fatalf("decoding %s = %v", established.Data, err)
	}
	if payload.SocketID == "" {
		t.Fatal("the connection was established without a socket id, which is what a client quotes back when it publishes")
	}

	return conn, payload.SocketID
}

// protocolSend writes one frame as a client would.
func protocolSend(t *testing.T, conn *ws.Conn, frame string) {
	t.Helper()

	if err := conn.WriteMessage(ws.TextMessage, []byte(frame)); err != nil {
		t.Fatalf("writing %s = %v", frame, err)
	}
}

// protocolNext reads one frame, and refuses one carrying the tenant.
//
// The second half is RULE 14 asserted on every frame this file reads rather than
// in a test of its own: the client asked about a name with no tenant in it and
// is answered about the same name, and one frame built the wrong way anywhere in
// protocol.go fails here.
func protocolNext(t *testing.T, conn *ws.Conn) joaju.Frame {
	t.Helper()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, message, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("reading a frame = %v", err)
	}
	if strings.Contains(string(message), tenant) {
		t.Fatalf("a frame reached the wire carrying the tenant: %s", message)
	}

	f, err := joaju.Decode(message)
	if err != nil {
		t.Fatalf("the server wrote something that is not a frame: %v (%s)", err, message)
	}

	return f
}

// protocolRefusal reads one pusher:error and answers what it says.
func protocolRefusal(t *testing.T, conn *ws.Conn) (joaju.ErrorCode, string) {
	t.Helper()

	f := protocolNext(t, conn)
	if f.Event != joaju.EventError {
		t.Fatalf("the answer was %s, want %s", f.Event, joaju.EventError)
	}

	var payload struct {
		Code    joaju.ErrorCode `json:"code"`
		Message string          `json:"message"`
	}
	if err := json.Unmarshal(f.Data, &payload); err != nil {
		t.Fatalf("decoding the refusal %s = %v", f.Data, err)
	}

	return payload.Code, payload.Message
}

// protocolBarrier sends a ping and waits for the pong.
//
// It is how this file waits for something that produces no frame of its own --
// an unsubscribe, a refusal that was dropped -- without sleeping: [joaju.Protocol]
// says the frames of one socket are handled in order by one goroutine, so a pong
// is proof that everything sent before it has been acted on.
func protocolBarrier(t *testing.T, conn *ws.Conn) {
	t.Helper()

	protocolSend(t, conn, `{"event":"pusher:ping"}`)
	if f := protocolNext(t, conn); f.Event != joaju.EventPong {
		t.Fatalf("the answer to a ping was %s, want %s", f.Event, joaju.EventPong)
	}
}

// protocolSilence asserts that nothing else arrives, and that the socket is
// still open while it does not.
func protocolSilence(t *testing.T, conn *ws.Conn) {
	t.Helper()

	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, message, err := conn.ReadMessage()
	if err == nil {
		t.Fatalf("the socket was sent %s, want nothing", message)
	}

	var closed *ws.CloseError
	if errors.As(err, &closed) {
		t.Fatalf("the socket was closed: %v", closed)
	}
}

// subscribe sends one subscription and reads its confirmation.
func protocolSubscribe(t *testing.T, conn *ws.Conn, channel string) joaju.Frame {
	t.Helper()

	protocolSend(t, conn, `{"event":"pusher:subscribe","data":{"channel":"`+channel+`"}}`)

	f := protocolNext(t, conn)
	if f.Event != joaju.EventSubscriptionSucceeded {
		t.Fatalf("subscribing to %s answered %s (%s), want %s", channel, f.Event, f.Data, joaju.EventSubscriptionSucceeded)
	}

	return f
}

func TestNewPusherRefusesAWiringItCannotServe(t *testing.T) {
	for _, one := range []struct {
		name      string
		broker    joaju.Broker
		subscribe joaju.SubscriptionPolicy
	}{
		{"no broker", nil, &protocolPolicy{}},
		{"no subscription policy", joaju.NewMemoryBroker(), nil},
	} {
		t.Run(one.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("NewPusher() accepted a wiring with a missing part, and the socket that found out would have found out on a live connection")
				}
			}()

			joaju.NewPusher(one.broker, one.subscribe, joaju.PusherConfig{})
		})
	}
}

func TestPusherEstablishesTheConnectionWithTheSocketIDAndTheActivityTimeout(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{ActivityTimeout: 30 * time.Second})

	conn, _, err := f.dial(t, "http://"+f.host(t))
	if err != nil {
		t.Fatalf("dialling = %v", err)
	}
	defer func() { _ = conn.Close() }()

	established := protocolNext(t, conn)
	if established.Event != joaju.EventConnectionEstablished {
		t.Fatalf("the first frame was %s, want %s", established.Event, joaju.EventConnectionEstablished)
	}

	var payload struct {
		SocketID        string `json:"socket_id"`
		ActivityTimeout int    `json:"activity_timeout"`
	}
	if err := json.Unmarshal(established.Data, &payload); err != nil {
		t.Fatalf("decoding %s = %v", established.Data, err)
	}
	if payload.SocketID == "" {
		t.Fatal("the client was not given its socket id, which is what excludes it from its own broadcast")
	}
	if payload.ActivityTimeout != 30 {
		t.Fatalf("activity_timeout = %d, want 30 -- the client has no other way to know when to ping", payload.ActivityTimeout)
	}
}

// A public channel is authorized like every other one. RULE 17 has no exception
// for reads, and [joaju.ChannelType.Guarded] says whether a policy may allow a
// subscription freely -- never whether one is asked.
func TestPusherAsksThePolicyAboutAPublicChannelToo(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{})
	conn, _ := f.open(t)

	confirmation := protocolSubscribe(t, conn, "orders.17")
	if confirmation.Channel != "orders.17" {
		t.Fatalf("the confirmation named %s, want orders.17", confirmation.Channel)
	}
	if string(confirmation.Data) != "{}" {
		t.Fatalf("the confirmation carried %s, want {} -- only a presence channel has data", confirmation.Data)
	}

	asked := f.policy.seen()
	if len(asked) != 1 {
		t.Fatalf("the policy was asked %d times, want once: a public channel is a read like any other", len(asked))
	}
	if asked[0].Channel.Requested() != "orders.17" || asked[0].Channel.Tenant() != tenant {
		t.Fatalf("the policy was asked about %+v, want orders.17 of %s", asked[0].Channel, tenant)
	}
	if asked[0].Socket == "" {
		t.Fatal("the policy was not told which socket was asking")
	}
}

func TestPusherRefusesASubscriptionThePolicyRefusesAndKeepsTheSocket(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{})
	f.policy.deny = func(s joaju.Subscription) error {
		if strings.HasPrefix(s.Channel.Requested(), "private-") {
			return errors.New("subject u1 does not own order 17")
		}

		return nil
	}

	conn, _ := f.open(t)

	protocolSend(t, conn, `{"event":"pusher:subscribe","data":{"channel":"private-orders.17"}}`)
	code, message := protocolRefusal(t, conn)
	if code != joaju.CodeUnauthorized {
		t.Fatalf("a refused subscription answered %d, want %d", code, joaju.CodeUnauthorized)
	}
	if strings.Contains(message, "order 17") || strings.Contains(message, "u1") {
		t.Fatalf("the refusal disclosed the policy's reason: %q", message)
	}

	// The socket lives, and the channels it can have it still has: a refusal is
	// about one frame.
	confirmation := protocolSubscribe(t, conn, "orders.17")
	if confirmation.Channel != "orders.17" {
		t.Fatalf("after a refusal the socket subscribed to %s, want orders.17", confirmation.Channel)
	}
}

func TestPusherKeepsTheChannelsItHasAfterARefusedFrame(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{})
	conn, _ := f.open(t)

	protocolSubscribe(t, conn, "orders.17")

	protocolSend(t, conn, `{"event":"pusher:nonsense"}`)
	if code, _ := protocolRefusal(t, conn); code != joaju.CodeInvalidMessage {
		t.Fatalf("an unknown event answered %d, want %d", code, joaju.CodeInvalidMessage)
	}

	if status, body := f.post(t, "/apps/"+serverAppID+"/events",
		`{"name":"OrderShipped","channel":"orders.17","data":"{\"id\":17}"}`); status != http.StatusOK {
		t.Fatalf("publishing answered %d: %s", status, body)
	}

	delivered := protocolNext(t, conn)
	if delivered.Event != "OrderShipped" || delivered.Channel != "orders.17" {
		t.Fatalf("the socket received %+v, want the event on the channel it kept", delivered)
	}
	if string(delivered.Data) != `{"id":17}` {
		t.Fatalf("the event carried %s, want {\"id\":17}", delivered.Data)
	}
}

// The two frames a client may not send, and the reason the second one matters: a
// client able to send pusher_internal:member_added is a client able to invent
// the members of a presence channel it is on.
func TestPusherRefusesWhatOnlyTheServerMaySend(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{})
	conn, _ := f.open(t)

	for _, frame := range []string{
		`{"event":"pusher_internal:member_added","channel":"presence-room.1","data":"{\"user_id\":\"nobody\"}"}`,
		`{"event":"pusher:connection_established","data":"{\"socket_id\":\"1.1\"}"}`,
		`{"event":"OrderShipped","channel":"orders.17","data":"{}"}`,
		`{`,
	} {
		protocolSend(t, conn, frame)
		if code, _ := protocolRefusal(t, conn); code != joaju.CodeInvalidMessage {
			t.Fatalf("%s answered %d, want %d", frame, code, joaju.CodeInvalidMessage)
		}
	}

	// None of them reached a policy, so none of them reached a channel.
	if asked := f.policy.seen(); len(asked) != 0 {
		t.Fatalf("the policy was asked about %v, want nothing", asked)
	}
}

func TestPusherRefusesAChannelNameThatCarriesATenant(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{})
	conn, _ := f.open(t)

	protocolSend(t, conn, `{"event":"pusher:subscribe","data":{"channel":"`+tenant+`:private-orders.17"}}`)
	if code, _ := protocolRefusal(t, conn); code != joaju.CodeInvalidMessage {
		t.Fatalf("a channel name carrying a tenant answered %d, want %d: naming a tenant is choosing whose events you hear", code, joaju.CodeInvalidMessage)
	}
	if asked := f.policy.seen(); len(asked) != 0 {
		t.Fatalf("the policy was asked about %v, want nothing: the name was refused before it existed", asked)
	}
}

func TestPusherAnswersAPingAndSaysNothingToAPong(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{})
	conn, _ := f.open(t)

	protocolBarrier(t, conn)

	// A pong is the client saying it is there, and the deadline that proves it
	// was reset by the server before the protocol saw the frame.
	protocolSend(t, conn, `{"event":"pusher:pong"}`)
	protocolSilence(t, conn)
}

// The presence channel end to end: the list a subscriber is given includes
// itself, the others are told it arrived, and they are told when it goes.
func TestPusherPublishesTheMembersOfAPresenceChannelToEachOther(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{})

	first, _ := f.open(t)
	protocolSend(t, first, `{"event":"pusher:subscribe","data":{"channel":"presence-room.1","channel_data":"{\"user_id\":\"u1\",\"user_info\":{\"name\":\"Ana\"}}"}}`)

	confirmation := protocolNext(t, first)
	if confirmation.Event != joaju.EventSubscriptionSucceeded {
		t.Fatalf("subscribing answered %s (%s)", confirmation.Event, confirmation.Data)
	}
	if got := protocolMembers(t, confirmation); len(got) != 1 || got[0] != "u1" {
		t.Fatalf("the first subscriber was given %v, want itself in the list", got)
	}

	second, _ := f.open(t)
	protocolSend(t, second, `{"event":"pusher:subscribe","data":{"channel":"presence-room.1","channel_data":"{\"user_id\":\"u2\"}"}}`)

	joined := protocolNext(t, second)
	if got := protocolMembers(t, joined); len(got) != 2 || got[0] != "u1" || got[1] != "u2" {
		t.Fatalf("the second subscriber was given %v, want both members", got)
	}

	added := protocolNext(t, first)
	if added.Event != joaju.EventMemberAdded || added.Channel != "presence-room.1" {
		t.Fatalf("the first subscriber was told %+v, want %s", added, joaju.EventMemberAdded)
	}
	if !strings.Contains(string(added.Data), `"user_id":"u2"`) {
		t.Fatalf("the arrival carried %s, want the member that arrived", added.Data)
	}

	// The member is a claim the client made, and the policy is what compares it
	// against the subject. It cannot do that if it is never shown it.
	asked := f.policy.seen()
	if len(asked) != 2 || asked[1].Member.UserID != "u2" {
		t.Fatalf("the policy was asked about %+v, want the member the second client claimed to be", asked)
	}

	_ = second.Close()

	removed := protocolNext(t, first)
	if removed.Event != joaju.EventMemberRemoved {
		t.Fatalf("after a socket closed the others were told %+v, want %s", removed, joaju.EventMemberRemoved)
	}
	if string(removed.Data) != `{"user_id":"u2"}` {
		t.Fatalf("the departure carried %s, want the user_id alone", removed.Data)
	}
}

// protocolMembers is the ids in the presence block of a subscription
// confirmation.
func protocolMembers(t *testing.T, f joaju.Frame) []string {
	t.Helper()

	var payload struct {
		Presence struct {
			Count int      `json:"count"`
			IDs   []string `json:"ids"`
		} `json:"presence"`
	}
	if err := json.Unmarshal(f.Data, &payload); err != nil {
		t.Fatalf("decoding the presence block %s = %v", f.Data, err)
	}
	if payload.Presence.Count != len(payload.Presence.IDs) {
		t.Fatalf("the presence block counted %d and listed %v", payload.Presence.Count, payload.Presence.IDs)
	}

	return payload.Presence.IDs
}

// A cache channel says out loud that it has nothing yet, and replays what it has
// to whoever arrives next.
func TestPusherReplaysACacheChannelAndSaysWhenThereIsNothingToReplay(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{})

	first, _ := f.open(t)
	protocolSend(t, first, `{"event":"pusher:subscribe","data":{"channel":"cache-prices"}}`)

	// The replay belongs to the channel and the confirmation to the protocol, so
	// the miss arrives first. A client that bound its handlers before
	// subscribing -- which is how a client subscribes -- reads both.
	missed := protocolNext(t, first)
	if missed.Event != joaju.EventCacheMiss || missed.Channel != "cache-prices" {
		t.Fatalf("the first subscriber was told %+v, want %s", missed, joaju.EventCacheMiss)
	}
	if confirmation := protocolNext(t, first); confirmation.Event != joaju.EventSubscriptionSucceeded {
		t.Fatalf("after the miss came %s, want %s", confirmation.Event, joaju.EventSubscriptionSucceeded)
	}

	if status, body := f.post(t, "/apps/"+serverAppID+"/events",
		`{"name":"prices.updated","channel":"cache-prices","data":"{\"eur\":1}"}`); status != http.StatusOK {
		t.Fatalf("publishing answered %d: %s", status, body)
	}
	if delivered := protocolNext(t, first); delivered.Event != "prices.updated" {
		t.Fatalf("the subscriber received %+v, want the event that was published", delivered)
	}

	second, _ := f.open(t)
	protocolSend(t, second, `{"event":"pusher:subscribe","data":{"channel":"cache-prices"}}`)

	replayed := protocolNext(t, second)
	if replayed.Event != "prices.updated" || string(replayed.Data) != `{"eur":1}` {
		t.Fatalf("the second subscriber was given %+v, want the last event replayed", replayed)
	}
	if confirmation := protocolNext(t, second); confirmation.Event != joaju.EventSubscriptionSucceeded {
		t.Fatalf("after the replay came %s, want %s", confirmation.Event, joaju.EventSubscriptionSucceeded)
	}

	// The replay went to whoever arrived and to nobody else.
	protocolSilence(t, first)
}

func TestPusherRefusesAClientEventWhenTheyAreOff(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{})
	conn, _ := f.open(t)

	protocolSubscribe(t, conn, "private-room.1")

	protocolSend(t, conn, `{"event":"client-typing","channel":"private-room.1","data":"{\"at\":1}"}`)
	code, message := protocolRefusal(t, conn)
	if code != joaju.CodeRateLimited {
		t.Fatalf("a client event on a server that does not relay them answered %d, want %d", code, joaju.CodeRateLimited)
	}
	if message != joaju.ErrClientEventsDisabled.Message {
		t.Fatalf("the refusal said %q, want %q", message, joaju.ErrClientEventsDisabled.Message)
	}
}

func TestPusherRefusesAClientEventOnAChannelNoPolicyGuarded(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{ClientEvents: joaju.ClientEventsOn})
	conn, _ := f.open(t)

	protocolSubscribe(t, conn, "room.1")

	protocolSend(t, conn, `{"event":"client-typing","channel":"room.1","data":"{\"at\":1}"}`)
	code, message := protocolRefusal(t, conn)
	if code != joaju.CodeUnauthorized {
		t.Fatalf("a client event on a public channel answered %d, want %d", code, joaju.CodeUnauthorized)
	}
	if message != joaju.ErrClientEventChannel.Message {
		t.Fatalf("the refusal said %q, want %q", message, joaju.ErrClientEventChannel.Message)
	}
}

func TestPusherRefusesAClientEventFromSomebodyWhoIsNotOnTheChannel(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{ClientEvents: joaju.ClientEventsOn})

	// Somebody is on the channel, so what is refused is this sender and not the
	// absence of the channel.
	member, _ := f.open(t)
	protocolSubscribe(t, member, "private-room.1")

	stranger, _ := f.open(t)
	protocolSend(t, stranger, `{"event":"client-typing","channel":"private-room.1","data":"{\"at\":1}"}`)

	code, message := protocolRefusal(t, stranger)
	if code != joaju.CodeUnauthorized {
		t.Fatalf("a client event from somebody not on the channel answered %d, want %d", code, joaju.CodeUnauthorized)
	}
	if message != joaju.ErrNotSubscribed.Message {
		t.Fatalf("the refusal said %q, want %q", message, joaju.ErrNotSubscribed.Message)
	}

	// And nothing was relayed to the people who are on it.
	protocolSilence(t, member)
}

func TestPusherRelaysAClientEventToTheOthersAndNotToTheSender(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{ClientEvents: joaju.ClientEventsOn})

	sender, _ := f.open(t)
	protocolSubscribe(t, sender, "private-room.1")

	other, _ := f.open(t)
	protocolSubscribe(t, other, "private-room.1")

	protocolSend(t, sender, `{"event":"client-typing","channel":"private-room.1","data":"{\"at\":1}"}`)

	relayed := protocolNext(t, other)
	if relayed.Event != "client-typing" || relayed.Channel != "private-room.1" {
		t.Fatalf("the other subscriber received %+v, want the client event", relayed)
	}
	if string(relayed.Data) != `{"at":1}` {
		t.Fatalf("the client event carried %s, want the payload it was sent with", relayed.Data)
	}

	// The sender already drew its own message. Delivering it back would draw it
	// twice.
	protocolSilence(t, sender)
}

func TestPusherLeavesTheChannelWhenTheClientUnsubscribes(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{})
	conn, _ := f.open(t)

	protocolSubscribe(t, conn, "orders.17")

	protocolSend(t, conn, `{"event":"pusher:unsubscribe","data":{"channel":"orders.17"}}`)
	// Nothing answers an unsubscribe, so the pong is what says it has happened.
	protocolBarrier(t, conn)

	if status, body := f.post(t, "/apps/"+serverAppID+"/events",
		`{"name":"OrderShipped","channel":"orders.17","data":"{}"}`); status != http.StatusOK {
		t.Fatalf("publishing answered %d: %s", status, body)
	}
	protocolSilence(t, conn)

	// The last subscriber left, so the channel went with it.
	created, removed := f.observer.counts()
	if len(created) != 1 || created[0] != tenant+":orders.17" {
		t.Fatalf("the observer was told of %v created, want the one channel", created)
	}
	if len(removed) != 1 || removed[0] != tenant+":orders.17" {
		t.Fatalf("the observer was told of %v removed, want the one channel", removed)
	}

	status, body := f.get(t, "/apps/"+serverAppID+"/channels")
	if status != http.StatusOK {
		t.Fatalf("listing channels answered %d", status)
	}
	if strings.Contains(string(body), "orders.17") {
		t.Fatalf("the channel list still holds a channel nobody is on: %s", body)
	}
}

// A channel is announced as gone only when it is gone.
//
// [joaju.Broker.Remove] is a request and not a promise -- an implementation keeps
// a channel that filled up again between the caller deciding it was empty and
// the call arriving -- so the answer comes from asking afterwards and never from
// the call having returned no error. A count that goes down while the thing it
// counts stays is a dashboard that drifts, one channel at a time, until it is
// worth nothing.
func TestPusherAnnouncesAChannelGoneOnlyWhenTheBrokerDroppedIt(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{}, protocolKeepingBroker{joaju.NewMemoryBroker()})
	conn, _ := f.open(t)

	protocolSubscribe(t, conn, "orders.17")

	protocolSend(t, conn, `{"event":"pusher:unsubscribe","data":{"channel":"orders.17"}}`)
	protocolBarrier(t, conn)

	created, removed := f.observer.counts()
	if len(created) != 1 {
		t.Fatalf("the observer was told of %v created, want the one channel", created)
	}
	if len(removed) != 0 {
		t.Fatalf("the observer was told %v went, and the broker still holds them", removed)
	}
}

// Unsubscribing from a channel a socket is not on is not an error: it is a
// reconnect that raced its own cleanup.
func TestPusherIgnoresAnUnsubscribeFromAChannelItIsNotOn(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{})
	conn, _ := f.open(t)

	protocolSend(t, conn, `{"event":"pusher:unsubscribe","data":{"channel":"orders.17"}}`)
	protocolBarrier(t, conn)

	if created, removed := f.observer.counts(); len(created) != 0 || len(removed) != 0 {
		t.Fatalf("the observer was told of %v created and %v removed, want nothing", created, removed)
	}
}

func TestPusherTakesASocketOffItsChannelsWhenItCloses(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{})
	conn, _ := f.open(t)

	protocolSubscribe(t, conn, "orders.17")
	protocolSubscribe(t, conn, "invoices.9")

	_ = conn.Close()

	f.observer.waitForRemoved(t, tenant+":orders.17")
	f.observer.waitForRemoved(t, tenant+":invoices.9")

	status, body := f.get(t, "/apps/"+serverAppID+"/channels")
	if status != http.StatusOK {
		t.Fatalf("listing channels answered %d", status)
	}
	if strings.Contains(string(body), "orders.17") || strings.Contains(string(body), "invoices.9") {
		t.Fatalf("a closed socket left its channels behind: %s", body)
	}
}

// A channel exists once however many sockets ask for it, and it is announced
// once. [joaju.Broker.FindOrCreate] does not say whether it created anything, and
// a count that goes up twice and down once is a dashboard that never returns to
// zero.
func TestPusherAnnouncesAChannelOnceHoweverManySocketsSubscribe(t *testing.T) {
	f := newProtocolFixture(t, joaju.PusherConfig{})

	first, _ := f.open(t)
	protocolSubscribe(t, first, "orders.17")

	second, _ := f.open(t)
	protocolSubscribe(t, second, "orders.17")

	// A resubscription is not a second channel either.
	protocolSubscribe(t, first, "orders.17")

	created, removed := f.observer.counts()
	if len(created) != 1 || created[0] != tenant+":orders.17" {
		t.Fatalf("the observer was told of %v, want one channel created", created)
	}
	if len(removed) != 0 {
		t.Fatalf("the observer was told of %v removed, want nothing: both sockets are still on it", removed)
	}

	// And the channel is not dropped while somebody is still on it. If it were,
	// the publish below would find no channel here and the event would reach
	// nobody.
	_ = second.Close()
	f.waitForConnections(t, 1)

	if status, body := f.post(t, "/apps/"+serverAppID+"/events",
		`{"name":"OrderShipped","channel":"orders.17","data":"{}"}`); status != http.StatusOK {
		t.Fatalf("publishing answered %d: %s", status, body)
	}
	if delivered := protocolNext(t, first); delivered.Event != "OrderShipped" {
		t.Fatalf("the remaining subscriber received %+v, want the event", delivered)
	}
	if _, removed := f.observer.counts(); len(removed) != 0 {
		t.Fatalf("the observer was told of %v removed, want nothing: one of the two sockets is still on it", removed)
	}
}
