package pusher

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
	"github.com/arandu-io/joaju"
)

// fuzzMaxMessages is how many frames one input may carry.
//
// The frames are read one after another on a socket's own goroutine, so the
// interesting inputs are the short sequences where one frame changes what the
// next one means -- subscribing and then publishing, or leaving twice. A cap
// keeps a mutation that is mostly separators from spending the budget on the
// same empty frame.
const fuzzMaxMessages = 64

// fuzzSink keeps what the protocol wrote, so a target can read the frames a
// client would have received.
type fuzzSink struct {
	mu       sync.Mutex
	messages [][]byte
}

func (s *fuzzSink) Send(_ context.Context, message []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, append([]byte(nil), message...))

	return nil
}

func (s *fuzzSink) Terminate(context.Context) error { return nil }

func (s *fuzzSink) written() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([][]byte(nil), s.messages...)
}

// fuzzRefusals is every sentence this package sends a client, and it is a closed
// set.
//
// A refusal reaches the client as a code and a fixed message; the reason a
// policy wrote names the subject and the channel and belongs in the log. A
// message outside this set on the wire is a cause that was disclosed.
var fuzzRefusals = map[string]bool{
	ErrOverQuota.Message:            true,
	ErrUnauthorized.Message:         true,
	ErrOriginNotAllowed.Message:     true,
	ErrNotSubscribed.Message:        true,
	ErrClientEventChannel.Message:   true,
	ErrInvalidMessage.Message:       true,
	ErrRateLimited.Message:          true,
	ErrClientEventsDisabled.Message: true,
}

// fuzzConnection is one socket held by a subject of [fuzzTenant].
func fuzzConnection(t testing.TB) (*joaju.Connection, *fuzzSink) {
	t.Helper()

	g, err := auth.Authorize(context.Background(), channelTestConnectPolicy{},
		auth.Subject{ID: "bruno", Tenant: fuzzTenant}, joaju.Connect, joaju.Handshake{Socket: "7.1"})
	if err != nil {
		t.Fatalf("authorizing the handshake: %v", err)
	}

	sink := &fuzzSink{}
	conn, err := joaju.NewConnection(g, "7.1", sink)
	if err != nil {
		t.Fatalf("opening the socket: %v", err)
	}

	return conn, sink
}

// FuzzProtocolMessage runs whole sequences of arbitrary frames through the
// protocol, on a live broker, and then closes the socket.
//
// A frame that is read is matched, answered and recorded, so what the codec
// accepts becomes state shared with every other socket in the process: a seat,
// a channel in the [joaju.Broker], and a member list other subscribers read. The
// assertions are about what a client may cause and about what it is left with.
// Nothing here may panic, everything written back has to be a frame a client can
// read, no frame may name the tenant it was scoped to, no refusal may carry the
// reason it was refused, and a socket that closed has to leave nothing behind.
//
// The input is one message per newline, which no frame can contain unescaped.
// A permissive [joaju.SubscriptionPolicy] and [ClientEventsOn] are the widest
// configuration this package offers, so the paths the checks below guard are all
// reachable.
func FuzzProtocolMessage(f *testing.F) {
	f.Add([]byte(`{"event":"pusher:ping"}`))
	f.Add([]byte(`{"event":"pusher:pong"}`))
	f.Add([]byte(`{"event":"pusher:subscribe","data":{"channel":"orders"}}`))
	f.Add([]byte(`{"event":"pusher:subscribe","data":{"channel":"private-orders"}}` + "\n" +
		`{"event":"client-typing","channel":"private-orders","data":"{}"}`))
	f.Add([]byte(`{"event":"pusher:subscribe","data":{"channel":"presence-room","channel_data":"{\"user_id\":\"u1\"}"}}` + "\n" +
		`{"event":"client-typing","channel":"presence-room","data":"{}","user_id":"impostor"}`))
	// Publishing on a channel the socket never reached, which must not read the
	// seat it does not hold.
	f.Add([]byte(`{"event":"client-typing","channel":"private-orders","data":"{}"}`))
	// Leaving twice, and leaving a channel that was never joined.
	f.Add([]byte(`{"event":"pusher:subscribe","data":{"channel":"private-a"}}` + "\n" +
		`{"event":"pusher:unsubscribe","data":{"channel":"private-a"}}` + "\n" +
		`{"event":"pusher:unsubscribe","data":{"channel":"private-a"}}`))
	// Subscribing to the same channel twice, which is a reconnect racing itself.
	f.Add([]byte(`{"event":"pusher:subscribe","data":{"channel":"cache-q"}}` + "\n" +
		`{"event":"pusher:subscribe","data":{"channel":"cache-q"}}`))
	// A presence channel with no member, which the channel refuses after the
	// protocol has already reached for it.
	f.Add([]byte(`{"event":"pusher:subscribe","data":{"channel":"presence-room"}}`))
	// The events only a server may send.
	f.Add([]byte(`{"event":"pusher_internal:member_added","channel":"presence-room","data":"{}"}`))
	f.Add([]byte("{}\n\n{"))

	f.Fuzz(func(t *testing.T, data []byte) {
		ctx := context.Background()
		broker := NewMemoryBroker()
		protocol := NewPusher(broker, channelTestJoinPolicy{}, PusherConfig{ClientEvents: ClientEventsOn})
		conn, sink := fuzzConnection(t)

		if err := protocol.Open(ctx, conn); err != nil {
			t.Fatalf("opening the connection = %v", err)
		}

		messages := bytes.Split(data, []byte{'\n'})
		if len(messages) > fuzzMaxMessages {
			messages = messages[:fuzzMaxMessages]
		}
		for _, message := range messages {
			// The error is the caller's to log. A refused frame is not a refused
			// socket, and the server reads the next one.
			_ = protocol.Message(ctx, conn, message)
		}

		for _, written := range sink.written() {
			frame, err := Decode(written)
			if err != nil {
				t.Fatalf("the server wrote %q, which it cannot read back: %v", written, err)
			}
			if strings.Contains(frame.Channel, broadcasting.TenantSeparator) {
				t.Fatalf("the server wrote channel %q, which names a tenant", frame.Channel)
			}
			if frame.Event != joaju.EventError {
				continue
			}

			var refusal struct {
				Code    ErrorCode `json:"code"`
				Message string    `json:"message"`
			}
			if err := json.Unmarshal(frame.Data, &refusal); err != nil {
				t.Fatalf("the server refused with %q, which is not an error frame: %v", frame.Data, err)
			}
			if !fuzzRefusals[refusal.Message] {
				t.Fatalf("the server told the client %q, which is not one of its refusals", refusal.Message)
			}
		}

		// A socket that closed leaves nothing behind: not a seat, and not a
		// channel it was the last one on. Whatever a client can make this record
		// it can make it record again, so a name that outlives its socket is
		// memory a connection bought and never gave back.
		protocol.Close(ctx, conn)

		if seats := len(protocol.(*pusher).seats); seats != 0 {
			t.Fatalf("a closed socket left %d seats behind", seats)
		}

		held, err := broker.All(ctx, fuzzJoinGrant(t, "reader"))
		if err != nil {
			t.Fatalf("listing the channels of %s = %v", fuzzTenant, err)
		}
		if len(held) != 0 {
			names := make([]string, 0, len(held))
			for _, c := range held {
				names = append(names, c.Name().Requested())
			}
			t.Fatalf("a closed socket left the channels %v behind", names)
		}
	})
}
