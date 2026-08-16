package joaju

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/arandu-io/hesape/auth"
)

// EventCacheMiss tells a subscriber of a cache channel that there is nothing to
// replay yet, so it stops waiting for the event it would have been sent.
//
// It is the one protocol event name [joaju] does not already declare, because
// it belongs to the cache channels and those exist only here, in the wire
// format.
const EventCacheMiss = ProtocolPrefix + "cache_miss"

// ClientEventPrefix is the namespace of an event a client published rather than
// received: "client-".
//
// It is the third reserved prefix and the only one that travels inwards.
// [ProtocolPrefix] and [InternalPrefix] name what the server says; this one
// names what a browser says to every other browser on the channel, which is why
// [ClientEvents] exists and why it is off in its zero value.
const ClientEventPrefix = "client-"

// ErrorCode is a code from Pusher's error table, sent in the data of
// [EventError].
//
// The number is the contract, not the message: an existing client branches on
// the code and only prints the text. The ranges are Pusher's, and a client
// reads them without a table -- 4000 to 4099 means do not come back, 4100 to
// 4199 means come back after a backoff, 4200 to 4299 means come back now.
type ErrorCode int

// The codes this server sends. The numbers are the protocol's, so a client that
// already branches on them branches on these.
const (
	// CodeOverQuota is 4004: too many sockets are open.
	CodeOverQuota ErrorCode = 4004
	// CodeUnauthorized is 4009: the client may not do that. Every refusal a
	// Policy produces arrives here.
	CodeUnauthorized ErrorCode = 4009
	// CodeOverCapacity is 4100: come back later, with a backoff.
	CodeOverCapacity ErrorCode = 4100
	// CodeInvalidMessage is 4200: the frame was not something this server could
	// read. It is the code for everything that is not one of the others.
	CodeInvalidMessage ErrorCode = 4200
	// CodeRateLimited is 4301: the client is sending too fast, or asked for
	// something this server does not offer it.
	CodeRateLimited ErrorCode = 4301
)

// ProtocolError is a refusal expressed in the protocol's own terms.
//
// It carries only a code and a fixed sentence, and that is deliberate: it is
// the one error in this package whose text reaches a client, so it never
// contains the reason. A Policy refuses a subscription with a sentence naming
// the subject and the resource, and that sentence belongs in the log; what the
// client is told is 4009.
//
// The values are comparable, so the ones declared below can be compared with
// errors.Is even after being wrapped.
type ProtocolError struct {
	// Code is the number the client branches on.
	Code ErrorCode
	// Message is the sentence the client may print. It is fixed per code.
	Message string
}

// Error makes a [ProtocolError] an error.
func (e ProtocolError) Error() string {
	return fmt.Sprintf("joaju: pusher error %d: %s", e.Code, e.Message)
}

// Frame is the [EventError] frame that carries this refusal to the client.
func (e ProtocolError) Frame() Frame {
	// A struct of an int and a string has no encoding that can fail.
	data, _ := json.Marshal(struct {
		Code    ErrorCode `json:"code"`
		Message string    `json:"message"`
	}{Code: e.Code, Message: e.Message})

	return Frame{Event: EventError, Data: data}
}

// The refusals this package sends, with the wording the protocol's clients
// already carry in their own message tables.
var (
	// ErrOverQuota is 4004, and is what a server past its connection limit
	// answers a new socket.
	ErrOverQuota = ProtocolError{Code: CodeOverQuota, Message: "Application is over connection quota"}
	// ErrUnauthorized is 4009, and is what every refusal from a
	// [ConnectPolicy] or a [SubscriptionPolicy] becomes on the wire.
	ErrUnauthorized = ProtocolError{Code: CodeUnauthorized, Message: "Connection is unauthorized"}
	// ErrOriginNotAllowed is 4009: the Origin the browser sent is one a
	// [ConnectPolicy] refused. See [Handshake].
	ErrOriginNotAllowed = ProtocolError{Code: CodeUnauthorized, Message: "Origin not allowed"}
	// ErrNotSubscribed is 4009: a client tried to publish on a channel it is
	// not on.
	ErrNotSubscribed = ProtocolError{Code: CodeUnauthorized, Message: "The client is not a member of the specified channel."}
	// ErrClientEventChannel is 4009: client events exist only on the channels a
	// [SubscriptionPolicy] guarded. See [ClientEvents].
	ErrClientEventChannel = ProtocolError{Code: CodeUnauthorized, Message: "Client event rejected - only supported on private and presence channels"}
	// ErrInvalidMessage is 4200, and is what anything unreadable becomes. It is
	// also the answer to an unknown event name.
	ErrInvalidMessage = ProtocolError{Code: CodeInvalidMessage, Message: "Invalid message format"}
	// ErrRateLimited is 4301: the client is sending faster than it may.
	ErrRateLimited = ProtocolError{Code: CodeRateLimited, Message: "Rate limit exceeded"}
	// ErrClientEventsDisabled is 4301: this server does not relay client
	// events, which is the default. See [ClientEvents].
	ErrClientEventsDisabled = ProtocolError{Code: CodeRateLimited, Message: "The app does not have client messaging enabled."}
)

// ErrorFrame is the frame a client is sent about any error at all.
//
// It is the only translation from an internal error to something a client
// reads, and it exists so there is one: a caller that built the frame itself
// would be a caller choosing how much of the cause to disclose, and it takes
// one such caller to send a policy's refusal -- which names the subject, the
// action and the resource -- to the browser that was refused.
//
// A [ProtocolError] keeps its own code. An auth.ErrForbidden from any depth,
// which is what a [ConnectPolicy] or a [SubscriptionPolicy] produces, becomes
// [ErrUnauthorized]. Everything else becomes [ErrInvalidMessage], and its text
// is dropped. The caller logs err.
func ErrorFrame(err error) Frame {
	var pe ProtocolError
	if errors.As(err, &pe) {
		return pe.Frame()
	}
	if errors.Is(err, auth.ErrForbidden) {
		return ErrUnauthorized.Frame()
	}

	return ErrInvalidMessage.Frame()
}

// Frame is one Pusher message, in both directions.
//
// The envelope is three fields, and the protocol's oddity is in the middle one:
//
//	{"event":"pusher:connection_established","data":"{\"socket_id\":\"7.1\"}"}
//
// data is a JSON string that contains JSON. It is not a nesting anybody would
// choose, and it is not ours to change -- every Pusher client parses the string
// a second time, so a frame that put an object there is a frame they cannot
// read. [Frame.Data] holds the inner value decoded, and the encoding happens
// once, here, so that no caller has to remember which side of it they are on.
//
// The frame's Channel is a plain string and not a [ChannelName], because that
// is what the field is: on the way in it is what the client asked for, and on
// the way out it is [ChannelName.Requested]. The constructors below do the
// conversion, and they are the reason no code path can write
// [ChannelName.String] -- with the tenant in front of it -- into a frame.
//
// The zero value is not a frame: encoding one without an event name fails
// rather than emitting {}.
type Frame struct {
	// Event is the event name, and is required. It is one of the [ProtocolPrefix]
	// names, one of the [InternalPrefix] names, an application event, or a
	// [ClientEventPrefix] one.
	Event string
	// Channel is the channel the frame is about, with no tenant in it. It is
	// absent on the frames that are about the socket rather than a channel:
	// [EventConnectionEstablished], [EventPing], [EventPong] and most
	// [EventError] frames.
	Channel string
	// Data is the payload, decoded. It is encoded as a JSON string on the way
	// out and decoded from one on the way in.
	Data json.RawMessage
	// UserID is the sender of a relayed client event, and is empty on every
	// other frame. It is Pusher's user_id, top level and beside the channel
	// rather than inside the data.
	UserID string
}

// MarshalJSON writes the envelope, with [Frame.Data] encoded as the protocol's
// string-containing-JSON.
//
// It is a method rather than a function so that the encoding cannot be
// sidestepped: json.Marshal of a Frame, anywhere, produces the same bytes
// [Encode] does.
func (f Frame) MarshalJSON() ([]byte, error) {
	if f.Event == "" {
		return nil, errors.New("joaju: a frame with no event name is not a frame")
	}

	// data is a pointer so that omitempty can tell "no data" from "the empty
	// string", which are different frames: pusher:pong carries no data field at
	// all.
	wire := struct {
		Event   string  `json:"event"`
		Data    *string `json:"data,omitempty"`
		Channel string  `json:"channel,omitempty"`
		UserID  string  `json:"user_id,omitempty"`
	}{Event: f.Event, Channel: f.Channel, UserID: f.UserID}

	if len(f.Data) > 0 {
		// Compacting is what makes the output of a given frame one sequence of
		// bytes rather than however the caller happened to indent it.
		var buf bytes.Buffer
		if err := json.Compact(&buf, f.Data); err != nil {
			return nil, fmt.Errorf("joaju: the data of a %s frame is not JSON: %w", f.Event, err)
		}
		encoded := buf.String()
		wire.Data = &encoded
	}

	return json.Marshal(wire)
}

// UnmarshalJSON reads the envelope, undoing the double encoding of the data
// field when it finds it.
//
// The field arrives either way and both are legal: pusher-js sends the data of
// pusher:subscribe as an object, and sends the data of a client event as a
// string containing JSON. A string whose contents are not JSON stays the string
// it is, which means a client that sends the data "123" is heard as the number
// 123. That ambiguity is the protocol's, and matching it is the point.
func (f *Frame) UnmarshalJSON(b []byte) error {
	var wire struct {
		Event   string          `json:"event"`
		Data    json.RawMessage `json:"data"`
		Channel string          `json:"channel"`
		UserID  string          `json:"user_id"`
	}
	if err := json.Unmarshal(b, &wire); err != nil {
		return err
	}

	*f = Frame{
		Event:   wire.Event,
		Channel: wire.Channel,
		Data:    unwrapJSONString(wire.Data),
		UserID:  wire.UserID,
	}

	return nil
}

// unwrapJSONString takes one layer of JSON string off a value that has one, and
// leaves everything else alone. It is the reading half of the double encoding,
// and it is one function because the frame's data and a subscription's
// channel_data both arrive that way.
func unwrapJSONString(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	if raw[0] != '"' {
		return raw
	}

	var inner string
	if err := json.Unmarshal(raw, &inner); err != nil {
		return raw
	}
	if !json.Valid([]byte(inner)) {
		return raw
	}

	return json.RawMessage(inner)
}

// Encode is the bytes of one frame, ready to be written to a socket.
func Encode(f Frame) ([]byte, error) { return json.Marshal(f) }

// Decode reads one frame off a socket.
//
// It refuses a frame with no event name, which json.Unmarshal alone would
// accept, and it refuses it as [ErrInvalidMessage] -- so the caller can hand
// the error straight to [ErrorFrame] and the client is told 4200 rather than
// the parser's complaint about byte 31.
func Decode(message []byte) (Frame, error) {
	var f Frame
	if err := json.Unmarshal(message, &f); err != nil {
		return Frame{}, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	if f.Event == "" {
		return Frame{}, fmt.Errorf("%w: the frame named no event", ErrInvalidMessage)
	}

	return f, nil
}

// IsProtocol reports whether this is one of the protocol's own events, the
// "pusher:" ones.
func (f Frame) IsProtocol() bool { return strings.HasPrefix(f.Event, ProtocolPrefix) }

// IsInternal reports whether this is one of the events only the server may
// send, the "pusher_internal:" ones.
func (f Frame) IsInternal() bool { return strings.HasPrefix(f.Event, InternalPrefix) }

// IsClientEvent reports whether a client published this, rather than the
// server. See [ClientEvents].
func (f Frame) IsClientEvent() bool { return strings.HasPrefix(f.Event, ClientEventPrefix) }

// ClientMaySend is the refusal, if any, of a frame that arrived from a client.
//
// It is the whole list of what a socket may say, and it is a closed one:
//
//   - pusher:subscribe, pusher:unsubscribe, pusher:ping and pusher:pong;
//   - anything beginning with client-, which [ClientEvents] then decides about.
//
// Everything else is [ErrInvalidMessage]. That includes the "pusher_internal:"
// events, and refusing them is the reason this function exists rather than the
// server switching on the name: pusher_internal:member_added is the frame that
// tells a presence channel who joined, and a client able to send one is a
// client able to invent members of a channel it is on. It also includes an
// application event with no prefix at all, which is published over the HTTP
// route by something holding the credentials, never over a socket by a browser.
func (f Frame) ClientMaySend() error {
	if f.IsClientEvent() {
		return nil
	}

	switch f.Event {
	case EventSubscribe, EventUnsubscribe, EventPing, EventPong:
		return nil
	}

	return fmt.Errorf("%w: %s is not an event a client may send", ErrInvalidMessage, f.Event)
}

// ConnectionEstablished is the first frame the server sends, and it hands the
// client the socket id it will quote back when it publishes.
//
// activityTimeout is how long the client may stay silent before it should ping,
// and it goes out in seconds because the protocol counts in seconds. A duration
// under a second is sent as one: zero would tell the client to ping
// immediately. A zero or negative duration omits the field, and the client
// falls back to its own default.
func ConnectionEstablished(id SocketID, activityTimeout time.Duration) Frame {
	payload := struct {
		SocketID        SocketID `json:"socket_id"`
		ActivityTimeout int      `json:"activity_timeout,omitempty"`
	}{SocketID: id}

	if activityTimeout > 0 {
		payload.ActivityTimeout = int(activityTimeout.Round(time.Second) / time.Second)
		if payload.ActivityTimeout == 0 {
			payload.ActivityTimeout = 1
		}
	}

	// A string and an int have no encoding that can fail.
	data, _ := json.Marshal(payload)

	return Frame{Event: EventConnectionEstablished, Data: data}
}

// Ping is the server asking a silent socket whether it is still there.
func Ping() Frame { return Frame{Event: EventPing} }

// Pong answers a client's [EventPing]. It carries no data, which is the frame
// Pusher's clients expect: {"event":"pusher:pong"}.
func Pong() Frame { return Frame{Event: EventPong} }

// SubscriptionSucceeded confirms a subscription and carries [Channel.Data].
//
// data is empty on every kind of channel but a presence one, and it still goes
// out as an object: an internal frame always carries {} rather than no data
// field, because a client that finds the field missing has nothing to parse.
func SubscriptionSucceeded(name ChannelName, data map[string]any) (Frame, error) {
	if name.IsZero() {
		return Frame{}, errors.New("joaju: a subscription confirmation needs a channel")
	}
	if data == nil {
		data = map[string]any{}
	}

	encoded, err := json.Marshal(data)
	if err != nil {
		return Frame{}, fmt.Errorf("joaju: the data of %s could not be encoded: %w", name.Requested(), err)
	}

	return Frame{Event: EventSubscriptionSucceeded, Channel: name.Requested(), Data: encoded}, nil
}

// MemberAdded tells the rest of a presence channel that somebody joined. It
// carries the whole [Member], which is user_id and user_info.
func MemberAdded(name ChannelName, m Member) (Frame, error) {
	if name.IsZero() {
		return Frame{}, errors.New("joaju: a member announcement needs a channel")
	}

	encoded, err := json.Marshal(m)
	if err != nil {
		return Frame{}, fmt.Errorf("joaju: the member of %s could not be encoded: %w", name.Requested(), err)
	}

	return Frame{Event: EventMemberAdded, Channel: name.Requested(), Data: encoded}, nil
}

// MemberRemoved tells the rest of a presence channel that somebody left.
//
// It carries the user_id alone, and not [Member.Info]: the departure identifies
// a member the others already have, and sending their information again on the
// way out would put a copy of it in a frame that nobody reads it from.
func MemberRemoved(name ChannelName, m Member) (Frame, error) {
	if name.IsZero() {
		return Frame{}, errors.New("joaju: a member announcement needs a channel")
	}

	// A struct of one string has no encoding that can fail.
	data, _ := json.Marshal(struct {
		UserID string `json:"user_id"`
	}{UserID: m.UserID})

	return Frame{Event: EventMemberRemoved, Channel: name.Requested(), Data: data}, nil
}

// CacheMiss tells a new subscriber of a cache channel that there is nothing to
// replay, so it stops waiting for an event that is not coming.
func CacheMiss(name ChannelName) (Frame, error) {
	if name.IsZero() {
		return Frame{}, errors.New("joaju: a cache miss needs a channel")
	}

	return Frame{Event: EventCacheMiss, Channel: name.Requested()}, nil
}

// EventFrame is the frame that carries an [Event] to a subscriber.
//
// It is the one place [Event] becomes bytes, and it is where the channel loses
// its tenant: [ChannelName.Requested] goes out, [ChannelName.String] never
// does. The client asked for "private-orders.17" and is answered about
// "private-orders.17"; that it was "acme:private-orders.17" here is not its to
// see, and a frame naming the tenant would tell every subscriber the name of
// the namespace their neighbours are in.
//
// [Event.Socket] is not in the frame. It says who not to send this to, which is
// the sender's business and the channel's, and it is answered before this is
// called.
//
// [Event.UserID] is, and only when there is one. It is set on a client event
// relayed from a presence channel and on nothing else, and [Frame.MarshalJSON]
// leaves the field out when it is empty -- so a private channel's frames keep
// the shape they had. A user_id of "" is a key a client validating a schema has
// to be taught to expect, in exchange for a value that never names anybody.
func EventFrame(e Event) (Frame, error) {
	if e.Name == "" {
		return Frame{}, errors.New("joaju: an event with no name cannot be sent")
	}
	if e.Channel.IsZero() {
		return Frame{}, errors.New("joaju: an event with no channel cannot be sent")
	}

	return Frame{Event: e.Name, Channel: e.Channel.Requested(), Data: e.Data, UserID: e.UserID}, nil
}

// SubscribeRequest is the data of a [EventSubscribe] frame: what a client asked
// to listen to, and what it offered in support.
type SubscribeRequest struct {
	// Channel is the name the client asked for, exactly as it sent it and with
	// no tenant in it. The caller turns it into a [ChannelName] with
	// [NewChannelName], which is where the tenant comes from the Grant.
	Channel string `json:"channel"`

	// Auth is the client's Pusher authentication signature, and this package
	// does not verify it. It is carried to [Subscription.Auth], where a
	// [SubscriptionPolicy] may.
	//
	// In the Pusher protocol this string is the whole authorization: the
	// application signs "socket_id:channel" with the shared secret over a
	// separate HTTP round trip, and the socket server checks the HMAC. That is
	// what nothing here does. In a mounted application the subject on the Grant
	// came through the host application's own front door, and a signature that
	// could also allow a subscription would be a second mechanism for one
	// decision -- particularly this one, which allows a channel without ever
	// naming a tenant.
	//
	// What is different is who is asked. The signature travels as evidence
	// rather than as authority: it allows nothing by itself, it builds no
	// tenant and no Grant, and a policy that ignores it refuses precisely what
	// it refused before. In a process that authenticates nobody there is no
	// first mechanism for it to be a second one to, and there it is the only
	// evidence about a browser there is -- which is why cmd/joaju could serve
	// no private or presence channel while [Subscription] had no field for it.
	Auth string `json:"auth,omitempty"`

	// ChannelData is the presence information the client offered, and it is a
	// claim rather than a fact -- see [Subscription.Member]. Read it with
	// [SubscribeRequest.Member].
	//
	// It reaches a policy undecoded as well, as [Subscription.ChannelData],
	// because it is the third part of what [SubscribeRequest.Auth] was computed
	// over and re-encoding it would change the bytes and so the hash.
	ChannelData json.RawMessage `json:"channel_data,omitempty"`
}

// Subscribe decodes the data of a [EventSubscribe] frame.
func (f Frame) Subscribe() (SubscribeRequest, error) {
	if f.Event != EventSubscribe {
		return SubscribeRequest{}, fmt.Errorf("%w: %s is not %s", ErrInvalidMessage, f.Event, EventSubscribe)
	}

	var r SubscribeRequest
	if len(f.Data) > 0 {
		if err := json.Unmarshal(f.Data, &r); err != nil {
			return SubscribeRequest{}, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
		}
	}
	if r.Channel == "" {
		return SubscribeRequest{}, fmt.Errorf("%w: the subscription named no channel", ErrInvalidMessage)
	}
	// channel_data arrives as a string containing JSON, one layer deeper than
	// the frame's own data, because that is how the clients send it.
	r.ChannelData = unwrapJSONString(r.ChannelData)

	return r, nil
}

// Unsubscribe decodes the data of a [EventUnsubscribe] frame, and is the
// channel name the client asked to leave -- raw, with no tenant in it, like
// [SubscribeRequest.Channel].
func (f Frame) Unsubscribe() (string, error) {
	if f.Event != EventUnsubscribe {
		return "", fmt.Errorf("%w: %s is not %s", ErrInvalidMessage, f.Event, EventUnsubscribe)
	}

	var r struct {
		Channel string `json:"channel"`
	}
	if len(f.Data) > 0 {
		if err := json.Unmarshal(f.Data, &r); err != nil {
			return "", fmt.Errorf("%w: %v", ErrInvalidMessage, err)
		}
	}
	if r.Channel == "" {
		return "", fmt.Errorf("%w: the unsubscription named no channel", ErrInvalidMessage)
	}

	return r.Channel, nil
}

// Member reads the presence data the client offered, and is the zero [Member]
// when it offered none.
//
// It accepts a user_id that arrived as a number as well as one that arrived as
// a string, because the clients send both. The number is kept as it was written
// rather than parsed and reprinted, so 007 stays 007.
//
// What it returns is a claim. A client sends its own channel_data, so the
// user_id in it is the one the client typed: a [SubscriptionPolicy] compares it
// against the subject on the Grant, and one that does not is a policy that lets
// a subscriber join a presence channel as somebody else.
func (r SubscribeRequest) Member() (Member, error) {
	if len(r.ChannelData) == 0 {
		return Member{}, nil
	}

	var wire struct {
		UserID json.RawMessage `json:"user_id"`
		Info   json.RawMessage `json:"user_info"`
	}
	if err := json.Unmarshal(r.ChannelData, &wire); err != nil {
		return Member{}, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}

	id, err := memberID(wire.UserID)
	if err != nil {
		return Member{}, err
	}

	return Member{UserID: id, Info: unwrapJSONString(wire.Info)}, nil
}

// memberID reads a user_id that may have been written as a string or as a
// number.
func memberID(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	if raw[0] == '"' {
		var id string
		if err := json.Unmarshal(raw, &id); err != nil {
			return "", fmt.Errorf("%w: %v", ErrInvalidMessage, err)
		}
		return id, nil
	}

	var id json.Number
	if err := json.Unmarshal(raw, &id); err != nil {
		return "", fmt.Errorf("%w: a user_id is a string or a number, not %s", ErrInvalidMessage, raw)
	}

	return id.String(), nil
}

// ClientEvents is whether this server relays the events a client publishes, and
// its zero value is off.
//
// A client event is a frame one browser sends that every other browser on the
// channel receives, and the server does not look at it. It is the only path in
// this repository where a client is the source of what other clients are told,
// and it is the half of the protocol worth being afraid of:
//
//   - the payload is the sender's, so whatever the receivers render from it is
//     rendered from a stranger's bytes;
//   - the sender is a browser, so its identity is whatever a
//     [SubscriptionPolicy] settled at subscription time and nothing more;
//   - nothing on the server sees the message go past, so there is no audit
//     trail, no validation and no rate limit that is not built here.
//
// So it is off unless something turns it on, which is the protocol's default
// too. When it is on, [ClientEvents.Accept] still holds two lines that do not
// move: the channel has to be one a policy guarded, and the sender has to be
// subscribed to it.
//
// # Why there are two states and not three
//
// The state this deliberately does not have is a third one that relays a client
// event without requiring the sender to be a member, forwarding its frame
// verbatim. It would change three things -- two of which cannot exist here, and
// the third of which is the authorization:
//
//   - the sender would not have to be on the channel. There is no frame a
//     client can send that reaches a [Channel] it did not subscribe to: the
//     caller of Accept is handed the seat the socket already holds, and nothing
//     on that path asks a [Broker] for a channel. Adding the lookup would be a
//     socket publishing into a private channel no [SubscriptionPolicy] ever
//     saw, which is the leak this repository exists not to have;
//   - the relayed payload would be the sender's frame verbatim -- every extra
//     top-level key it carried included, a user_id the sender wrote for itself
//     among them. Here a relayed frame is built field by field out of an
//     [Event], and there is no verbatim path for a setting to pick;
//   - no user_id would be stamped. Here it comes off the channel's record of
//     the seat, so the only way not to have one is not to have a seat, which is
//     the first point again.
//
// So the setting would be a name for the membership check with a value that
// turns it off, and one more way to relay a client event -- the looser of two
// paths being the one that ends up in production.
type ClientEvents bool

// The two states of [ClientEvents], named so a call site reads as a decision
// rather than as a bare true.
const (
	// ClientEventsOff refuses every client event. It is the zero value.
	ClientEventsOff ClientEvents = false
	// ClientEventsOn relays a client event from a subscriber of a guarded
	// channel, and only from one.
	ClientEventsOn ClientEvents = true
)

// Accept decides whether a client-published frame may be relayed, and turns it
// into the [Event] that would be relayed.
//
// channel is the [ChannelName] the caller resolved from the frame's channel
// field with [NewChannelName] and the socket's Grant -- so the tenant is
// already settled and is the Grant's, never the name the sender typed. Its
// [ChannelName.Requested] has to match what the frame said, which catches the
// caller resolving one name and relaying another. from is the sender, and it
// ends up in [Event.Socket] so the relay does not send them their own message
// back. sender and subscribed are the seat the channel is holding for them, and
// [Channel.Find] answers both at once.
//
// sender becomes [Event.UserID], and is the whole of who the receivers are told
// this came from. It is the [Member] the channel seated, so it is the identity a
// [SubscriptionPolicy] was asked about; it is the zero [Member] off a presence
// channel, and the relayed frame then carries no user_id at all. The frame's own
// [Frame.UserID] is never read here, and that is the point of taking the sender
// as an argument rather than off f: a client that writes a user_id into a client
// event is a client naming somebody else as the sender, and the channel's record
// is the one place that answer is not the sender's to write.
//
// The refusals, in the order they are made: not a client event at all is
// [ErrInvalidMessage]; the switch being off is [ErrClientEventsDisabled]; a
// public channel is [ErrClientEventChannel]; not being subscribed is
// [ErrNotSubscribed]. Each is a [ProtocolError], so [ErrorFrame] carries it to
// the client with its code intact.
func (c ClientEvents) Accept(f Frame, channel ChannelName, from SocketID, sender Member, subscribed bool) (Event, error) {
	if !f.IsClientEvent() {
		return Event{}, fmt.Errorf("%w: %s is not a client event", ErrInvalidMessage, f.Event)
	}
	if c != ClientEventsOn {
		return Event{}, ErrClientEventsDisabled
	}
	if channel.IsZero() || f.Channel == "" {
		return Event{}, fmt.Errorf("%w: the client event named no channel", ErrInvalidMessage)
	}
	if channel.Requested() != f.Channel {
		return Event{}, fmt.Errorf("%w: the client event named %s and was resolved as %s", ErrInvalidMessage, f.Channel, channel.Requested())
	}
	if !channel.Type().Guarded() {
		return Event{}, ErrClientEventChannel
	}
	if !subscribed {
		return Event{}, ErrNotSubscribed
	}

	return Event{Name: f.Event, Channel: channel, Data: f.Data, Socket: from, UserID: sender.UserID}, nil
}
