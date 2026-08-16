package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/joaju"
)

// The end to end tests: a real [joaju.Server] over a real socket, with the
// client that is embedded in this package driving it from a JavaScript runtime.
//
// This is the half that matters. A handler test proves the bytes were served; it
// proves nothing about whether they speak the protocol, and the protocol is the
// entire reason the file exists. Everything here skips when no runtime is
// installed -- see findJSRuntime -- so the suite passes on a machine with none
// and says which tests did not run.
//
// The scenarios drive themselves. Each one publishes through the server's own
// HTTP API and closes its own socket through the terminate route, so there is no
// stepping between Go and the runtime: the process runs, prints one JSON object,
// and exits.

const (
	e2eAppID  = "app-1"
	e2eAppKey = "key-1"
	e2eTenant = "acme"
)

// e2eConnect admits every handshake.
//
// The interesting decision here is the per-channel one, and a connect policy
// that refused would only stop the tests from reaching it. Refusal at this layer
// has its own tests in the root package.
type e2eConnect struct{}

func (e2eConnect) Can(context.Context, auth.Subject, auth.Action, joaju.Handshake) error {
	return nil
}

// e2eSubscribe is the application's decision about one channel, written the way
// an application's would be.
//
// The three refusals are each about something the client had to get right:
//
//   - a guarded channel with no signature means the browser never called the
//     authorization endpoint, or called it and dropped the answer;
//   - a presence member who is not the subject means the browser sent somebody
//     else's channel_data, which is the impersonation [joaju.Subscription]
//     warns about;
//   - "forbidden" is the channel that exists to be refused, so that the 4009
//     path is exercised on a socket that then goes on working.
//
// The first two only apply to a subscription that came off a socket. The API
// routes ask this same policy about a channel with no socket, no member and no
// signature -- an application publishing an event is not a browser -- and a
// policy that demanded a browser's evidence there would refuse every publish.
//
// The signature itself is not verified, and that is [joaju.SubscribeRequest]'s
// own position for a mounted application: the subject on the Grant arrived
// through the front door, so a signature that could also allow a subscription
// would be a second mechanism for one decision.
type e2eSubscribe struct{}

func (e2eSubscribe) Can(_ context.Context, subject auth.Subject, _ auth.Action, s joaju.Subscription) error {
	if s.Channel.IsZero() {
		// The collection question, which the channel-list route asks.
		return nil
	}

	name := s.Channel.Requested()
	if strings.Contains(name, "forbidden") {
		return fmt.Errorf("%w: %s may not hear %s", auth.ErrForbidden, subject.ID, name)
	}
	if s.Socket == "" {
		return nil
	}
	if s.Channel.Type().Guarded() && s.Auth == "" {
		return fmt.Errorf("%w: %s offered no authorization for %s", auth.ErrForbidden, subject.ID, name)
	}
	if s.Channel.Type().Presence() && s.Member.UserID != subject.ID {
		return fmt.Errorf("%w: %s asked to join %s as %q", auth.ErrForbidden, subject.ID, name, s.Member.UserID)
	}

	return nil
}

// e2eProtocol wraps the Pusher protocol so that a test can see what the client
// sent, and can decline to answer it.
//
// Swallowing pusher:ping is how the "the pong never came" case is produced: the
// socket stays open at the transport layer and stops answering at the protocol
// layer, which is what a proxy that went away looks like from a browser. There
// is no way to arrange that from the client side, which is the point.
type e2eProtocol struct {
	joaju.Protocol

	swallowPing bool

	mu     sync.Mutex
	frames []string
}

func (p *e2eProtocol) Message(ctx context.Context, conn *joaju.Connection, message []byte) error {
	p.mu.Lock()
	p.frames = append(p.frames, string(message))
	p.mu.Unlock()

	if p.swallowPing && bytes.Contains(message, []byte(`"`+joaju.EventPing+`"`)) {
		return nil
	}

	return p.Protocol.Message(ctx, conn, message)
}

func (p *e2eProtocol) sent() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.frames...)
}

// e2eSubjects is the subject hesape/auth's Authenticate middleware would have
// left on the context, and the fixture's answer to a browser being unable to set
// a header on a WebSocket.
//
// Sockets are handed the subjects in order, so the first client to connect is
// the first name and the second is the second -- which is how one process stands
// in for two signed-in people. Everything else gets the first name, because an
// application publishing an event is one caller however many browsers are
// connected.
func e2eSubjects(next http.Handler, sequence ...string) http.Handler {
	var mu sync.Mutex
	var opened int

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := sequence[0]
		if strings.HasPrefix(r.URL.Path, "/app/") {
			mu.Lock()
			if opened < len(sequence) {
				id = sequence[opened]
			} else {
				id = sequence[len(sequence)-1]
			}
			opened++
			mu.Unlock()
		}

		next.ServeHTTP(w, r.WithContext(auth.WithSubject(r.Context(),
			auth.Subject{ID: id, Tenant: e2eTenant})))
	})
}

// e2eAuthorize is the application's /broadcasting/auth endpoint, in the shape
// the Pusher protocol expects.
//
// It answers a signature for anything, including the channel the policy then
// refuses -- an authorization endpoint that also enforced would be the second
// mechanism, and the refusal under test is the server's.
//
// The user comes from a header because the browser half of this fixture sets one:
// fetch can, and the WebSocket constructor cannot, which is why the socket's
// subject is assigned in e2eSubjects instead.
func e2eAuthorize(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "unreadable", http.StatusBadRequest)
		return
	}

	channel := r.PostFormValue("channel_name")
	socket := r.PostFormValue("socket_id")
	if channel == "" || socket == "" {
		http.Error(w, "an authorization needs a socket and a channel", http.StatusBadRequest)
		return
	}

	user := r.Header.Get("X-Joaju-User")
	if user == "" {
		user = "ana"
	}

	body := map[string]string{"auth": e2eAppKey + ":" + socket + ":" + channel}
	if strings.HasPrefix(channel, "presence-") {
		// channel_data travels as a JSON string containing JSON, which is the
		// layer the server undoes with unwrapJSONString.
		data, err := json.Marshal(map[string]any{
			"user_id":   user,
			"user_info": map[string]string{"name": user},
		})
		if err != nil {
			http.Error(w, "unencodable", http.StatusInternalServerError)
			return
		}
		body["channel_data"] = string(data)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(body)
}

// e2eServer is one running application: the socket server, the authorization
// endpoint, and the middleware that signs people in.
type e2eServer struct {
	http     *httptest.Server
	protocol *e2eProtocol
}

// newE2EServer starts one.
//
// activityTimeout is what the server tells the client to ping after, and it is
// the only reason it is a parameter: a scenario about the ping wants a second,
// and every other scenario wants it far enough away not to interfere.
func newE2EServer(t *testing.T, activityTimeout time.Duration, swallowPing bool, subjects ...string) *e2eServer {
	t.Helper()

	broker := joaju.NewMemoryBroker()
	subscribe := e2eSubscribe{}

	// The same Broker reaches the protocol and the server, which is the wiring
	// [joaju.NewServer] refuses to guess at.
	protocol := &e2eProtocol{
		Protocol: joaju.NewPusher(broker, subscribe, joaju.PusherConfig{
			ActivityTimeout: activityTimeout,
			ClientEvents:    joaju.ClientEventsOn,
		}),
		swallowPing: swallowPing,
	}

	server, err := joaju.NewServer(joaju.ServerConfig{
		AppID:     e2eAppID,
		AppKey:    e2eAppKey,
		Broker:    broker,
		Connect:   e2eConnect{},
		Subscribe: subscribe,
		Protocol:  protocol,
		// Every scenario causes a refusal on purpose. The suite's output is not
		// where they are read.
		Log: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("NewServer() = %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /broadcasting/auth", e2eAuthorize)
	mux.Handle("/", server)

	front := httptest.NewServer(e2eSubjects(mux, subjects...))
	t.Cleanup(func() {
		server.Close(context.Background())
		front.Close()
	})

	return &e2eServer{http: front, protocol: protocol}
}

// environment is what the scenario reads to find this server.
func (s *e2eServer) environment() []string {
	return []string{
		"JOAJU_HTTP=" + s.http.URL,
		"JOAJU_WS=ws" + strings.TrimPrefix(s.http.URL, "http"),
		"JOAJU_KEY=" + e2eAppKey,
		"JOAJU_APP=" + e2eAppID,
	}
}

// socketIDShape is what the protocol's clients print, and what newSocketID mints.
var socketIDShape = regexp.MustCompile(`^\d+\.\d+$`)

// The whole protocol in one pass, through two browsers on one server.
func TestTheClientSpeaksTheProtocolEndToEnd(t *testing.T) {
	server := newE2EServer(t, time.Minute, false, "ana", "bruno")
	result := runScenario(t, "protocol.js", server.environment())

	// The socket id is the first thing the connection is for: it is what a
	// publish quotes so that the publisher does not receive its own message.
	if id := text(t, result, "socketId"); !socketIDShape.MatchString(id) {
		t.Errorf("the socket id is %q, and the protocol's clients print <digits>.<digits>", id)
	}
	if state := text(t, result, "state"); state != "connected" {
		t.Errorf("the state after connection_established is %q, want connected", state)
	}

	for _, key := range []string{"publicSubscribed", "privateSubscribed"} {
		if !flag(t, result, key) {
			t.Errorf("%s is false: the server never confirmed the subscription", key)
		}
	}

	// The presence list arrives with the confirmation, and the new subscriber is
	// in it: the server builds it after seating them, on purpose.
	if got := list(t, result, "membersAtFirst"); !equal(got, []string{"ana"}) {
		t.Errorf("the presence channel opened with %v, want [ana]", got)
	}

	// An event published over the HTTP API, delivered to the socket.
	if got := object(t, result, "event"); !equal(keysOf(got), []string{"id"}) || got["id"] != float64(7) {
		t.Errorf("the delivered event carried %v, want {id: 7}", got)
	}
	if got := text(t, result, "eventChannel"); got != "orders" {
		t.Errorf("the event named channel %q: the client asked for orders and the tenant is not its to see", got)
	}

	// The second browser, as a second person.
	added := object(t, result, "memberAdded")
	if added["user_id"] != "bruno" {
		t.Errorf("member_added named %v, want bruno", added["user_id"])
	}
	if got := list(t, result, "membersAfterJoin"); !equal(got, []string{"ana", "bruno"}) {
		t.Errorf("the first client's member list is %v, want [ana bruno]", got)
	}
	if got := list(t, result, "brunoMembers"); !equal(got, []string{"ana", "bruno"}) {
		t.Errorf("the second client's member list is %v, want [ana bruno]", got)
	}

	// A client event: one browser to the other, and the user_id on it is the one
	// the channel seated rather than the one the sender wrote.
	if got := object(t, result, "clientEvent"); got["at"] != "the keyboard" {
		t.Errorf("the client event carried %v", got)
	}
	if got := text(t, result, "clientEventUser"); got != "bruno" {
		t.Errorf("the client event came from %q, want bruno", got)
	}

	// A refusal is 4009 and it is not the end of the connection: the frame was
	// refused, and everything already subscribed keeps working.
	if got := number(t, result, "refusedCode"); got != 4009 {
		t.Errorf("the refused subscription answered %v, want 4009", got)
	}
	if !flag(t, result, "stillConnected") {
		t.Error("a refused subscription took the connection down with it")
	}
	if !flag(t, result, "stillSubscribed") {
		t.Error("a refused subscription dropped the channels that had been allowed")
	}

	if got := object(t, result, "memberRemoved"); got["user_id"] != "bruno" {
		t.Errorf("member_removed named %v, want bruno", got["user_id"])
	}
	if got := list(t, result, "membersAfterLeave"); !equal(got, []string{"ana"}) {
		t.Errorf("the member list after the departure is %v, want [ana]", got)
	}

	if !flag(t, result, "publicWhisperRefused") {
		t.Error("a client event on a public channel was sent: the client should refuse it before the server has to")
	}

	// The tenant, seen from the browser: it is on every name the server holds
	// and on nothing the client ever receives.
	if encoded, err := json.Marshal(result); err != nil {
		t.Fatal(err)
	} else if bytes.Contains(encoded, []byte(e2eTenant+":")) {
		t.Errorf("the tenant reached the browser: %s", encoded)
	}
	for _, frame := range server.protocol.sent() {
		if strings.Contains(frame, e2eTenant+":") {
			t.Errorf("the client sent a name carrying a tenant: %s", frame)
		}
	}
}

// The activity_timeout is the server's instruction, and a client that does not
// obey it is a client the server hangs up on.
func TestTheClientPingsOnTheServersActivityTimeout(t *testing.T) {
	server := newE2EServer(t, time.Second, false, "ana")
	result := runScenario(t, "ping.js", server.environment())

	// Read off the frame and not off the options: the client's own default is two
	// minutes, so a client that ignored the frame would report that instead.
	if got := number(t, result, "activityTimeout"); got != 1000 {
		t.Errorf("the client is pinging every %vms, and the server said 1000", got)
	}
	if before, after := text(t, result, "socketId"), text(t, result, "socketIdAfter"); before != after {
		t.Errorf("the socket changed from %s to %s while nothing was happening", before, after)
	}
	if state := text(t, result, "state"); state != "connected" {
		t.Errorf("after two and a half seconds of silence the client is %q", state)
	}

	pings := 0
	for _, frame := range server.protocol.sent() {
		if strings.Contains(frame, joaju.EventPing) {
			pings++
		}
	}
	if pings < 2 {
		t.Errorf("the server received %d pings in two and a half seconds at a one second timeout, want at least 2: %v",
			pings, server.protocol.sent())
	}
}

// A socket that is open and answering nothing is the case the ping exists for,
// and the only way out of it is the client giving up on its own.
func TestTheClientReconnectsWhenThePongDoesNotCome(t *testing.T) {
	server := newE2EServer(t, time.Second, true, "ana")
	result := runScenario(t, "silence.js", server.environment())

	if !flag(t, result, "changed") {
		t.Errorf("the client kept a socket that stopped answering: %s and %s",
			text(t, result, "first"), text(t, result, "second"))
	}
	if state := text(t, result, "state"); state != "connected" {
		t.Errorf("the state after the second connection is %q, want connected", state)
	}
}

// A reconnect is not just a new socket: the channels have to come back, and the
// guarded one has to be authorized again -- the signature covers the socket id.
func TestTheClientReconnectsAndResubscribes(t *testing.T) {
	server := newE2EServer(t, time.Minute, false, "ana")
	result := runScenario(t, "reconnect.js", server.environment())

	if !flag(t, result, "changed") {
		t.Fatalf("the socket id did not change across the reconnect: %s", text(t, result, "first"))
	}
	for _, key := range []string{"publicSubscribed", "privateSubscribed"} {
		if !flag(t, result, key) {
			t.Errorf("%s is false after the reconnect: the channel did not come back", key)
		}
	}
	if got := object(t, result, "publicEvent"); got["id"] != float64(9) {
		t.Errorf("the event on the resubscribed public channel was %v, want {id: 9}", got)
	}
	if got := object(t, result, "privateEvent"); got["id"] != float64(11) {
		t.Errorf("the event on the resubscribed private channel was %v, want {id: 11}", got)
	}
	if got := number(t, result, "held"); got != 2 {
		t.Errorf("the client holds %v channels after resubscribing, want 2: a second copy is a second seat", got)
	}
}

// ------------------------------------------------------------------
// Reading one scenario's result, which is a JSON object.
// ------------------------------------------------------------------

func text(t *testing.T, result map[string]any, key string) string {
	t.Helper()

	value, ok := result[key].(string)
	if !ok {
		t.Fatalf("the scenario reported no %s: %v", key, result[key])
	}

	return value
}

func flag(t *testing.T, result map[string]any, key string) bool {
	t.Helper()

	value, ok := result[key].(bool)
	if !ok {
		t.Fatalf("the scenario reported no %s: %v", key, result[key])
	}

	return value
}

func number(t *testing.T, result map[string]any, key string) float64 {
	t.Helper()

	value, ok := result[key].(float64)
	if !ok {
		t.Fatalf("the scenario reported no %s: %v", key, result[key])
	}

	return value
}

func object(t *testing.T, result map[string]any, key string) map[string]any {
	t.Helper()

	value, ok := result[key].(map[string]any)
	if !ok {
		t.Fatalf("the scenario reported no %s: %v", key, result[key])
	}

	return value
}

func list(t *testing.T, result map[string]any, key string) []string {
	t.Helper()

	raw, ok := result[key].([]any)
	if !ok {
		t.Fatalf("the scenario reported no %s: %v", key, result[key])
	}

	all := make([]string, 0, len(raw))
	for _, one := range raw {
		text, ok := one.(string)
		if !ok {
			t.Fatalf("%s holds %v, which is not a string", key, one)
		}
		all = append(all, text)
	}

	return all
}

func keysOf(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}

	return keys
}

func equal(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}

	return true
}
