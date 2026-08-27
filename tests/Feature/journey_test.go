package feature

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
	"github.com/arandu-io/joaju"
	"github.com/arandu-io/joaju/protocols/pusher"
	"github.com/arandu-io/joaju/tests"
	"github.com/arandu-io/joaju/ws"
)

// The journey an application takes, from the front door to a delivered event.
//
// The other two files here stand one server up with one subject signed into it,
// which is what a test of the frames or of the socket wants. This one stands up
// the shape a deployment has instead: several people signed in at once, more
// than one customer among them, all of them dialling the same process, and each
// one hearing only what somebody decided they could hear.
//
// What is proved here is what no single layer proves on its own. A refusal that
// reaches the client is protocol_test.go's; a refusal that also keeps the
// refused socket off the channel is this file's, because it needs the policy,
// the protocol, the broker and a publish in one place. The tenant rule is the
// same story: [joaju.NewChannelName] and pusher.MemoryBroker are each tested
// against it, and neither of them can show two customers on one server reading
// two different things off one channel name.
//
// The names are prefixed for the reason the other two files give: the package is
// one package, and the helpers of three files share one namespace. The frame
// helpers are NOT re-declared -- protocolSend, protocolSubscribe,
// protocolRefusal and protocolSilence are read from protocol_test.go, because a
// second way to read a frame is a second frame reader to keep correct.

// The application this file's servers are. One server is one application, so
// these are values and not a lookup.
const (
	journeyAppID  = "app-journey"
	journeyAppKey = "key-journey"
)

// journeyOtherTenant is the second customer on the server.
//
// It shares no substring with [tests.Tenant], and it has to: every frame this
// file reads is checked for both names, and a pair where one contained the other
// would make half of that check pass by accident.
const journeyOtherTenant = "globex"

// journeyChannel is the name BOTH customers ask for.
//
// One string, sent by two clients of two tenants, is the whole of the tenant
// test below: two different names would pass against a server with no tenant
// rule in it at all.
const journeyChannel = "orders.17"

// journeyModerationChannel is the guarded channel of this file's application,
// and journeyModeratorRole is who may hear it.
//
// The tenant is not in the string and never is: it is put there by
// [joaju.NewChannelName], from the Grant, before any policy sees the name.
const (
	journeyModerationChannel = broadcasting.PrivateChannelPrefix + "moderation"
	journeyModeratorRole     = "moderator"
)

// The three headers the front door below reads.
//
// They stand in for the session cookie a deployment reads, and nothing in joaju
// reads them: the middleware is what turns them into an auth.Subject, exactly as
// hesape/auth's Authenticate middleware turns a session into one. They are a
// header rather than a cookie because a test dials several people at once and a
// header is per request.
const (
	journeySubjectHeader = "X-Journey-Subject"
	journeyTenantHeader  = "X-Journey-Tenant"
	journeyRolesHeader   = "X-Journey-Roles"
)

// journeyCaller is one signed-in person: who they are, whose customer they are,
// and what they are. It is what a test dials, publishes and reads as.
type journeyCaller struct {
	id     string
	tenant string
	roles  []string
}

// sign puts the caller on a request, which is what the front door reads back.
//
// A caller with no id signs nothing, and that is the visitor with no session at
// all -- not a subject with an empty id. The two are answered differently, so
// they cannot be the same request here.
func (c journeyCaller) sign(h http.Header) {
	if c.id == "" {
		return
	}

	h.Set(journeySubjectHeader, c.id)
	h.Set(journeyTenantHeader, c.tenant)
	if len(c.roles) > 0 {
		h.Set(journeyRolesHeader, strings.Join(c.roles, ","))
	}
}

// journeyFront is the front door: it reads the headers and leaves the subject on
// the request context, which is the whole of what a deployment's authentication
// middleware does for this server.
//
// A request carrying no subject header passes through with no subject at all,
// because that is what joaju answers 401 to.
func journeyFront(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(journeySubjectHeader)
		if id == "" {
			next.ServeHTTP(w, r)

			return
		}

		subject := auth.Subject{ID: id, Tenant: r.Header.Get(journeyTenantHeader)}
		if roles := r.Header.Get(journeyRolesHeader); roles != "" {
			subject.Roles = strings.Split(roles, ",")
		}

		next.ServeHTTP(w, r.WithContext(auth.WithSubject(r.Context(), subject)))
	})
}

// journeyConnectPolicy is who may hold a socket on this application: anybody the
// front door identified.
//
// It admits both customers, and that is the point of this file: one process
// serving two tenants is a deployment somebody has, and it is the deployment in
// which a tenant rule can be got wrong.
type journeyConnectPolicy struct{}

func (journeyConnectPolicy) Can(_ context.Context, s auth.Subject, a auth.Action, _ joaju.Handshake) error {
	if a != joaju.Connect {
		return fmt.Errorf("no rule allows %s on a socket", a)
	}
	if s.IsGuest() {
		return errors.New("sign in before opening a socket")
	}

	return nil
}

// journeySubscribePolicy is who may hear what, and it is the only thing between
// a socket and a channel.
//
// allow is this fixture's rule. Nil allows every channel, which is what a test
// about delivery wants; a test about a refusal hands one in. It is a field and
// not a second type because what changes between these tests is one decision.
//
// asked is every subscription the policy saw, in order, and it is how a test
// says what the policy was SHOWN rather than only what it answered. The tenant
// test reads it: two clients sent one string, and the proof is that the policy
// was asked about two channels.
type journeySubscribePolicy struct {
	allow func(auth.Subject, joaju.Subscription) error

	mu    sync.Mutex
	asked []joaju.Subscription
}

func (p *journeySubscribePolicy) Can(_ context.Context, s auth.Subject, a auth.Action, sub joaju.Subscription) error {
	p.mu.Lock()
	p.asked = append(p.asked, sub)
	p.mu.Unlock()

	if a != broadcasting.ChannelJoin {
		return fmt.Errorf("no rule allows %s on a channel", a)
	}
	if s.IsGuest() {
		return errors.New("sign in before subscribing to a channel")
	}
	if p.allow == nil {
		return nil
	}

	return p.allow(s, sub)
}

// named is every subscription the policy was asked about that named a channel.
//
// The collection question -- the channel list route -- asks about the zero
// Subscription, so a test counting what a client asked for has to leave those
// out or count the reads it made itself.
func (p *journeySubscribePolicy) named() []joaju.Subscription {
	p.mu.Lock()
	defer p.mu.Unlock()

	var named []joaju.Subscription
	for _, one := range p.asked {
		if !one.Channel.IsZero() {
			named = append(named, one)
		}
	}

	return named
}

// journeyFixture is one running server and the policy a test reaches into.
type journeyFixture struct {
	http   *httptest.Server
	policy *journeySubscribePolicy
}

// newJourneyFixture stands up the wiring an application has: the in-memory
// Broker behind both halves, both policies, and the front door in front.
//
// There is no Relay, and there is nothing to pass one to: this is one process,
// which is the deployment the package documents itself as complete for.
func newJourneyFixture(t *testing.T, allow func(auth.Subject, joaju.Subscription) error) *journeyFixture {
	t.Helper()

	policy := &journeySubscribePolicy{allow: allow}
	broker := pusher.NewMemoryBroker()

	server, err := joaju.NewServer(joaju.ServerConfig{
		AppID:     journeyAppID,
		AppKey:    journeyAppKey,
		Broker:    broker,
		Connect:   journeyConnectPolicy{},
		Subscribe: policy,
		Protocol:  pusher.NewPusher(broker, policy, pusher.PusherConfig{}),
		// Several tests here refuse something on purpose, and the suite's output
		// is not where those refusals are read.
		Log: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatalf("NewServer() = %v", err)
	}

	f := &journeyFixture{http: httptest.NewServer(journeyFront(server)), policy: policy}
	t.Cleanup(func() {
		server.Close(context.Background())
		f.http.Close()
	})

	return f
}

// dial opens a socket as one caller, with the Origin a browser on this host
// sends.
func (f *journeyFixture) dial(t *testing.T, who journeyCaller) (*ws.Conn, *http.Response, error) {
	t.Helper()

	header := http.Header{"Origin": {f.http.URL}}
	who.sign(header)

	dialer := *ws.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second

	return dialer.Dial("ws"+strings.TrimPrefix(f.http.URL, "http")+"/app/"+journeyAppKey, header)
}

// open dials as one caller and reads pusher:connection_established, so that what
// a test reads next is an answer to something it sent.
func (f *journeyFixture) open(t *testing.T, who journeyCaller) *ws.Conn {
	t.Helper()

	conn, _, err := f.dial(t, who)
	if err != nil {
		t.Fatalf("dialling as %s of %s = %v", who.id, who.tenant, err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if established := journeyNext(t, conn); established.Event != joaju.EventConnectionEstablished {
		t.Fatalf("the first frame was %s, want %s", established.Event, joaju.EventConnectionEstablished)
	}

	return conn
}

// publish is the call an application makes when something happened, made as one
// caller.
//
// The tenant of the channel it reaches is the tenant of the subject that asked,
// and it is not a field of the body: there is no way to spell it here, which is
// the same reason a subscriber cannot spell it either.
func (f *journeyFixture) publish(t *testing.T, who journeyCaller, channel, event, data string) {
	t.Helper()

	body := `{"name":"` + event + `","channel":"` + channel + `","data":` + strconv.Quote(data) + `}`

	request, err := http.NewRequest(http.MethodPost, f.http.URL+"/apps/"+journeyAppID+"/events", strings.NewReader(body))
	if err != nil {
		t.Fatalf("building the publish of %s on %s = %v", event, channel, err)
	}
	request.Header.Set("Content-Type", "application/json")
	who.sign(request.Header)

	response, err := f.http.Client().Do(request)
	if err != nil {
		t.Fatalf("publishing %s on %s = %v", event, channel, err)
	}
	defer func() { _ = response.Body.Close() }()

	answered, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading the publish of %s on %s = %v", event, channel, err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("publishing %s on %s as %s of %s answered %d: %s", event, channel, who.id, who.tenant, response.StatusCode, answered)
	}
}

// get makes one API call as a caller and decodes what it answered.
//
// Every answer is checked for both tenants for the reason [journeyNext] checks
// every frame: these routes are what an operator's screen reads, and a customer
// name in one of them is one customer learning that the other exists.
func (f *journeyFixture) get(t *testing.T, who journeyCaller, path string, into any) {
	t.Helper()

	request, err := http.NewRequest(http.MethodGet, f.http.URL+path, nil)
	if err != nil {
		t.Fatalf("building GET %s = %v", path, err)
	}
	who.sign(request.Header)

	response, err := f.http.Client().Do(request)
	if err != nil {
		t.Fatalf("GET %s = %v", path, err)
	}
	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading GET %s = %v", path, err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s as %s of %s answered %d: %s", path, who.id, who.tenant, response.StatusCode, body)
	}
	if strings.Contains(string(body), tests.Tenant) || strings.Contains(string(body), journeyOtherTenant) {
		t.Fatalf("GET %s answered with a tenant in it: %s", path, body)
	}
	if err := json.Unmarshal(body, into); err != nil {
		t.Fatalf("decoding GET %s (%s) = %v", path, body, err)
	}
}

// connections is how many sockets this process holds for one caller's tenant,
// off the route an operator's screen reads.
func (f *journeyFixture) connections(t *testing.T, who journeyCaller) int {
	t.Helper()

	var answer struct {
		Connections int `json:"connections"`
	}
	f.get(t, who, "/apps/"+journeyAppID+"/connections", &answer)

	return answer.Connections
}

// channels is the channel list one caller's tenant is answered, keyed by the
// name a client asked for.
func (f *journeyFixture) channels(t *testing.T, who journeyCaller) map[string]map[string]any {
	t.Helper()

	var answer struct {
		Channels map[string]map[string]any `json:"channels"`
	}
	f.get(t, who, "/apps/"+journeyAppID+"/channels", &answer)

	return answer.Channels
}

// waitForConnections waits until this process holds n sockets for one caller's
// tenant.
//
// Closing a socket from the client's side answers immediately, and what follows
// happens on the reader goroutine the server holds it with. This is how a test
// acts on a socket having actually gone rather than on a Close having returned.
func (f *journeyFixture) waitForConnections(t *testing.T, who journeyCaller, n int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		held := f.connections(t, who)
		if held == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("this process holds %d sockets for %s, want %d", held, who.tenant, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// journeyNext reads one frame and refuses one carrying either tenant.
//
// [protocolNext] already refuses [tests.Tenant] on every frame this suite reads,
// and the second customer is refused here for the same reason and in the same
// place: a name that reached the wire with a tenant on it is a name a client
// could quote back, and the whole of the tenant rule is that it cannot.
func journeyNext(t *testing.T, conn *ws.Conn) pusher.Frame {
	t.Helper()

	f := protocolNext(t, conn)
	if strings.Contains(tests.Encode(t, f), journeyOtherTenant) {
		t.Fatalf("a frame reached the wire carrying the tenant %q: %+v", journeyOtherTenant, f)
	}

	return f
}

// A socket with no session at all does not become a socket.
//
// The refusal is an HTTP status and not a close code, and that is the stronger
// of the two: the decision happens before the upgrade, so there is no socket to
// close and nothing for a client to reconnect to. A close code would mean this
// server had already accepted somebody it had decided against.
func TestJourneyASocketWithNoSessionIsRefusedBeforeItExists(t *testing.T) {
	f := newJourneyFixture(t, nil)

	conn, response, err := f.dial(t, journeyCaller{})
	if err == nil {
		_ = conn.Close()
		t.Fatal("a socket opened for a request the front door left no subject on")
	}
	if response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("a request with no subject answered %v, want %d", response, http.StatusUnauthorized)
	}
	if asked := f.policy.named(); len(asked) != 0 {
		t.Fatalf("the subscription policy was asked about %v, want nothing: there was never a socket", asked)
	}
}

// A subscription no policy allowed is refused in a frame the client can read,
// and it is refused all the way down: what is published on that channel
// afterwards does not reach it.
//
// The second half is the one worth having. That the refusal ARRIVES, with the
// code a Pusher client branches on and without the policy's sentence in it, is
// TestPusherRefusesASubscriptionThePolicyRefusesAndKeepsTheSocket. What that
// test cannot say is whether the socket was also kept off the channel: a server
// that answered the frame with an error and subscribed anyway would pass it and
// leak every event published from then on.
func TestJourneyARefusedSubscriberIsToldAndThenHearsNothing(t *testing.T) {
	f := newJourneyFixture(t, func(s auth.Subject, sub joaju.Subscription) error {
		if sub.Channel.Requested() == journeyModerationChannel && !s.HasRole(journeyModeratorRole) {
			return fmt.Errorf("%s moderates nothing", s.ID)
		}

		return nil
	})

	reader := journeyCaller{id: "ana", tenant: tests.Tenant}
	moderator := journeyCaller{id: "bruno", tenant: tests.Tenant, roles: []string{journeyModeratorRole}}

	refused := f.open(t, reader)
	protocolSend(t, refused, `{"event":"pusher:subscribe","data":{"channel":"`+journeyModerationChannel+`"}}`)

	code, message := protocolRefusal(t, refused)
	if code != pusher.CodeUnauthorized {
		t.Fatalf("the refused subscription answered %d, want %d: a client that is told nothing cannot tell a refusal from a dropped network", code, pusher.CodeUnauthorized)
	}
	if strings.Contains(message, reader.id) || strings.Contains(message, "moderates") {
		t.Fatalf("the refusal disclosed the policy's reason: %q", message)
	}

	// Somebody the policy allows, so that there is a channel for the event to be
	// published on. Without one the publish below would reach nobody for a
	// reason that has nothing to do with the refusal.
	allowed := f.open(t, moderator)
	protocolSubscribe(t, allowed, journeyModerationChannel)

	f.publish(t, moderator, journeyModerationChannel, "CommentPosted", `{"id":9}`)

	delivered := journeyNext(t, allowed)
	if delivered.Event != "CommentPosted" || delivered.Channel != journeyModerationChannel {
		t.Fatalf("the moderator received %+v, want the event on the channel the policy allowed", delivered)
	}

	// And the socket the policy refused hears none of it. It is still open,
	// which is what protocolSilence also asserts: a refusal is about one frame.
	protocolSilence(t, refused)
}

// A subscriber hears its own channel and hears nothing of a channel it never
// asked for.
//
// Delivery on its own is proved several times over in protocol_test.go. What is
// here is the other half of the same sentence: two channels on one server are
// two groups of sockets, and the only thing keeping them apart is which group a
// socket is in.
func TestJourneyASubscriberHearsItsChannelAndNotOneItNeverAskedFor(t *testing.T) {
	f := newJourneyFixture(t, nil)

	ana := journeyCaller{id: "ana", tenant: tests.Tenant}
	bruno := journeyCaller{id: "bruno", tenant: tests.Tenant}

	mine := f.open(t, ana)
	protocolSubscribe(t, mine, journeyChannel)

	theirs := f.open(t, bruno)
	protocolSubscribe(t, theirs, "orders.18")

	// The order of the two publishes is what makes the assertion below hold with
	// no sleep in it. A publish has finished fanning out when it answers, and one
	// socket's frames are written in the order they were queued -- so a copy of
	// the first event delivered to the wrong socket would be sitting AHEAD of the
	// second event, and the first frame that socket reads would be the wrong one.
	f.publish(t, ana, journeyChannel, "OrderShipped", `{"id":17}`)
	f.publish(t, bruno, "orders.18", "OrderShipped", `{"id":18}`)

	delivered := journeyNext(t, mine)
	if delivered.Channel != journeyChannel || string(delivered.Data) != `{"id":17}` {
		t.Fatalf("the subscriber received %+v, want the event on the channel it subscribed to", delivered)
	}

	if crossed := journeyNext(t, theirs); crossed.Channel != "orders.18" || string(crossed.Data) != `{"id":18}` {
		t.Fatalf("the first frame the other socket read was %+v, want its own channel's event: it was on the wire before this one was published", crossed)
	}

	// And nothing of the other channel reaches this socket either. This is the
	// last read on it: a socket that times out a read has failed, which is what
	// protocolSilence asks it to do.
	protocolSilence(t, mine)
}

// Two customers, one process, one channel name. Neither one hears the other.
//
// This is the failure with no symptom on the side that suffers it. A subscriber
// receiving another customer's event receives a well-formed frame, on the
// channel it asked for, with the event name it was expecting; nothing about it
// looks wrong until somebody reads the payload.
//
// Both clients send the SAME string, and that is what makes the test worth
// running: with two different names it would pass against a server that had no
// tenant rule in it at all.
func TestJourneyTwoTenantsOnOneChannelNameDoNotHearEachOther(t *testing.T) {
	f := newJourneyFixture(t, nil)

	ana := journeyCaller{id: "ana", tenant: tests.Tenant}
	bruno := journeyCaller{id: "bruno", tenant: journeyOtherTenant}

	mine := f.open(t, ana)
	theirs := f.open(t, bruno)

	// The same string from both, and confirmed back to both unchanged: the name
	// a client asks for is the name it is answered about, because the tenant was
	// never in it.
	for _, one := range []struct {
		who  journeyCaller
		conn *ws.Conn
	}{{ana, mine}, {bruno, theirs}} {
		if confirmation := protocolSubscribe(t, one.conn, journeyChannel); confirmation.Channel != journeyChannel {
			t.Fatalf("%s of %s subscribed to %s and was answered about %s", one.who.id, one.who.tenant, journeyChannel, confirmation.Channel)
		}
	}

	// The construction rather than the assertion. Two clients sent one string
	// and the policy was asked about two channels, because [joaju.NewChannelName]
	// took the tenant off the Grant each of them arrived with. Reaching the other
	// name would mean holding the other Grant, and a client cannot write the
	// tenant into the name it sends either -- that is refused before a name
	// exists, in TestPusherRefusesAChannelNameThatCarriesATenant.
	asked := f.policy.named()
	if len(asked) != 2 {
		t.Fatalf("the policy was asked about %d named subscriptions, want 2", len(asked))
	}
	if asked[0].Channel.Requested() != asked[1].Channel.Requested() {
		t.Fatalf("the two clients asked for %q and %q: with two names this test proves nothing",
			asked[0].Channel.Requested(), asked[1].Channel.Requested())
	}
	if asked[0].Channel.String() == asked[1].Channel.String() {
		t.Fatalf("both subscriptions resolved to %q: one name for two customers is one channel for two customers", asked[0].Channel)
	}
	if asked[0].Channel.Tenant() != tests.Tenant || asked[1].Channel.Tenant() != journeyOtherTenant {
		t.Fatalf("the two channels belong to %q and %q, want %q and %q",
			asked[0].Channel.Tenant(), asked[1].Channel.Tenant(), tests.Tenant, journeyOtherTenant)
	}

	// Three publishes, interleaved, and the order is the assertion. A publish has
	// finished fanning out when it answers and one socket's frames are written in
	// the order they were queued, so an event delivered to the wrong customer is
	// an event sitting AHEAD of that customer's own next one -- which makes every
	// read below a read of the first frame rather than a wait for a frame that
	// never comes.
	//
	// The payloads name the person and not the customer: journeyNext fails a
	// frame carrying either tenant, and a payload spelling one would fail this
	// test for the wrong reason.
	f.publish(t, ana, journeyChannel, "OrderShipped", `{"from":"ana"}`)
	f.publish(t, bruno, journeyChannel, "OrderShipped", `{"from":"bruno"}`)
	f.publish(t, ana, journeyChannel, "OrderShipped", `{"from":"ana-again"}`)

	// The first customer's two, in order. The second one is where the other
	// direction is caught: the other customer's event was published between
	// them, so if it crossed it is what this read returns.
	if delivered := journeyNext(t, mine); string(delivered.Data) != `{"from":"ana"}` {
		t.Fatalf("the first customer received %+v, want its own first event", delivered)
	}
	if delivered := journeyNext(t, mine); string(delivered.Data) != `{"from":"ana-again"}` {
		t.Fatalf("the first customer's next frame was %+v, want its own second event: the other customer published on this same channel name in between", delivered)
	}

	// And the second customer's one. Its first frame is the proof for the other
	// direction: the first customer had already published on this name before
	// this event existed.
	if delivered := journeyNext(t, theirs); delivered.Channel != journeyChannel || string(delivered.Data) != `{"from":"bruno"}` {
		t.Fatalf("the second customer's first frame was %+v, want its own event", delivered)
	}

	// Nothing else for either of them. This is the last read on both sockets:
	// a socket that times out a read has failed, which is what protocolSilence
	// asks it to do.
	protocolSilence(t, theirs)
	protocolSilence(t, mine)
}

// The two routes an operator's screen reads answer for one customer, on a
// process holding two.
//
// It is the same leak as the one above wearing different clothes: a count that
// includes the other customer's sockets, or a list that names the other
// customer's channels. pusher.MemoryBroker is tested against this directly, but
// a broker that scoped correctly under a Grant nobody scoped would still answer
// the wrong number here.
func TestJourneyTheOperatorsRoutesAnswerForOneTenantOnly(t *testing.T) {
	f := newJourneyFixture(t, nil)

	ana := journeyCaller{id: "ana", tenant: tests.Tenant}
	bruno := journeyCaller{id: "bruno", tenant: journeyOtherTenant}

	mine := f.open(t, ana)
	protocolSubscribe(t, mine, journeyChannel)
	protocolSubscribe(t, mine, "ledger.a")

	theirs := f.open(t, bruno)
	protocolSubscribe(t, theirs, journeyChannel)
	protocolSubscribe(t, theirs, "ledger.b")

	for _, who := range []journeyCaller{ana, bruno} {
		if held := f.connections(t, who); held != 1 {
			t.Fatalf("%s of %s was told this process holds %d sockets, want 1: the other customer's socket is on the same process", who.id, who.tenant, held)
		}
	}

	// Each customer's own two channels, and neither one's private third name.
	// The shared name appears in both lists and cannot tell them apart, which is
	// exactly why each side also holds a name the other never asked for.
	for _, one := range []struct {
		who    journeyCaller
		theirs string
		others string
	}{{ana, "ledger.a", "ledger.b"}, {bruno, "ledger.b", "ledger.a"}} {
		listed := f.channels(t, one.who)
		if _, held := listed[journeyChannel]; !held {
			t.Fatalf("%s of %s was listed %v, want the shared name it is on", one.who.id, one.who.tenant, listed)
		}
		if _, held := listed[one.theirs]; !held {
			t.Fatalf("%s of %s was listed %v, want %q", one.who.id, one.who.tenant, listed, one.theirs)
		}
		if _, held := listed[one.others]; held {
			t.Fatalf("%s of %s was listed %q, which is a channel of the other customer: the list route publishes one customer's channel names to another", one.who.id, one.who.tenant, one.others)
		}
		if len(listed) != 2 {
			t.Fatalf("%s of %s was listed %v, want its own two channels", one.who.id, one.who.tenant, listed)
		}
	}
}

// One person with two tabs open: both of them receive, and closing one does not
// silence the other.
//
// A channel is a group of sockets and not a group of people, and this is where
// that shows. Every plausible way of getting it wrong -- keying subscribers by
// subject, unsubscribing a subject rather than a connection, dropping the
// channel when its first socket goes -- leaves one tab of one customer silently
// deaf, which is the kind of failure that is reported as "it works on my other
// screen".
func TestJourneyOneSubjectsSecondSocketHearsToo(t *testing.T) {
	f := newJourneyFixture(t, nil)

	ana := journeyCaller{id: "ana", tenant: tests.Tenant}
	first := f.open(t, ana)
	second := f.open(t, ana)

	protocolSubscribe(t, first, journeyChannel)
	protocolSubscribe(t, second, journeyChannel)

	f.publish(t, ana, journeyChannel, "OrderShipped", `{"id":17}`)

	for i, tab := range []*ws.Conn{first, second} {
		delivered := journeyNext(t, tab)
		if delivered.Event != "OrderShipped" || string(delivered.Data) != `{"id":17}` {
			t.Fatalf("tab %d received %+v, want the event both tabs are subscribed to", i+1, delivered)
		}
	}

	// One tab closes, and the close is waited for rather than assumed: Close
	// answers here immediately and the server acts on it in the goroutine
	// holding that socket, so publishing straight away would prove nothing.
	_ = first.Close()
	f.waitForConnections(t, ana, 1)

	f.publish(t, ana, journeyChannel, "OrderShipped", `{"id":18}`)

	delivered := journeyNext(t, second)
	if delivered.Event != "OrderShipped" || string(delivered.Data) != `{"id":18}` {
		t.Fatalf("the tab that stayed open received %+v after the other one closed, want the event", delivered)
	}
}
