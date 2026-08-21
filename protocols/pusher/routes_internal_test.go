package pusher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/joaju"
)

// This file is the HTTP API against a registry a test fills in.
//
// The two halves of a metrics route are what these prove: what this process
// holds, read through the Broker and the registry, and what the fleet answered.
// Adding them is the route's own work and it is the work that cannot be read
// off either half -- subscriptions are summed and members are not, and a route
// that got that backwards would report one person as two.

// routeTestRegistry is a [joaju.Registry] whose answers a test writes down.
//
// It records the tenant of every Grant it is handed, because the tenant is the
// only filter a route has and it comes off the Grant: a route that read one out
// of the path would pass this fake a Grant of one tenant and a name of another.
type routeTestRegistry struct {
	held    int
	heldErr error
	tally   joaju.FleetTally

	mu         sync.Mutex
	asked      []string
	terminated []string
}

func (r *routeTestRegistry) Connections(g auth.Grant) (int, error) {
	r.record(auth.Tenant(g))

	if r.heldErr != nil {
		return 0, r.heldErr
	}

	return r.held, nil
}

func (r *routeTestRegistry) Terminate(_ context.Context, g auth.Grant, subject string) (int, error) {
	r.record(auth.Tenant(g))

	r.mu.Lock()
	defer r.mu.Unlock()
	r.terminated = append(r.terminated, subject)

	return len(r.terminated), nil
}

func (r *routeTestRegistry) Fleet(_ context.Context, g auth.Grant, _ string) joaju.FleetTally {
	r.record(auth.Tenant(g))

	return r.tally
}

func (r *routeTestRegistry) record(tenant string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.asked = append(r.asked, tenant)
}

func (r *routeTestRegistry) tenants() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.asked...)
}

func (r *routeTestRegistry) closed() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.terminated...)
}

// The application every server in this file is.
const routeTestAppID = "app-1"

// routeTestAPI is what a [joaju.Server] would have handed [pusher.Routes].
func routeTestAPI(broker joaju.Broker, registry joaju.Registry) joaju.API {
	return joaju.API{
		AppID:       routeTestAppID,
		Broker:      broker,
		Connect:     channelTestConnectPolicy{},
		Subscribe:   channelTestJoinPolicy{},
		Registry:    registry,
		MaxBodySize: joaju.DefaultMaxBodySize,
		// Every test here causes a refusal on purpose, and the suite's output
		// is not where they are read.
		Log: slog.New(slog.DiscardHandler),
	}
}

// routeTestBroker is a Broker holding the given channels, reached under the
// tenant they were named in.
func routeTestBroker(t *testing.T, channels ...joaju.Channel) joaju.Broker {
	t.Helper()

	broker := NewMemoryBroker()
	for _, one := range channels {
		name := one.Name()
		held, err := broker.FindOrCreate(context.Background(),
			channelTestJoinGrant(t, name.Tenant(), "seeder"), name)
		if err != nil {
			t.Fatalf("seeding %s: %v", name, err)
		}
		// The broker makes the channel, so what a test set up on its own has to
		// be seated on the one the broker handed back.
		for _, subscriber := range one.Connections() {
			who := subscriber.Conn.Subject().ID
			if err := held.Subscribe(context.Background(),
				channelTestJoinGrant(t, name.Tenant(), who),
				subscriber.Conn, subscriber.Member); err != nil {
				t.Fatalf("seating %s on %s: %v", who, name, err)
			}
		}
	}

	return broker
}

// routeTestCall makes one request as a subject of the tenant, in place of
// hesape's Authenticate middleware -- which is the only thing anywhere that
// puts an auth.Subject on a request.
func routeTestCall(t *testing.T, api joaju.API, method, path, tenant, body string) (int, string) {
	t.Helper()

	var payload io.Reader
	if body != "" {
		payload = strings.NewReader(body)
	}

	request := httptest.NewRequest(method, path, payload)
	request = request.WithContext(auth.WithSubject(request.Context(), auth.Subject{ID: "caller", Tenant: tenant}))

	answer := httptest.NewRecorder()
	routes{api}.mux().ServeHTTP(answer, request)

	return answer.Code, strings.TrimSpace(answer.Body.String())
}

// routeTestGet is [routeTestCall] for the routes that read.
func routeTestGet(t *testing.T, api joaju.API, path, tenant string) (int, string) {
	t.Helper()

	return routeTestCall(t, api, http.MethodGet, "/apps/"+routeTestAppID+path, tenant, "")
}

// routeTestDecode reads one answer.
func routeTestDecode(t *testing.T, body string, into any) {
	t.Helper()

	if err := json.Unmarshal([]byte(body), into); err != nil {
		t.Fatalf("decoding %s = %v", body, err)
	}
}

// routeTestChannelBody is what the three channel routes answer with.
type routeTestChannelBody struct {
	Occupied          bool `json:"occupied"`
	SubscriptionCount int  `json:"subscription_count"`
	UserCount         int  `json:"user_count"`
}

func TestTheConnectionsRouteAddsTheFleetToWhatThisProcessHolds(t *testing.T) {
	t.Parallel()

	// Added and not reconciled: a socket is held by exactly one instance, so
	// the two counts have nothing in common to double-count.
	registry := &routeTestRegistry{held: 2, tally: joaju.FleetTally{Connections: 3}}
	api := routeTestAPI(routeTestBroker(t), registry)

	status, body := routeTestGet(t, api, "/connections", "acme")
	if status != http.StatusOK {
		t.Fatalf("counting sockets answered %d, want %d: %s", status, http.StatusOK, body)
	}
	if body != `{"connections":5}` {
		t.Fatalf("the route answered %s, want the two held here and the three the fleet answered", body)
	}

	// Both halves were asked about the tenant on the Grant, and the path names
	// only the application.
	for _, tenant := range registry.tenants() {
		if tenant != "acme" {
			t.Fatalf("the registry was asked about %q, want acme: the tenant comes off the Grant", tenant)
		}
	}
}

func TestTheConnectionsRouteFailsWhenTheRegistryRefusesTheGrant(t *testing.T) {
	t.Parallel()

	registry := &routeTestRegistry{heldErr: joaju.ErrNoGrant}
	api := routeTestAPI(routeTestBroker(t), registry)

	status, _ := routeTestGet(t, api, "/connections", "acme")
	if status != http.StatusForbidden {
		t.Fatalf("a refused count answered %d, want %d", status, http.StatusForbidden)
	}
}

func TestTheChannelRouteSumsSubscriptionsAndUnionsMembers(t *testing.T) {
	t.Parallel()

	// Bruno has a tab here and a tab on another instance. He is two
	// subscriptions and one member, which is the difference between the two
	// sums this route does.
	orders := channelTestChannel(t, "acme", "presence-orders.17")
	channelTestSubscribe(t, orders, "acme", "ana", joaju.Member{UserID: "ana"})
	channelTestSubscribe(t, orders, "acme", "bruno", joaju.Member{UserID: "bruno"})

	registry := &routeTestRegistry{tally: joaju.FleetTally{
		Channels: map[string]joaju.ChannelTally{
			"presence-orders.17": {
				Subscriptions: 3,
				Users:         map[string]bool{"bruno": true, "carla": true},
			},
		},
	}}
	api := routeTestAPI(routeTestBroker(t, orders), registry)

	status, body := routeTestGet(t, api, "/channels/presence-orders.17", "acme")
	if status != http.StatusOK {
		t.Fatalf("asking about one channel answered %d, want %d: %s", status, http.StatusOK, body)
	}

	var one routeTestChannelBody
	routeTestDecode(t, body, &one)
	if !one.Occupied || one.SubscriptionCount != 5 {
		t.Fatalf("the route answered %+v, want the two subscriptions here and the three the fleet answered", one)
	}
	if one.UserCount != 3 {
		t.Fatalf("the route answered %d members, want ana, bruno and carla -- bruno holds a socket on each instance and is one person", one.UserCount)
	}
}

func TestTheChannelUsersRouteUnionsTheFleetsMembers(t *testing.T) {
	t.Parallel()

	orders := channelTestChannel(t, "acme", "presence-orders.17")
	channelTestSubscribe(t, orders, "acme", "ana", joaju.Member{UserID: "ana"})
	channelTestSubscribe(t, orders, "acme", "bruno", joaju.Member{UserID: "bruno"})

	registry := &routeTestRegistry{tally: joaju.FleetTally{
		Channels: map[string]joaju.ChannelTally{
			"presence-orders.17": {
				Subscriptions: 3,
				Users:         map[string]bool{"bruno": true, "carla": true},
			},
		},
	}}
	api := routeTestAPI(routeTestBroker(t, orders), registry)

	status, body := routeTestGet(t, api, "/channels/presence-orders.17/users", "acme")
	if status != http.StatusOK {
		t.Fatalf("asking for the members answered %d, want %d: %s", status, http.StatusOK, body)
	}

	var members struct {
		Users []struct {
			ID string `json:"id"`
		} `json:"users"`
	}
	routeTestDecode(t, body, &members)

	named := make([]string, 0, len(members.Users))
	for _, user := range members.Users {
		named = append(named, user.ID)
	}
	slices.Sort(named)

	// A union and never a concatenation: bruno is on both instances and is
	// named once.
	if !slices.Equal(named, []string{"ana", "bruno", "carla"}) {
		t.Fatalf("the members were %v, want ana, bruno and carla once each", named)
	}
}

func TestTheChannelListNamesAChannelOnlyTheFleetHolds(t *testing.T) {
	t.Parallel()

	orders := channelTestChannel(t, "acme", "presence-orders.17")
	channelTestSubscribe(t, orders, "acme", "ana", joaju.Member{UserID: "ana"})

	registry := &routeTestRegistry{tally: joaju.FleetTally{
		Channels: map[string]joaju.ChannelTally{
			"presence-orders.17": {Subscriptions: 1, Users: map[string]bool{"bruno": true}},
			// A channel not one socket of this instance is on. Left out, the
			// list would tell a customer they are talking on fewer channels
			// than they are.
			"invoices.9": {Subscriptions: 2},
			// A presence channel this instance has never seen still publishes
			// how many people are on it.
			"presence-invoices.9": {Subscriptions: 2, Users: map[string]bool{"carla": true, "dora": true}},
		},
	}}
	api := routeTestAPI(routeTestBroker(t, orders), registry)

	status, body := routeTestGet(t, api, "/channels", "acme")
	if status != http.StatusOK {
		t.Fatalf("listing the channels answered %d, want %d: %s", status, http.StatusOK, body)
	}
	// The key is the name the client asked for: the tenant they are held under
	// is not its to read back.
	if strings.Contains(body, "acme:") {
		t.Fatalf("the channel list carried the tenant: %s", body)
	}

	var listed struct {
		Channels map[string]routeTestChannelBody `json:"channels"`
	}
	routeTestDecode(t, body, &listed)

	held, named := listed.Channels["presence-orders.17"]
	if !named || !held.Occupied || held.UserCount != 2 {
		t.Fatalf("the list says %+v about presence-orders.17, want it occupied with ana and bruno", held)
	}
	elsewhere, named := listed.Channels["invoices.9"]
	if !named {
		t.Fatalf("the list is %v, want the channel only the fleet holds in it", listed.Channels)
	}
	if !elsewhere.Occupied {
		t.Fatalf("the list says %+v about invoices.9, want it occupied: somebody is on it, just not here", elsewhere)
	}
	if elsewhere.UserCount != 0 {
		t.Fatalf("the list says %+v about invoices.9, and a channel that is not a presence one has no members", elsewhere)
	}
	unseen, named := listed.Channels["presence-invoices.9"]
	if !named || unseen.UserCount != 2 {
		t.Fatalf("the list says %+v about presence-invoices.9, want the two people the fleet named", unseen)
	}
}

func TestTheTerminateRouteClosesThroughTheRegistry(t *testing.T) {
	t.Parallel()

	registry := &routeTestRegistry{}
	api := routeTestAPI(routeTestBroker(t), registry)

	status, body := routeTestCall(t, api, http.MethodPost,
		"/apps/"+routeTestAppID+"/users/u-17/terminate_connections", "acme", "")
	if status != http.StatusOK {
		t.Fatalf("terminating answered %d, want %d: %s", status, http.StatusOK, body)
	}

	if closed := registry.closed(); !slices.Equal(closed, []string{"u-17"}) {
		t.Fatalf("the registry was asked to close %v, want the user id in the path", closed)
	}
	// The path names the user and the Grant names the tenant. A caller naming
	// another customer's user id closes nothing, because the registry is asked
	// about both.
	for _, tenant := range registry.tenants() {
		if tenant != "acme" {
			t.Fatalf("the registry was asked about %q, want acme", tenant)
		}
	}
}

func TestTheHealthRouteReadsNothing(t *testing.T) {
	t.Parallel()

	// No API at all: the route holds nothing, so a zero one is enough to serve
	// it. That is the whole of why it needs no Grant.
	answer := httptest.NewRecorder()
	routes{}.mux().ServeHTTP(answer, httptest.NewRequest(http.MethodGet, "/up", nil))

	if answer.Code != http.StatusOK {
		t.Fatalf("GET /up answered %d, want %d", answer.Code, http.StatusOK)
	}
	if got := strings.TrimSpace(answer.Body.String()); got != "OK" {
		t.Fatalf("GET /up answered %q, want OK", got)
	}
}

func TestARouteOfAnotherAppIsNotFound(t *testing.T) {
	t.Parallel()

	api := routeTestAPI(routeTestBroker(t), &routeTestRegistry{})

	for _, path := range []string{
		"/apps/other/channels",
		"/apps/other/channels/orders.17",
		"/apps/other/channels/orders.17/users",
		"/apps/other/connections",
	} {
		status, _ := routeTestCall(t, api, http.MethodGet, path, "acme", "")
		if status != http.StatusNotFound {
			t.Fatalf("GET %s answered %d, want %d", path, status, http.StatusNotFound)
		}
	}
}

func TestAPublishedEventIsHandedToTheFleetAfterTheLocalDelivery(t *testing.T) {
	t.Parallel()

	orders := channelTestChannel(t, "acme", "orders.17")
	channelTestSubscribe(t, orders, "acme", "ana", joaju.Member{})

	carried := &routeTestCarrier{Broker: routeTestBroker(t, orders)}
	api := routeTestAPI(carried, &routeTestRegistry{})

	status, body := routeTestCall(t, api, http.MethodPost, "/apps/"+routeTestAppID+"/events", "acme",
		`{"name":"order.paid","channel":"orders.17","data":"{\"id\":17}","socket_id":"1234.5678"}`)
	if status != http.StatusOK {
		t.Fatalf("publishing answered %d, want %d: %s", status, http.StatusOK, body)
	}

	// The fleet hears about it after the local delivery and never instead of
	// it: the sockets held here were served by the broadcast.
	events := carried.events()
	if len(events) != 1 {
		t.Fatalf("the fleet was handed %d events, want the one that was published", len(events))
	}
	if events[0].Name != "order.paid" || events[0].Channel.Tenant() != "acme" {
		t.Fatalf("the fleet was handed %+v, want acme's order.paid", events[0])
	}
	// The excluded socket travels, because the exclusion is fleet-wide: the
	// socket that published is held by whichever instance it dialled.
	if events[0].Socket != joaju.SocketID("1234.5678") {
		t.Fatalf("the excluded socket is %q, want 1234.5678", events[0].Socket)
	}
}

func TestAnEventOnAChannelNobodyHereHoldsStillReachesTheFleet(t *testing.T) {
	t.Parallel()

	// This instance holds no channel under that name, and the instance holding
	// the sockets that are on it is usually not the one that took the request.
	carried := &routeTestCarrier{Broker: routeTestBroker(t)}
	api := routeTestAPI(carried, &routeTestRegistry{})

	status, body := routeTestCall(t, api, http.MethodPost, "/apps/"+routeTestAppID+"/events", "acme",
		`{"name":"order.paid","channel":"orders.17","data":"{\"id\":17}"}`)
	if status != http.StatusOK {
		t.Fatalf("publishing to a channel nobody here holds answered %d, want %d: %s", status, http.StatusOK, body)
	}
	if events := carried.events(); len(events) != 1 {
		t.Fatalf("the fleet was handed %d events, want the one that was published", len(events))
	}
}

// routeTestCarrier is a Broker that is also a [joaju.Carrier], which is what
// [joaju.RelayedBroker] makes one. It records what the fleet was handed.
type routeTestCarrier struct {
	joaju.Broker

	mu      sync.Mutex
	carried []joaju.Event
}

func (b *routeTestCarrier) Carry(_ context.Context, e joaju.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.carried = append(b.carried, e)
}

func (b *routeTestCarrier) events() []joaju.Event {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]joaju.Event(nil), b.carried...)
}

var _ joaju.Carrier = (*routeTestCarrier)(nil)
