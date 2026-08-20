package pusher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/arandu-io/hesape/auth"
	"github.com/arandu-io/hesape/broadcasting"
	"github.com/arandu-io/joaju"
)

// This file is the HTTP half of the Pusher protocol: the eight routes a client
// calls instead of opening a socket.
//
// They are here and not in [joaju] for the reason the frames are: a wire format
// is what a client speaks over a socket AND what it calls over HTTP, and
// /apps/{appId}/events is as much this protocol's as pusher:subscribe is. The
// server owns the socket, hands a [joaju.API] to [joaju.Protocol.Routes], and
// mounts what comes back.
//
// # The authorization shape, which every route follows
//
//	[joaju.ConnectPolicy]       once per request  may this subject be here at all
//	[joaju.SubscriptionPolicy]  once per channel  may this subject reach this channel
//
// Both run here and not only on the socket, because listing channels, counting
// subscribers and reading a presence channel's members are reads of who is
// talking to whom, and there is no exception for reads. A dashboard that lists
// channels without a policy is a tenant boundary that holds everywhere except
// the dashboard.
//
// A [joaju.Handshake] is what the ConnectPolicy is asked about here as well,
// with Socket empty -- an API caller is asking the same question a browser
// asks, minus the socket it wants opened. Inventing a third auth.Action for it
// would mean a third policy an application has to remember to write, and the
// question it would answer is the one Connect already answers.
//
// The Connect Grant is also what a [joaju.ChannelName] is built from here,
// because a name needs a tenant and a tenant comes off a Grant. The channel a
// caller named in the path is never trusted for that: it supplies the name
// after the tenant, and nothing else.
//
// # What is NOT here
//
// The socket route. An upgrade is the transport's: the server compares the app
// key, mints the socket id, runs the ConnectPolicy and hands what comes out to
// a [joaju.Protocol] as a [joaju.Connection]. Nothing in this package is ever
// given an http.ResponseWriter it may hijack.
//
// Any credential of its own. These routes verify no app_secret HMAC: a
// standalone socket server has to, having no session to read, but a mounted one
// has already been through the host application's front door, and a second
// credential would be a second way to prove who is calling.

// routes is the Pusher HTTP API, built out of everything the server lets a
// route reach.
//
// It embeds the [joaju.API] rather than holding it in a field so that a handler
// reads Broker, Registry and Subscribe by their own names -- the whole of this
// type's state is that value, and the receiver is spelled api because that is
// what it is.
type routes struct {
	joaju.API
}

// Routes are the Pusher HTTP API, on the patterns the protocol names them.
func (p *pusher) Routes(api joaju.API) http.Handler {
	return routes{api}.mux()
}

// mux registers every route on a ServeMux of its own.
//
// It is the protocol's mux and not the server's, so the patterns here read as
// the protocol's list: what a client may call, in one place, beside the frames
// it would otherwise have sent.
func (api routes) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /apps/{appId}/events", api.events)
	mux.HandleFunc("POST /apps/{appId}/batch_events", api.batchEvents)
	mux.HandleFunc("GET /apps/{appId}/connections", api.connections)
	mux.HandleFunc("GET /apps/{appId}/channels", api.channels)
	mux.HandleFunc("GET /apps/{appId}/channels/{channel}", api.channel)
	mux.HandleFunc("GET /apps/{appId}/channels/{channel}/users", api.channelUsers)
	mux.HandleFunc("POST /apps/{appId}/users/{userId}/terminate_connections", api.terminate)
	mux.HandleFunc("GET /up", up)

	return mux
}

// publishRequest is the body of POST /apps/{appId}/events, and one element of
// the batch of POST /apps/{appId}/batch_events.
//
// Data is a string and not a json.RawMessage because the Pusher protocol says
// so: the payload travels as a JSON string containing JSON, in both directions.
// [joaju.Event.Data] holds it decoded, which is the one place the double
// encoding is undone.
type publishRequest struct {
	Name     string   `json:"name"`
	Data     string   `json:"data"`
	Channel  string   `json:"channel"`
	Channels []string `json:"channels"`
	SocketID string   `json:"socket_id"`
}

// events is POST /apps/{appId}/events: publish one event.
func (api routes) events(w http.ResponseWriter, r *http.Request) {
	grant, ok := api.enter(w, r)
	if !ok {
		return
	}

	var body publishRequest
	if !api.decode(w, r, &body) {
		return
	}
	if err := api.publish(r.Context(), grant, body); err != nil {
		api.fail(w, r, err)

		return
	}

	writeJSON(w, http.StatusOK, struct{}{})
}

// batchEvents is POST /apps/{appId}/batch_events: publish many.
func (api routes) batchEvents(w http.ResponseWriter, r *http.Request) {
	grant, ok := api.enter(w, r)
	if !ok {
		return
	}

	var body struct {
		Batch []publishRequest `json:"batch"`
	}
	if !api.decode(w, r, &body) {
		return
	}
	// Each element is authorized on its own. A batch is a convenience for the
	// caller and never a way to reach a channel one of its elements could not
	// have reached alone.
	for _, one := range body.Batch {
		if err := api.publish(r.Context(), grant, one); err != nil {
			api.fail(w, r, err)

			return
		}
	}

	writeJSON(w, http.StatusOK, struct{}{})
}

// publish delivers one event to every channel it names, on this instance and on
// the others.
//
// A channel nobody is subscribed to HERE is not an error, and it is not the end
// of the event either: this instance holds no channel under that name, and the
// instance holding the sockets that are on it is usually not the one that took
// the request. So the local delivery is skipped and [carry] runs anyway. On a
// deployment of one that is the same "delivered to no one" it always was.
func (api routes) publish(ctx context.Context, connected auth.Grant, body publishRequest) error {
	if body.Name == "" {
		return errors.New("joaju: an event needs a name")
	}
	// The two reserved namespaces are the server's own voice. A client that
	// could publish pusher_internal:member_added could invent members, and one
	// that could publish pusher:error could tell another client its
	// subscription was refused.
	if strings.HasPrefix(body.Name, joaju.ProtocolPrefix) || strings.HasPrefix(body.Name, joaju.InternalPrefix) {
		return fmt.Errorf("joaju: %q is a reserved event name, and only the server may send one", body.Name)
	}
	// The payload is a JSON string containing JSON, and this is where the outer
	// string is undone. It is checked here rather than trusted, because
	// [joaju.Event.Data] is a json.RawMessage from here on and an invalid one
	// fails at the far end of a fanout, on somebody else's socket.
	if !json.Valid([]byte(body.Data)) {
		return errors.New("joaju: the data of an event has to be JSON")
	}

	requested := body.Channels
	if body.Channel != "" {
		requested = append([]string{body.Channel}, requested...)
	}
	if len(requested) == 0 {
		return errors.New("joaju: an event needs a channel")
	}

	for _, one := range requested {
		name, err := joaju.NewChannelName(connected, one)
		if err != nil {
			return err
		}
		grant, err := api.reach(ctx, connected.Subject(), name)
		if err != nil {
			return err
		}

		event := joaju.Event{
			Name:    body.Name,
			Channel: name,
			Data:    json.RawMessage(body.Data),
			Socket:  joaju.SocketID(body.SocketID),
		}

		channel, err := api.Broker.Find(ctx, grant, name)
		switch {
		case errors.Is(err, joaju.ErrNoChannel):
			// Nobody here is on it. The fleet still hears about it, below.
		case err != nil:
			return err
		default:
			// Broadcast and not BroadcastToAll: an empty [joaju.Event.Socket]
			// already means everybody, so the sender is excluded when there is
			// one and nobody is when there is not, through one call.
			if err := channel.Broadcast(ctx, event); err != nil {
				return err
			}
		}

		// The other instances, after the local delivery and never instead of
		// it. This does not fail the request -- see [carry].
		carry(ctx, api.Broker, event)
	}

	return nil
}

// connections is GET /apps/{appId}/connections: how many sockets.
func (api routes) connections(w http.ResponseWriter, r *http.Request) {
	grant, ok := api.enter(w, r)
	if !ok {
		return
	}

	open, err := api.Registry.Connections(grant)
	if err != nil {
		api.fail(w, r, err)

		return
	}

	// Added and not reconciled: a socket is held by exactly one instance, so
	// the fleet's count and this one's have nothing in common to double-count.
	open += api.Registry.Fleet(r.Context(), grant, "").Connections

	writeJSON(w, http.StatusOK, map[string]any{"connections": open})
}

// channels is GET /apps/{appId}/channels: the channel list.
func (api routes) channels(w http.ResponseWriter, r *http.Request) {
	connected, ok := api.enter(w, r)
	if !ok {
		return
	}

	// The collection question, asked of the same policy that answers about one
	// channel: auth.Policy is asked about the zero resource for a collection
	// action, and a Grant issued for anything but broadcasting.ChannelJoin
	// reaches no Broker method.
	grant, err := auth.Authorize(r.Context(), api.Subscribe, connected.Subject(), broadcasting.ChannelJoin, joaju.Subscription{})
	if err != nil {
		api.fail(w, r, err)

		return
	}

	channels, err := api.Broker.All(r.Context(), grant)
	if err != nil {
		api.fail(w, r, err)

		return
	}

	elsewhere := api.Registry.Fleet(r.Context(), grant, "")

	// The key is [joaju.ChannelName.Requested] and never
	// [joaju.ChannelName.String]: the caller asked about its own channels and
	// the tenant they are held under is not its to read back.
	listed := make(map[string]any, len(channels)+len(elsewhere.Channels))
	for _, channel := range channels {
		requested := channel.Name().Requested()
		fleet := elsewhere.Channel(requested)
		entry := map[string]any{"occupied": len(channel.Connections())+fleet.Subscriptions > 0}
		if channel.Name().Type().Presence() {
			entry["user_count"] = countMembers(channel, fleet.Users)
		}
		listed[requested] = entry
	}
	// A channel every subscriber of which is on another instance is still a
	// channel this tenant has, and leaving it out would make the list say a
	// customer is talking on fewer channels than they are.
	for requested, fleet := range elsewhere.Channels {
		if _, held := listed[requested]; held {
			continue
		}

		entry := map[string]any{"occupied": fleet.Subscriptions > 0}
		if fleetPresence(grant, requested) {
			entry["user_count"] = len(fleet.Users)
		}
		listed[requested] = entry
	}

	writeJSON(w, http.StatusOK, map[string]any{"channels": listed})
}

// channel is GET /apps/{appId}/channels/{channel}: one channel.
func (api routes) channel(w http.ResponseWriter, r *http.Request) {
	held, grant, ok := api.find(w, r)
	if !ok {
		return
	}

	name := held.Name()
	fleet := api.Registry.Fleet(r.Context(), grant, name.Requested()).Channel(name.Requested())

	// A subscription is one socket on one channel and one socket is on one
	// instance, so these add. The member count below does not: two tabs on two
	// instances are counted by both and are one person.
	subscribers := len(held.Connections()) + fleet.Subscriptions
	body := map[string]any{
		"occupied":           subscribers > 0,
		"subscription_count": subscribers,
	}
	if name.Type().Presence() {
		body["user_count"] = countMembers(held, fleet.Users)
	}

	writeJSON(w, http.StatusOK, body)
}

// channelUsers is GET /apps/{appId}/channels/{channel}/users: the members of a
// presence channel.
func (api routes) channelUsers(w http.ResponseWriter, r *http.Request) {
	held, grant, ok := api.find(w, r)
	if !ok {
		return
	}
	if !held.Name().Type().Presence() {
		api.refuse(w, r, http.StatusBadRequest, "Only a presence channel has users.", nil)

		return
	}

	requested := held.Name().Requested()
	fleet := api.Registry.Fleet(r.Context(), grant, requested).Channel(requested)

	seen := make(map[string]bool)
	users := make([]map[string]string, 0, len(held.Connections())+len(fleet.Users))
	for _, subscriber := range held.Connections() {
		// One member may hold several sockets on one channel -- two tabs -- and
		// the member list names people, not sockets.
		if id := subscriber.Member.UserID; id != "" && !seen[id] {
			seen[id] = true
			users = append(users, map[string]string{"id": id})
		}
	}
	// The two tabs may also be on two instances, which is the same person and
	// the same reason: this is a union and never a concatenation.
	for _, id := range fleet.Members() {
		if !seen[id] {
			seen[id] = true
			users = append(users, map[string]string{"id": id})
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"users": users})
}

// terminate is POST /apps/{appId}/users/{userId}/terminate_connections.
func (api routes) terminate(w http.ResponseWriter, r *http.Request) {
	grant, ok := api.enter(w, r)
	if !ok {
		return
	}

	// The tenant comes off the Grant, so a caller naming another customer's
	// user id closes nothing: the registry matches on both, and the userId in
	// the path only ever narrows what the Grant already scoped.
	if _, err := api.Registry.Terminate(r.Context(), grant, r.PathValue("userId")); err != nil {
		api.fail(w, r, err)

		return
	}

	writeJSON(w, http.StatusOK, struct{}{})
}

// up is GET /up: the health check.
//
// It is the one route with no Grant, and it can be, because it reads nothing:
// it says this process is answering, which whoever can reach the port already
// knows. It is a function and not a method for the same reason -- there is
// nothing on the [joaju.API] it could want.
func up(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// enter is the first half of every route here: the app has to be this one, the
// request has to carry a subject, and the [joaju.ConnectPolicy] has to allow
// it.
func (api routes) enter(w http.ResponseWriter, r *http.Request) (auth.Grant, bool) {
	if r.PathValue("appId") != api.AppID {
		api.refuse(w, r, http.StatusNotFound, "Unknown app.", nil)

		return auth.Grant{}, false
	}

	subject, ok := auth.SubjectFrom(r.Context())
	if !ok {
		api.refuse(w, r, http.StatusUnauthorized, "Unauthenticated.", nil)

		return auth.Grant{}, false
	}

	// Socket is empty: an API caller is asking whether it may act on this
	// server, which is the question [joaju.Connect] answers, and it is not
	// asking for a socket to be opened. Origin is carried through because a
	// browser calling the API sends one and the policy that judges it is the
	// same one.
	grant, err := auth.Authorize(r.Context(), api.Connect, subject, joaju.Connect, joaju.Handshake{
		Origin: r.Header.Get("Origin"),
	})
	if err != nil {
		api.fail(w, r, err)

		return auth.Grant{}, false
	}

	return grant, true
}

// find is the second half of the two routes that name one channel:
// [routes.enter], then the name built out of the Grant, then the
// [joaju.SubscriptionPolicy], then the Broker.
//
// The Grant comes back with the channel because both routes have a second half
// of their own -- [joaju.Registry.Fleet], which reads the tenant off it and off
// nothing that arrived with the request.
func (api routes) find(w http.ResponseWriter, r *http.Request) (joaju.Channel, auth.Grant, bool) {
	connected, ok := api.enter(w, r)
	if !ok {
		return nil, auth.Grant{}, false
	}

	name, err := joaju.NewChannelName(connected, r.PathValue("channel"))
	if err != nil {
		api.fail(w, r, err)

		return nil, auth.Grant{}, false
	}
	grant, err := api.reach(r.Context(), connected.Subject(), name)
	if err != nil {
		api.fail(w, r, err)

		return nil, auth.Grant{}, false
	}

	held, err := api.Broker.Find(r.Context(), grant, name)
	if err != nil {
		api.fail(w, r, err)

		return nil, auth.Grant{}, false
	}

	return held, grant, true
}

// reach runs the [joaju.SubscriptionPolicy] for one channel and answers the
// Grant it issued, which is the only thing a [joaju.Broker] accepts.
//
// The Socket of the [joaju.Subscription] is empty, and stays empty: a route is
// a request and holds no socket, so a policy that reads one is being told the
// truth about which one asked.
func (api routes) reach(ctx context.Context, subject auth.Subject, name joaju.ChannelName) (auth.Grant, error) {
	return auth.Authorize(ctx, api.Subscribe, subject, broadcasting.ChannelJoin, joaju.Subscription{
		Channel: name,
	})
}

// decode reads a JSON body of at most [joaju.API.MaxBodySize].
func (api routes) decode(w http.ResponseWriter, r *http.Request, into any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, api.MaxBodySize)
	// Unknown fields are ignored rather than refused: the Pusher HTTP API has
	// fields this server does not read -- "info" is one -- and a client SDK
	// that sends one is not making a mistake.
	if err := json.NewDecoder(r.Body).Decode(into); err != nil {
		api.refuse(w, r, http.StatusBadRequest, "The body is not the JSON this route reads.", err)

		return false
	}

	return true
}

// fail answers an error from a policy, a name or a broker.
//
// A refusal says only that it was refused. The sentence a Policy wrote names
// the subject and often the resource, and it goes to the log rather than to
// whoever was refused.
func (api routes) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, auth.ErrForbidden):
		api.refuse(w, r, http.StatusForbidden, "Forbidden.", err)
	case errors.Is(err, joaju.ErrNoChannel):
		api.refuse(w, r, http.StatusNotFound, "Unknown channel.", err)
	default:
		api.refuse(w, r, http.StatusBadRequest, "The request could not be served.", err)
	}
}

// refuse writes the answer and records the reason.
func (api routes) refuse(w http.ResponseWriter, r *http.Request, status int, message string, err error) {
	if err != nil {
		api.Log.InfoContext(r.Context(), "joaju: a request was refused",
			slog.String("route", r.Method+" "+r.URL.Path),
			slog.Int("status", status),
			slog.Any("error", err))
	}

	writeJSON(w, status, map[string]string{"message": message})
}

// writeJSON writes one JSON body.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// countMembers is how many distinct people a presence channel holds, which is
// not how many sockets it holds: two tabs are one member.
//
// fleet is the same channel's members on the other instances, and it is a set
// of ids rather than a count for exactly the reason this function exists: the
// two tabs may be on two instances, and adding two counts would make one person
// two people. It is empty on a deployment of one.
func countMembers(held joaju.Channel, fleet map[string]bool) int {
	seen := make(map[string]bool, len(fleet))
	for id := range fleet {
		seen[id] = true
	}
	for _, subscriber := range held.Connections() {
		if id := subscriber.Member.UserID; id != "" {
			seen[id] = true
		}
	}

	return len(seen)
}

// fleetPresence reports whether a channel only the other instances hold
// publishes its members, so that the channel list can say how many people are
// on one this instance has never seen.
//
// The kind is read through [joaju.ChannelName.Type], which is the one place a
// prefix is compared, and the name is built under the asking Grant -- so the
// [joaju.ChannelName] it goes through carries this request's own tenant and
// never one that arrived with an answer. A name the fleet answered with that
// cannot be a name here is not a presence channel and is not anything else
// either.
func fleetPresence(g auth.Grant, requested string) bool {
	name, err := joaju.NewChannelName(g, requested)

	return err == nil && name.Type().Presence()
}
