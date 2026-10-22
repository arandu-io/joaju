package main

import (
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
)

// socketPrefix is the path of the socket route, and the only route
// [addressLimit] counts.
const socketPrefix = "/app/"

// addressLimit caps how many sockets one client address may hold open at once.
//
// The library already caps sockets per tenant, and that is the right limit for
// an application with many customers. This process serves one tenant to
// anonymous browsers, so the tenant's cap is the whole process's: one address
// that opens ten thousand sockets refuses every other visitor, and nothing in
// the request identifies it as anything but a browser. A cap per address is the
// identity an anonymous socket does have.
//
// It counts only the socket route. The API is signed with the app secret and
// is the application's own traffic, and the health check reads nothing. A
// socket is counted from the moment its request arrives until the server
// returns from it, which is when the socket has closed: the server reads a
// socket on the goroutine net/http gave its upgrade request.
//
// # Which address is the client's
//
// The peer of the TCP connection, unless that peer is a trusted proxy, in
// which case the X-Forwarded-For it appended is read from the right: the first
// entry that is not itself a trusted proxy is the client. Reading from the left
// would believe whatever the browser wrote there, which is how an address
// names itself something else; reading from the right stops at the last hop a
// trusted proxy vouched for.
type addressLimit struct {
	// limit is how many sockets one address may hold. Zero or less is no limit.
	limit int
	// trusted are the peers whose X-Forwarded-For is believed.
	trusted []netip.Prefix
	log     *slog.Logger
	next    http.Handler

	mu   sync.Mutex
	open map[netip.Addr]int
}

// newAddressLimit builds the limit in front of next.
func newAddressLimit(limit int, trusted []netip.Prefix, log *slog.Logger, next http.Handler) *addressLimit {
	return &addressLimit{
		limit:   limit,
		trusted: trusted,
		log:     log,
		next:    next,
		open:    make(map[netip.Addr]int),
	}
}

// ServeHTTP counts a socket request against its address, refuses it with 429
// when the address already holds the limit, and otherwise hands it on and
// takes it off the count when the socket is gone.
func (l *addressLimit) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if l.limit <= 0 || r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, socketPrefix) {
		l.next.ServeHTTP(w, r)
		return
	}

	client, ok := l.client(r)
	if !ok {
		// A peer address net/http could not report is not one a limit can be
		// kept for, and refusing it would refuse a socket for the server's own
		// failure to say who it is.
		l.next.ServeHTTP(w, r)
		return
	}

	if !l.take(client) {
		l.log.InfoContext(r.Context(), "joaju: a socket was refused by the per-address limit",
			slog.String("address", client.String()),
			slog.Int("limit", l.limit),
			// The one mistake that makes this fire for everybody at once.
			slog.String("hint", "if this address is a reverse proxy, list it in "+envPrefix+"TRUSTED_PROXIES"))
		http.Error(w, "Too many sockets from this address.", http.StatusTooManyRequests)
		return
	}
	defer l.release(client)

	l.next.ServeHTTP(w, r)
}

// take counts one more socket for the address, unless it is already at the
// limit.
func (l *addressLimit) take(client netip.Addr) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.open[client] >= l.limit {
		return false
	}
	l.open[client]++

	return true
}

// release counts one socket fewer, and forgets an address that holds none, so
// the map is as large as the set of addresses holding a socket right now and
// not the set that ever held one.
func (l *addressLimit) release(client netip.Addr) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.open[client] <= 1 {
		delete(l.open, client)
		return
	}
	l.open[client]--
}

// client is the address a request is counted against. See [addressLimit] for
// how a trusted proxy's X-Forwarded-For is read.
func (l *addressLimit) client(r *http.Request) (netip.Addr, bool) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return netip.Addr{}, false
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, false
	}
	peer = peer.Unmap().WithZone("")
	if !l.believes(peer) {
		return peer, true
	}

	hops := forwardedFor(r.Header.Values("X-Forwarded-For"))
	for i := len(hops) - 1; i >= 0; i-- {
		hop, err := netip.ParseAddr(hops[i])
		if err != nil {
			// A hop the proxy wrote that is not an address is the end of what
			// can be read; what lies to its left was written by somebody the
			// proxy did not vouch for.
			return peer, true
		}
		hop = hop.Unmap().WithZone("")
		if !l.believes(hop) {
			return hop, true
		}
		peer = hop
	}

	// Every hop is a trusted proxy, or there were none: the nearest of them is
	// the most specific address there is.
	return peer, true
}

// believes reports whether an address is one of the trusted proxies.
func (l *addressLimit) believes(addr netip.Addr) bool {
	for _, prefix := range l.trusted {
		if prefix.Contains(addr) {
			return true
		}
	}

	return false
}

// forwardedFor splits every X-Forwarded-For header into its hops, in order. A
// proxy may append a header of its own rather than extend the last one, so the
// headers are one list.
func forwardedFor(headers []string) []string {
	var hops []string
	for _, header := range headers {
		for _, hop := range strings.Split(header, ",") {
			if hop = strings.TrimSpace(hop); hop != "" {
				hops = append(hops, hop)
			}
		}
	}

	return hops
}
