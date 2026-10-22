package main

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/arandu-io/joaju/ws"
)

// heldSocket is a next handler that stands in for a socket: it does not return
// until the test lets it, which is what the server does for as long as a
// socket is open.
type heldSocket struct {
	entered chan struct{}
	release chan struct{}
}

func newHeldSocket() *heldSocket {
	return &heldSocket{entered: make(chan struct{}, 64), release: make(chan struct{})}
}

func (h *heldSocket) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	h.entered <- struct{}{}
	<-h.release
	w.WriteHeader(http.StatusSwitchingProtocols)
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// socketRequest is the upgrade request of a browser at the given peer.
func socketRequest(peer string, forwarded ...string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/app/"+testAppKey, nil)
	r.RemoteAddr = peer
	for _, hop := range forwarded {
		r.Header.Add("X-Forwarded-For", hop)
	}

	return r
}

func TestAddressLimitRefusesTheSocketPastTheLimitAndOnlyForThatAddress(t *testing.T) {
	t.Parallel()

	held := newHeldSocket()
	limit := newAddressLimit(2, nil, quietLog(), held)

	answers := make(chan int, 8)
	open := func(peer string) {
		w := httptest.NewRecorder()
		limit.ServeHTTP(w, socketRequest(peer))
		answers <- w.Code
	}

	go open("203.0.113.7:1000")
	go open("203.0.113.7:1001")
	<-held.entered
	<-held.entered

	// The third from the same address is refused at once, without reaching
	// the server.
	go open("203.0.113.7:1002")
	select {
	case code := <-answers:
		if code != http.StatusTooManyRequests {
			t.Fatalf("the third socket of one address answered %d, want %d", code, http.StatusTooManyRequests)
		}
	case <-held.entered:
		t.Fatal("the third socket of one address reached the server")
	case <-time.After(5 * time.Second):
		t.Fatal("the third socket of one address was neither refused nor served")
	}

	// Another address is not affected by the first one's sockets.
	go open("198.51.100.9:2000")
	select {
	case <-held.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("another address was refused because of the first one's sockets")
	}

	// Closing the held sockets gives the address its allowance back.
	close(held.release)
	for range 3 {
		<-answers
	}
	w := httptest.NewRecorder()
	limit.ServeHTTP(w, socketRequest("203.0.113.7:1003"))
	if w.Code == http.StatusTooManyRequests {
		t.Fatal("an address whose sockets all closed is still refused")
	}
	limit.mu.Lock()
	n := len(limit.open)
	limit.mu.Unlock()
	if n != 0 {
		t.Fatalf("the limit still tracks %d addresses with no socket open", n)
	}
}

func TestAddressLimitCountsOnlyTheSocketRoute(t *testing.T) {
	t.Parallel()

	held := newHeldSocket()
	limit := newAddressLimit(1, nil, quietLog(), held)

	go limit.ServeHTTP(httptest.NewRecorder(), socketRequest("203.0.113.7:1000"))
	<-held.entered

	// The address is at its limit for sockets, and the API and the health
	// check still go through.
	for _, path := range []string{"/apps/3/connections", "/up"} {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.RemoteAddr = "203.0.113.7:1001"
		done := make(chan int, 1)
		go func() {
			w := httptest.NewRecorder()
			limit.ServeHTTP(w, r)
			done <- w.Code
		}()
		select {
		case <-held.entered:
		case code := <-done:
			t.Fatalf("GET %s answered %d and never reached the server", path, code)
		case <-time.After(5 * time.Second):
			t.Fatalf("GET %s was held up", path)
		}
	}
	close(held.release)
}

func TestAddressLimitReadsATrustedProxysForwardedForFromTheRight(t *testing.T) {
	t.Parallel()

	limit := newAddressLimit(1, defaultTrustedProxies, quietLog(), http.NotFoundHandler())

	for name, one := range map[string]struct {
		request *http.Request
		want    string
	}{
		"a browser connecting directly": {socketRequest("203.0.113.7:1000"), "203.0.113.7"},
		"a browser naming another address for itself": {
			socketRequest("203.0.113.7:1000", "198.51.100.1"), "203.0.113.7",
		},
		"a browser behind the proxy": {socketRequest("10.0.0.2:1000", "203.0.113.7"), "203.0.113.7"},
		"a browser behind the proxy that wrote a header of its own": {
			socketRequest("10.0.0.2:1000", "198.51.100.1, 203.0.113.7"), "203.0.113.7",
		},
		"a browser behind two proxies": {
			socketRequest("127.0.0.1:1000", "203.0.113.7, 10.0.0.2"), "203.0.113.7",
		},
		"a proxy that appended a header": {
			socketRequest("10.0.0.2:1000", "198.51.100.1", "203.0.113.7"), "203.0.113.7",
		},
		"a proxy that forwarded nothing": {socketRequest("10.0.0.2:1000"), "10.0.0.2"},
		"a hop that is not an address":   {socketRequest("10.0.0.2:1000", "203.0.113.7, unknown"), "10.0.0.2"},
		"an IPv4 peer written as IPv6":   {socketRequest("[::ffff:203.0.113.7]:1000"), "203.0.113.7"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, ok := limit.client(one.request)
			if !ok {
				t.Fatal("no client address was worked out")
			}
			if want := netip.MustParseAddr(one.want); got != want {
				t.Fatalf("client = %s, want %s", got, want)
			}
		})
	}

	// Believing nobody makes the peer the client, whatever it forwarded.
	none := newAddressLimit(1, []netip.Prefix{}, quietLog(), http.NotFoundHandler())
	if got, _ := none.client(socketRequest("10.0.0.2:1000", "203.0.113.7")); got != netip.MustParseAddr("10.0.0.2") {
		t.Fatalf("with no trusted proxy the client = %s, want the peer", got)
	}
}

// The limit, in front of the real server, holds a socket's place for as long as
// the socket is open -- which is the property the whole count rests on.
func TestTheProcessRefusesAnAddressItsSocketPastTheLimit(t *testing.T) {
	t.Parallel()

	server := newHTTPServer(newAddressLimit(2, []netip.Prefix{}, quietLog(), process(t)), quietLog())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	dialer := *ws.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second
	url := "ws://" + listener.Addr().String() + "/app/" + testAppKey

	for i := range 2 {
		conn, _, err := dialer.Dial(url, nil)
		if err != nil {
			t.Fatalf("socket %d of 2 = %v", i+1, err)
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Fatalf("reading the first frame of socket %d = %v", i+1, err)
		}
	}

	conn, response, err := dialer.Dial(url, nil)
	if err == nil {
		_ = conn.Close()
		t.Fatal("a third socket from one address opened past a limit of two")
	}
	if response == nil || response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("the third socket answered %v, want %d", response, http.StatusTooManyRequests)
	}
}
