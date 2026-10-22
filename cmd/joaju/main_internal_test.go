package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/arandu-io/joaju"
	"github.com/arandu-io/joaju/ws"
)

// The process as main assembles it: the front door in front of the server
// newServer builds, so what is proven here is the wiring and not a policy taken
// out of it.
const processTestAppID = "3"

// process builds the handler run() serves, with the test clock in the front
// door so a signature stamped at testClock is inside the window.
func process(t *testing.T) http.Handler {
	t.Helper()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := newServer(config{
		AppID:     processTestAppID,
		AppKey:    testAppKey,
		AppSecret: testAppSecret,
		Tenant:    testTenant,
	}, log)
	if err != nil {
		t.Fatalf("newServer() = %v", err)
	}
	t.Cleanup(func() { server.Close(context.Background()) })

	return frontDoor{
		appKey:  testAppKey,
		secret:  []byte(testAppSecret),
		tenant:  testTenant,
		maxBody: joaju.DefaultMaxBodySize,
		now:     func() time.Time { return testClock },
		log:     log,
		next:    server,
	}
}

// signedJSON is a request the application would make: signed with the app
// secret, and declared JSON whenever it is a POST, which is what every Pusher
// server library sends.
func signedJSON(t *testing.T, method, path, body string) *http.Request {
	t.Helper()

	r := sign(t, method, path, body)
	if method == http.MethodPost {
		r.Header.Set("Content-Type", "application/json")
	}

	return r
}

func TestTheProcessServesTheApplicationThatSignedWithTheAppSecret(t *testing.T) {
	t.Parallel()

	handler := process(t)
	for _, call := range []struct{ method, path, body string }{
		{http.MethodPost, "/apps/3/events", `{"name":"order.placed","channel":"private-orders.17","data":"{}"}`},
		{http.MethodPost, "/apps/3/batch_events", `{"batch":[{"name":"order.placed","channel":"orders.17","data":"{}"}]}`},
		{http.MethodGet, "/apps/3/connections", ``},
		{http.MethodPost, "/apps/3/users/user-7/terminate_connections", `{}`},
	} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, signedJSON(t, call.method, call.path, call.body))
		if w.Code != http.StatusOK {
			t.Errorf("%s %s answered %d %s, want %d: the signed application lost the API", call.method, call.path, w.Code, w.Body, http.StatusOK)
		}
	}
}

func TestTheProcessRefusesTheAPIToAnUnsignedCaller(t *testing.T) {
	t.Parallel()

	handler := process(t)
	r := httptest.NewRequest(http.MethodPost, "/apps/3/events", nil)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("an unsigned publish answered %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestTheHTTPServerBoundsEveryPhaseOfARequest(t *testing.T) {
	t.Parallel()

	server := newHTTPServer(http.NotFoundHandler(), slog.New(slog.NewTextHandler(io.Discard, nil)))

	for name, bound := range map[string]time.Duration{
		"ReadHeaderTimeout": server.ReadHeaderTimeout,
		"ReadTimeout":       server.ReadTimeout,
		"WriteTimeout":      server.WriteTimeout,
		"IdleTimeout":       server.IdleTimeout,
	} {
		if bound <= 0 {
			t.Errorf("%s = %s: a client may hold a connection in that phase for as long as it likes", name, bound)
		}
	}
	if server.ReadHeaderTimeout > server.ReadTimeout {
		t.Errorf("ReadHeaderTimeout %s is longer than ReadTimeout %s, which already bounds the headers", server.ReadHeaderTimeout, server.ReadTimeout)
	}
}

// The deadlines net/http puts on a connection stay on it when the upgrade
// hijacks it. A socket has to outlive them, because the server replaces both
// before it reads or writes a frame.
func TestASocketOutlivesTheHTTPServersTimeouts(t *testing.T) {
	t.Parallel()

	server := newHTTPServer(process(t), slog.New(slog.NewTextHandler(io.Discard, nil)))
	const short = 200 * time.Millisecond
	server.ReadHeaderTimeout, server.ReadTimeout, server.WriteTimeout, server.IdleTimeout = short, short, short, short

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	dialer := *ws.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second
	conn, _, err := dialer.Dial("ws://"+listener.Addr().String()+"/app/"+testAppKey, nil)
	if err != nil {
		t.Fatalf("dialling = %v", err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, first, err := conn.ReadMessage(); err != nil || !strings.Contains(string(first), "pusher:connection_established") {
		t.Fatalf("the first frame = %q, %v; want pusher:connection_established", first, err)
	}

	// Well past every timeout of the http.Server.
	time.Sleep(4 * short)

	if err := conn.WriteMessage(ws.TextMessage, []byte(`{"event":"pusher:ping","data":{}}`)); err != nil {
		t.Fatalf("writing a ping after the http.Server's timeouts = %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, answer, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("the socket ended once the http.Server's timeouts passed: %v", err)
	}
	if !strings.Contains(string(answer), "pusher:pong") {
		t.Fatalf("the answer to a ping = %q, want pusher:pong", answer)
	}
}

// The channel list is the collection question: the route asks the
// SubscriptionPolicy about no channel in particular before it lists them.
func TestTheProcessListsChannelsForTheApplication(t *testing.T) {
	t.Parallel()

	handler := process(t)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, signedJSON(t, http.MethodGet, "/apps/3/channels", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("GET /apps/3/channels answered %d %s, want %d", w.Code, w.Body, http.StatusOK)
	}
	if body := strings.TrimSpace(w.Body.String()); body != `{"channels":{}}` {
		t.Fatalf("an empty process listed %s, want no channels", body)
	}
}
