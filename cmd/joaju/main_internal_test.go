package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/arandu-io/joaju"
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
