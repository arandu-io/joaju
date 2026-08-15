package ws

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// echoServer upgrades and sends back whatever it reads.
func echoServer(t *testing.T) *httptest.Server {
	t.Helper()

	upgrader := &Upgrader{HandshakeTimeout: 5 * time.Second}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, http.Header{"X-Joaju": []string{"1"}})
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		for {
			kind, message, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err := conn.WriteMessage(kind, message); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)

	return server
}

func socketURL(server *httptest.Server) string {
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

// TestAcceptKeyAnswersTheExampleInTheRFC is the one vector RFC 6455 prints, in
// section 1.3.
//
// Every websocket implementation on earth agrees on this pair, so a mismatch
// here means nothing will ever complete a handshake with this server -- and the
// symptom would be a browser saying only that the connection failed.
func TestAcceptKeyAnswersTheExampleInTheRFC(t *testing.T) {
	const (
		key  = "dGhlIHNhbXBsZSBub25jZQ=="
		want = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
	)

	if got := acceptKey(key); got != want {
		t.Fatalf("acceptKey(%q) = %q, want %q", key, got, want)
	}
}

// TestHeaderContainsTokenReadsAListAndNotAValue is the most common way a
// hand-written handshake check is wrong: a browser behind a proxy sends
// "Connection: keep-alive, Upgrade", and a server comparing the whole value
// refuses it.
func TestHeaderContainsTokenReadsAListAndNotAValue(t *testing.T) {
	for _, one := range []struct {
		value string
		want  bool
	}{
		{"Upgrade", true},
		{"upgrade", true},
		{"keep-alive, Upgrade", true},
		{" Upgrade , keep-alive", true},
		{"keep-alive", false},
		{"Upgraded", false},
		{"", false},
	} {
		header := http.Header{}
		if one.value != "" {
			header.Set("Connection", one.value)
		}
		if got := headerContainsToken(header, "Connection", "Upgrade"); got != one.want {
			t.Errorf("Connection: %q contains Upgrade = %v, want %v", one.value, got, one.want)
		}
	}
}

// TestDialAndUpgradeCompleteAHandshake is the round trip, and it also checks
// that the caller's own response header rode along on the 101.
func TestDialAndUpgradeCompleteAHandshake(t *testing.T) {
	server := echoServer(t)

	conn, response, err := DefaultDialer.Dial(socketURL(server), nil)
	if err != nil {
		t.Fatalf("dialling = %v", err)
	}
	defer func() { _ = conn.Close() }()

	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("the handshake answered %d, want %d", response.StatusCode, http.StatusSwitchingProtocols)
	}
	if response.Header.Get("X-Joaju") != "1" {
		t.Fatal("the header the handler passed in did not reach the 101")
	}

	if err := conn.WriteMessage(TextMessage, []byte("o compilador")); err != nil {
		t.Fatalf("writing = %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	kind, got, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("reading = %v", err)
	}
	if kind != TextMessage || string(got) != "o compilador" {
		t.Fatalf("the echo came back as opcode %d %q", kind, got)
	}
}

// TestUpgradeAcceptsAHandshakeFromItsOwnOrigin is the browser case that must
// keep working, and it is the other half of the test below it.
func TestUpgradeAcceptsAHandshakeFromItsOwnOrigin(t *testing.T) {
	server := echoServer(t)

	header := http.Header{"Origin": []string{server.URL}}
	conn, _, err := DefaultDialer.Dial(socketURL(server), header)
	if err != nil {
		t.Fatalf("dialling from the server's own origin = %v", err)
	}
	_ = conn.Close()
}

// TestUpgradeRefusesEveryHandshakeThatIsNotOne walks the checks in order, and
// the first case is the security one.
//
// A websocket handshake has no preflight and no CORS: the browser attaches the
// user's cookies to a socket any page on the internet can open. Without the
// origin check, this server is a cross-site websocket hijacking hole, and the
// hole is silent.
func TestUpgradeRefusesEveryHandshakeThatIsNotOne(t *testing.T) {
	server := echoServer(t)

	for _, one := range []struct {
		name    string
		method  string
		headers map[string]string
		want    int
	}{
		{
			name:    "an Origin naming another host",
			headers: map[string]string{"Origin": "http://evil.example"},
			want:    http.StatusForbidden,
		},
		{
			name:   "a POST",
			method: http.MethodPost,
			want:   http.StatusMethodNotAllowed,
		},
		{
			name:    "no Upgrade token in Connection",
			headers: map[string]string{"Connection": "keep-alive"},
			want:    http.StatusBadRequest,
		},
		{
			name:    "an Upgrade that is not websocket",
			headers: map[string]string{"Upgrade": "h2c"},
			want:    http.StatusBadRequest,
		},
		{
			name:    "another protocol version",
			headers: map[string]string{"Sec-WebSocket-Version": "8"},
			want:    http.StatusUpgradeRequired,
		},
		{
			name:    "no key to answer",
			headers: map[string]string{"Sec-WebSocket-Key": ""},
			want:    http.StatusBadRequest,
		},
	} {
		t.Run(one.name, func(t *testing.T) {
			method := one.method
			if method == "" {
				method = http.MethodGet
			}

			request, err := http.NewRequest(method, server.URL, nil)
			if err != nil {
				t.Fatalf("building the request = %v", err)
			}
			request.Header.Set("Connection", "Upgrade")
			request.Header.Set("Upgrade", "websocket")
			request.Header.Set("Sec-WebSocket-Version", "13")
			request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
			for name, value := range one.headers {
				if value == "" {
					request.Header.Del(name)

					continue
				}
				request.Header.Set(name, value)
			}

			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatalf("sending = %v", err)
			}
			defer func() { _ = response.Body.Close() }()
			_, _ = io.Copy(io.Discard, response.Body)

			if response.StatusCode != one.want {
				t.Fatalf("%s answered %d, want %d", one.name, response.StatusCode, one.want)
			}
			if one.want == http.StatusUpgradeRequired && response.Header.Get("Sec-WebSocket-Version") != "13" {
				t.Fatal("a version refusal did not say which version to speak")
			}
		})
	}
}

// TestDialRefusesAServerThatAnswersTheWrongAcceptKey is what stops a dial from
// succeeding against a proxy, a cache or anything else that says 101 without
// having read the key.
func TestDialRefusesAServerThatAnswersTheWrongAcceptKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		_, _ = io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\n"+
			"Upgrade: websocket\r\nConnection: Upgrade\r\n"+
			"Sec-WebSocket-Accept: not-the-answer\r\n\r\n")
	}))
	defer server.Close()

	conn, _, err := DefaultDialer.Dial(socketURL(server), nil)
	if !errors.Is(err, ErrBadHandshake) {
		if conn != nil {
			_ = conn.Close()
		}
		t.Fatalf("dialling a server with the wrong accept key = %v, want %v", err, ErrBadHandshake)
	}
}

// TestDialReturnsTheResponseSoTheCallerCanReadWhy is what the server's tests
// depend on: a 403 from the origin check and a 404 from an unknown app key are
// both a failed dial, and only the response tells them apart.
func TestDialReturnsTheResponseSoTheCallerCanReadWhy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no such app", http.StatusNotFound)
	}))
	defer server.Close()

	conn, response, err := DefaultDialer.Dial(socketURL(server), nil)
	if conn != nil {
		_ = conn.Close()
	}
	if !errors.Is(err, ErrBadHandshake) {
		t.Fatalf("dialling = %v, want %v", err, ErrBadHandshake)
	}
	if response == nil || response.StatusCode != http.StatusNotFound {
		t.Fatalf("the response did not come back with the status")
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading the body = %v", err)
	}
	if !strings.Contains(string(body), "no such app") {
		t.Fatalf("the body did not survive the failed dial: %q", body)
	}
}

// TestDialRefusesAScheme is the guard against an http:// URL reaching a
// websocket dialler and hanging until the handshake times out.
func TestDialRefusesAScheme(t *testing.T) {
	if _, _, err := DefaultDialer.Dial("http://example.invalid/socket", nil); err == nil {
		t.Fatal("an http:// URL was accepted as a websocket")
	}
}

// TestWriteAcceptDropsTheHeadersTheProtocolOwns keeps a caller from answering
// its own handshake, or claiming an extension nothing here implements.
func TestWriteAcceptDropsTheHeadersTheProtocolOwns(t *testing.T) {
	var written strings.Builder
	extra := http.Header{
		"Sec-WebSocket-Accept":     []string{"forged"},
		"Sec-WebSocket-Extensions": []string{"permessage-deflate"},
		"Upgrade":                  []string{"h2c"},
		"X-Trace":                  []string{"kept"},
	}

	if err := writeAccept(&written, "dGhlIHNhbXBsZSBub25jZQ==", extra); err != nil {
		t.Fatalf("writing the accept = %v", err)
	}

	response := written.String()
	for _, forbidden := range []string{"forged", "permessage-deflate", "h2c"} {
		if strings.Contains(response, forbidden) {
			t.Fatalf("the 101 carried %q, which the caller must not be able to set", forbidden)
		}
	}
	if !strings.Contains(response, "X-Trace: kept") {
		t.Fatal("a header the caller may set was dropped")
	}
	if !strings.Contains(response, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") {
		t.Fatal("the 101 did not carry the answer to the key")
	}
}

// TestWriteAcceptStripsALineBreakFromAHeaderValue is response splitting: a
// newline in a value ends this response and starts one the caller controls.
func TestWriteAcceptStripsALineBreakFromAHeaderValue(t *testing.T) {
	var written strings.Builder
	extra := http.Header{"X-Trace": []string{"good\r\nX-Injected: bad"}}

	if err := writeAccept(&written, "dGhlIHNhbXBsZSBub25jZQ==", extra); err != nil {
		t.Fatalf("writing the accept = %v", err)
	}

	if strings.Contains(written.String(), "\r\nX-Injected") {
		t.Fatal("a header value split the response")
	}
}
