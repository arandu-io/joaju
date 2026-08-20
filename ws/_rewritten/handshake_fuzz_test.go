package ws

import (
	"bufio"
	"bytes"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// recordingConn is a scripted connection that keeps what was written to it,
// which is how the 101 is inspected after the socket has been taken over.
type recordingConn struct {
	scriptedConn
	written bytes.Buffer
}

func (c *recordingConn) Write(b []byte) (int, error) { return c.written.Write(b) }

// hijackable is an http.ResponseWriter whose connection can be taken over.
//
// The recorder of net/http/httptest cannot be: it has no connection under it,
// and a handshake that cannot hijack is refused before it reaches anything
// worth fuzzing.
type hijackable struct {
	header   http.Header
	status   int
	body     bytes.Buffer
	reader   *bufio.Reader
	hijacked *recordingConn
}

func (w *hijackable) Header() http.Header         { return w.header }
func (w *hijackable) Write(b []byte) (int, error) { return w.body.Write(b) }
func (w *hijackable) WriteHeader(status int)      { w.status = status }

func (w *hijackable) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.hijacked, bufio.NewReadWriter(w.reader, bufio.NewWriter(w.hijacked)), nil
}

// FuzzUpgrade feeds arbitrary bytes to the handshake, as a request off the wire.
//
// The request is parsed by net/http first, the way it is in production, so what
// is under test is the negotiation and not the HTTP grammar: the token lists,
// the version, the origin, the key, and the response that is written by hand
// onto a connection net/http no longer owns.
//
// The extra header is the caller's, and it is fuzzed because that is the one
// string in the response that this package does not construct. A line break in
// it would end the 101 and start a response the caller never wrote.
func FuzzUpgrade(f *testing.F) {
	const handshake = "GET /app/key?protocol=7 HTTP/1.1\r\n" +
		"Host: joaju.test\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: keep-alive, Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"

	f.Add([]byte(handshake), "1")
	f.Add([]byte(strings.Replace(handshake, "Host: joaju.test\r\n", "Host: joaju.test\r\nOrigin: http://joaju.test\r\n", 1)), "")
	f.Add([]byte(strings.Replace(handshake, "Host: joaju.test\r\n", "Host: joaju.test\r\nOrigin: http://elsewhere.test\r\n", 1)), "")
	f.Add([]byte(strings.Replace(handshake, "Version: 13", "Version: 8", 1)), "")
	f.Add([]byte(strings.Replace(handshake, "GET", "POST", 1)), "")
	f.Add([]byte(handshake), "trace\r\nX-Injected: yes")

	f.Fuzz(func(t *testing.T, data []byte, extra string) {
		buffered := bufio.NewReader(bytes.NewReader(data))

		request, err := http.ReadRequest(buffered)
		if err != nil {
			return
		}

		writer := &hijackable{
			header:   http.Header{},
			reader:   buffered,
			hijacked: &recordingConn{scriptedConn: scriptedConn{r: bytes.NewReader(nil)}},
		}

		upgrader := &Upgrader{}
		conn, err := upgrader.Upgrade(writer, request, http.Header{"X-Joaju-Trace": []string{extra}})

		response := writer.hijacked.written.String()
		if err != nil {
			if strings.HasPrefix(response, "HTTP/1.1 101") {
				t.Fatalf("a refused handshake still switched protocols: %q", response)
			}

			return
		}
		defer func() { _ = conn.Close() }()

		if !strings.HasPrefix(response, "HTTP/1.1 101 Switching Protocols\r\n") {
			t.Fatalf("the accepted handshake answered %q", response)
		}
		if !strings.HasSuffix(response, "\r\n\r\n") || strings.Count(response, "\r\n\r\n") != 1 {
			t.Fatalf("the response carries more than one head: %q", response)
		}

		key := request.Header.Get("Sec-Websocket-Key")
		if !strings.Contains(response, "Sec-WebSocket-Accept: "+acceptKey(key)+"\r\n") {
			t.Fatalf("the response does not answer the key %q: %q", key, response)
		}

		// Everything the upgrade insisted on before it answered.
		if request.Method != http.MethodGet {
			t.Fatalf("a %s request was upgraded", request.Method)
		}
		if request.Header.Get("Sec-Websocket-Version") != "13" {
			t.Fatalf("version %q was upgraded", request.Header.Get("Sec-Websocket-Version"))
		}
		if key == "" {
			t.Fatal("a request with no key was upgraded")
		}
		if !headerContainsToken(request.Header, "Connection", "Upgrade") ||
			!strings.EqualFold(request.Header.Get("Upgrade"), "websocket") {
			t.Fatal("a request that did not ask to upgrade was upgraded")
		}
		if origin := request.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			if err != nil || !strings.EqualFold(u.Host, request.Host) {
				t.Fatalf("origin %q was accepted for host %q", origin, request.Host)
			}
		}

		// Nothing may have gone out through the ResponseWriter: the 101 is
		// written by hand, and a status written before the hijack would be a
		// second response on the same connection.
		if writer.status != 0 || writer.body.Len() != 0 {
			t.Fatalf("the accepted handshake also wrote status %d and %d bytes", writer.status, writer.body.Len())
		}
	})
}
