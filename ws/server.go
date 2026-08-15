package ws

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ErrNotUpgradable is a request that is not a WebSocket handshake.
//
// [Upgrader.Upgrade] has already answered the client by the time this is
// returned: the caller cannot write a second response, and having it try is how
// a handler ends up logging "superfluous WriteHeader". The error is for the log,
// not for the wire.
var ErrNotUpgradable = errors.New("ws: the request is not a websocket handshake")

// Upgrader turns an HTTP request into a [Conn].
//
// # What it does not have
//
// No CheckOrigin field. The same-origin check of [sameOrigin] always runs, and
// there is no configuration that widens it -- see that function for why a
// websocket handshake has no browser-side protection of its own. Cross-origin
// sockets are not available; when they are, one function changes rather than a
// list in a config file (RULE 9).
//
// No Subprotocols field. The Pusher protocol carries its version in the query
// string, so Sec-WebSocket-Protocol has nothing to negotiate here, and a field
// that is always empty is a field that will be wired wrong the first time
// somebody needs it.
//
// No compression. permessage-deflate is a second framing path and a CRIME-shaped
// hazard when a payload mixes attacker-controlled text with a session
// identifier. See the package doc.
type Upgrader struct {
	// HandshakeTimeout bounds writing the 101 response. Zero is no deadline.
	HandshakeTimeout time.Duration

	// ReadBufferSize and WriteBufferSize are per connection, in bytes. Zero
	// uses defaultBufferSize.
	//
	// These are multiplied by the connection count, which is the number that
	// matters on a server whose whole job is holding connections open: 64 KB
	// buffers and ten thousand sockets is 1.2 GB of buffer.
	ReadBufferSize  int
	WriteBufferSize int
}

// Upgrade completes the handshake and returns the connection.
//
// responseHeader is added to the 101, and is how a cookie or a trace header
// rides along. It must not carry Upgrade, Connection or Sec-WebSocket-Accept:
// those are the protocol's and Upgrade writes them itself.
//
// On any failure the client has already been answered with a status and a
// one-line body saying which check failed, because a handshake that fails
// silently is debugged by packet capture.
func (u *Upgrader) Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (*Conn, error) {
	if r.Method != http.MethodGet {
		return nil, refuse(w, http.StatusMethodNotAllowed, "the handshake must be a GET")
	}
	if !headerContainsToken(r.Header, "Connection", "Upgrade") {
		return nil, refuse(w, http.StatusBadRequest, "the Connection header must list Upgrade")
	}
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return nil, refuse(w, http.StatusBadRequest, "the Upgrade header must be websocket")
	}
	if r.Header.Get("Sec-Websocket-Version") != "13" {
		// The version goes back in a header as well as the body, which is what
		// section 4.4 asks for: it tells a client of another version what to
		// speak instead of leaving it to guess.
		w.Header().Set("Sec-WebSocket-Version", "13")

		return nil, refuse(w, http.StatusUpgradeRequired, "only websocket version 13 is supported")
	}
	if !sameOrigin(r) {
		return nil, refuse(w, http.StatusForbidden, "the Origin header names another host")
	}

	key := r.Header.Get("Sec-Websocket-Key")
	if key == "" {
		return nil, refuse(w, http.StatusBadRequest, "the Sec-WebSocket-Key header is missing")
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		// HTTP/2 is the usual reason: there is no connection to hijack, because
		// the request is a stream on a shared one. A websocket over HTTP/2 is
		// RFC 8441 and a different handshake entirely.
		return nil, refuse(w, http.StatusInternalServerError, "this connection cannot be upgraded")
	}

	netConn, brw, err := hijacker.Hijack()
	if err != nil {
		return nil, refuse(w, http.StatusInternalServerError, "the connection could not be taken over")
	}

	if brw.Reader.Buffered() > 0 {
		// Bytes arrived after the request and before the 101. A client is not
		// allowed to send frames yet, so this is either broken or an attempt at
		// smuggling something past whatever is in front of this server. There is
		// no safe way to interpret it.
		_ = netConn.Close()

		return nil, fmt.Errorf("%w: the client sent data before the handshake finished", ErrNotUpgradable)
	}

	if u.HandshakeTimeout > 0 {
		if err := netConn.SetWriteDeadline(time.Now().Add(u.HandshakeTimeout)); err != nil {
			_ = netConn.Close()

			return nil, err
		}
	}

	if err := writeAccept(netConn, key, responseHeader); err != nil {
		_ = netConn.Close()

		return nil, err
	}

	if u.HandshakeTimeout > 0 {
		// The handshake deadline was the handshake's. Leaving it in place would
		// kill the first broadcast that arrives after it, seconds later, on a
		// perfectly good connection.
		if err := netConn.SetWriteDeadline(time.Time{}); err != nil {
			_ = netConn.Close()

			return nil, err
		}
	}

	reader := brw.Reader
	if u.ReadBufferSize > 0 {
		reader = bufio.NewReaderSize(netConn, u.ReadBufferSize)
	}

	return newConn(netConn, reader, u.WriteBufferSize, false), nil
}

// writeAccept writes the 101 by hand, on the hijacked connection.
//
// By hand because the ResponseWriter is gone: hijacking took the connection out
// from under net/http, and with it every helper that would have formatted this.
// What is written is fixed by section 4.2.2 and does not vary.
func writeAccept(w io.Writer, key string, extra http.Header) error {
	var b strings.Builder
	b.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	b.WriteString("Upgrade: websocket\r\n")
	b.WriteString("Connection: Upgrade\r\n")
	b.WriteString("Sec-WebSocket-Accept: ")
	b.WriteString(acceptKey(key))
	b.WriteString("\r\n")

	for name, values := range extra {
		switch http.CanonicalHeaderKey(name) {
		case "Upgrade", "Connection", "Sec-Websocket-Accept", "Sec-Websocket-Extensions":
			// The protocol's own headers are written above. A caller that could
			// override them could answer the handshake wrong, or claim an
			// extension nothing here implements.
			continue
		}
		for _, value := range values {
			b.WriteString(name)
			b.WriteString(": ")
			// A newline in a header value is response splitting: it ends this
			// response and starts one the caller controls. The value is a
			// cookie or a trace id and neither contains a line break, so this
			// drops them rather than escaping.
			b.WriteString(strings.NewReplacer("\r", "", "\n", "").Replace(value))
			b.WriteString("\r\n")
		}
	}
	b.WriteString("\r\n")

	_, err := io.WriteString(w, b.String())

	return err
}

// refuse answers the handshake and returns the error to log.
func refuse(w http.ResponseWriter, status int, reason string) error {
	http.Error(w, reason, status)

	return fmt.Errorf("%w: %s", ErrNotUpgradable, reason)
}
