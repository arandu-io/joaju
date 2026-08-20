package ws

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ErrBadHandshake is a server that answered something other than 101.
//
// [Dialer.Dial] returns it together with the *http.Response, so a caller can
// read the status and the body. That pairing is the point: a 403 from the
// origin check and a 404 from an unknown app key are both a failed dial, and
// only the response tells them apart.
var ErrBadHandshake = errors.New("ws: the server did not accept the websocket handshake")

// Dialer opens a client connection.
//
// # Why there is a client here at all
//
// Two callers: the tests, which need to speak the protocol to the server they
// just started, and one instance talking to another. Neither is a browser, and
// that is what keeps this small.
//
// # What it does not have
//
// No proxy support, no redirect following, no cookie jar, no retry. A general
// websocket library carries those because it does not know who is calling; this
// one does. Proxy support in particular is where the only third-party import
// this repository would have had came from -- SOCKS5 lives in
// golang.org/x/net/proxy, and nothing here dials through a SOCKS proxy.
type Dialer struct {
	// HandshakeTimeout bounds the whole dial: connect, request, response. Zero
	// is no timeout, which on a network that drops packets means forever.
	HandshakeTimeout time.Duration

	// ReadBufferSize and WriteBufferSize are per connection, in bytes. Zero uses
	// defaultBufferSize.
	ReadBufferSize  int
	WriteBufferSize int

	// TLSClientConfig is used for wss. Nil is the Go default, which verifies the
	// certificate -- and it stays that way: a field here that turned
	// verification off would be the one line somebody copies into production.
	TLSClientConfig *tls.Config
}

// DefaultDialer is what a caller with no particular need uses.
//
// It is a pointer, and a caller that wants to change one field copies it first
// (dialer := *DefaultDialer) rather than mutating a value the whole process
// shares.
var DefaultDialer = &Dialer{HandshakeTimeout: 45 * time.Second}

// Dial opens a websocket to a ws:// or wss:// URL.
//
// The response is returned even on failure, whenever the server managed to send
// one. Its body has been read and closed on the success path, because there is
// no body to a 101 and the connection underneath it is now the websocket.
func (d *Dialer) Dial(rawURL string, requestHeader http.Header) (*Conn, *http.Response, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}

	var secure bool
	switch u.Scheme {
	case "ws":
	case "wss":
		secure = true
	default:
		return nil, nil, fmt.Errorf("ws: %q is not a websocket scheme", u.Scheme)
	}

	key, err := challengeKey()
	if err != nil {
		return nil, nil, err
	}

	request := &http.Request{
		Method:     http.MethodGet,
		URL:        u,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{},
		Host:       u.Host,
	}
	for name, values := range requestHeader {
		switch http.CanonicalHeaderKey(name) {
		case "Upgrade", "Connection", "Sec-Websocket-Key", "Sec-Websocket-Version":
			// The protocol's own headers are set below. A caller that could set
			// the key could not verify the answer to it.
			continue
		default:
			request.Header[http.CanonicalHeaderKey(name)] = values
		}
	}
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Sec-WebSocket-Key", key)
	request.Header.Set("Sec-WebSocket-Version", "13")

	netConn, err := d.connect(u, secure)
	if err != nil {
		return nil, nil, err
	}

	if d.HandshakeTimeout > 0 {
		if err := netConn.SetDeadline(time.Now().Add(d.HandshakeTimeout)); err != nil {
			_ = netConn.Close()

			return nil, nil, err
		}
	}

	if err := request.Write(netConn); err != nil {
		_ = netConn.Close()

		return nil, nil, err
	}

	size := d.ReadBufferSize
	if size <= 0 {
		size = defaultBufferSize
	}
	br := bufio.NewReaderSize(netConn, size)

	response, err := http.ReadResponse(br, request)
	if err != nil {
		_ = netConn.Close()

		return nil, nil, err
	}

	if response.StatusCode != http.StatusSwitchingProtocols {
		// The connection closes and the response goes back with its body
		// intact, so the caller can read why. This is the path the tests use
		// for every refusal the server has.
		_ = netConn.Close()

		return nil, response, fmt.Errorf("%w: %s", ErrBadHandshake, response.Status)
	}

	if err := verifyAccept(response, key); err != nil {
		_ = netConn.Close()

		return nil, response, err
	}

	if d.HandshakeTimeout > 0 {
		// Same reason as the server side: the deadline was the handshake's, and
		// leaving it would kill the connection mid-conversation.
		if err := netConn.SetDeadline(time.Time{}); err != nil {
			_ = netConn.Close()

			return nil, response, err
		}
	}

	response.Body = nopBody{}

	return newConn(netConn, br, d.WriteBufferSize, true), response, nil
}

// connect opens the TCP or TLS connection.
func (d *Dialer) connect(u *url.URL, secure bool) (net.Conn, error) {
	address := u.Host
	if u.Port() == "" {
		if secure {
			address = net.JoinHostPort(address, "443")
		} else {
			address = net.JoinHostPort(address, "80")
		}
	}

	dialer := &net.Dialer{Timeout: d.HandshakeTimeout}
	if !secure {
		return dialer.Dial("tcp", address)
	}

	config := d.TLSClientConfig
	if config == nil {
		config = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if config.ServerName == "" {
		// Cloned rather than mutated: the caller's config may be shared by
		// every dial in the process, and writing the hostname of this one into
		// it would send that name in the SNI of all the others.
		config = config.Clone()
		config.ServerName = u.Hostname()
	}

	return tls.DialWithDialer(dialer, "tcp", address, config)
}

// verifyAccept checks the server's answer to our challenge.
//
// Without this a dial succeeds against anything that says 101 -- a proxy, a
// cached response, a plain HTTP server that was confused into the right status.
// The accept header is the only proof that what is on the other end actually
// read our key and speaks this protocol.
func verifyAccept(response *http.Response, key string) error {
	if !strings.EqualFold(response.Header.Get("Upgrade"), "websocket") {
		return fmt.Errorf("%w: the response does not upgrade to websocket", ErrBadHandshake)
	}
	if !headerContainsToken(response.Header, "Connection", "Upgrade") {
		return fmt.Errorf("%w: the response does not carry Connection: Upgrade", ErrBadHandshake)
	}
	if response.Header.Get("Sec-Websocket-Accept") != acceptKey(key) {
		return fmt.Errorf("%w: the accept header does not answer the key that was sent", ErrBadHandshake)
	}
	if response.Header.Get("Sec-Websocket-Extensions") != "" {
		// Nothing here negotiates an extension, so a server claiming one has
		// answered a request that was never made -- and its frames may set the
		// reserved bits that [ReadFrame] refuses.
		return fmt.Errorf("%w: the server claims an extension that was not requested", ErrBadHandshake)
	}

	return nil
}

// nopBody stands in for the body of a 101, which has none.
//
// It exists so that a caller may call response.Body.Close() without a nil check.
// The real body would be the websocket itself, and handing that to something
// that reads bodies is how the first frame gets eaten.
type nopBody struct{}

func (nopBody) Read([]byte) (int, error) { return 0, errors.New("ws: a 101 response has no body") }
func (nopBody) Close() error             { return nil }
