package ws

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"net/http"
	"net/url"
	"strings"
)

// acceptGUID is the fixed string of RFC 6455 section 1.3.
//
// It is concatenated with the client's key before hashing, and its only job is
// to make the answer impossible to produce by accident: a server that echoes
// what it was sent, or a cache that replays an old response, does not know to
// add it. It is a constant of the protocol and not a secret.
const acceptGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// acceptKey is the Sec-WebSocket-Accept for a given Sec-WebSocket-Key.
//
// SHA-1 here is not a security choice and there is nothing to weaken: the input
// is a random value the client just published in a header, and the output proves
// only that the responder understood the protocol. RFC 6455 fixes the algorithm,
// so a different one would answer a handshake nothing on earth would accept.
func acceptKey(key string) string {
	h := sha1.New()
	h.Write([]byte(key))
	h.Write([]byte(acceptGUID))

	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// challengeKey is a fresh Sec-WebSocket-Key for a client handshake.
//
// Sixteen random bytes, base64. crypto/rand rather than math/rand: this one the
// RFC does call for, because a predictable key lets a response be forged or
// replayed from a cache.
func challengeKey() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(b[:]), nil
}

// headerContainsToken reports whether a comma-separated header lists a token,
// ignoring case and surrounding space.
//
// Connection and Upgrade are token lists, not single values: a browser behind a
// proxy may send "Connection: keep-alive, Upgrade", and a server that compares
// the whole value to "Upgrade" refuses a legitimate client. This is the most
// common way a hand-written handshake check is wrong.
func headerContainsToken(h http.Header, name, token string) bool {
	for _, value := range h.Values(name) {
		for _, one := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(one), token) {
				return true
			}
		}
	}

	return false
}

// sameOrigin reports whether the request's Origin names the host it was sent to.
//
// A request with no Origin passes: the header is a browser's, and its absence
// means the caller is not a browser -- a Go client, a test, another instance of
// this server. Nothing is being protected from those, because nothing is being
// attached to them without their knowledge.
//
// A request WITH an Origin naming another host is refused, and this is the check
// whose absence is the classic websocket hole. The same-origin policy does not
// apply to a websocket handshake: there is no preflight, and the browser sends
// the user's cookies to whatever host the script names. Without this check, any
// page on the internet opens an authenticated socket into this server on its
// visitor's behalf.
//
// There is no field that widens it. Cross-origin sockets are not available, and
// when they are, this one function changes -- not a list in a config file
// (RULE 9).
func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}

	u, err := url.Parse(origin)
	if err != nil {
		return false
	}

	return strings.EqualFold(u.Host, r.Host)
}
