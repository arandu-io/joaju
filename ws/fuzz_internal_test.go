// Copyright 2013 The HYZIS WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ws

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// allocationSlack is how much reading one frame may allocate on top of the bytes
// it was handed.
//
// A reader holding n bytes cannot produce a payload longer than n, so anything
// far above n is memory the peer's declared length bought without sending a byte
// for it. The slack covers the connection's own buffers, the frame, and whatever
// the runtime charges to the goroutine while it works.
const allocationSlack = 8 << 20

// declaresExtendedLength reports whether the header uses the eight-byte length
// form of section 5.2, which is the only one that can name more bytes than a
// machine has.
//
// The allocation invariant is checked on those headers alone: reading the memory
// statistics stops the world, and the two shorter length forms cannot declare
// more than 65535 bytes.
func declaresExtendedLength(data []byte) bool {
	return len(data) >= 10 && data[1]&0x7f == 127
}

// fuzzConn reads the fuzzer's bytes as if a peer had sent them and throws away
// whatever is written back.
//
// A writer is needed and not optional: a frame that breaks the protocol makes a
// best effort to tell the peer so, and a connection with nowhere to write that
// panics instead of failing.
func fuzzConn(data []byte, isServer bool, limit int64) *Conn {
	c := newTestConn(bytes.NewReader(data), io.Discard, isServer)
	c.SetReadLimit(limit)
	return c
}

// FuzzReadFrame feeds arbitrary bytes to the frame reader.
//
// This is the first thing a peer's bytes reach, before anything is known about
// it, so what is asserted is what the reader promises about untrusted input: it
// returns rather than panicking, and it never allocates on a length nobody sent
// the bytes for.
func FuzzReadFrame(f *testing.F) {
	// The masked "Hello" of section 5.7, and its unmasked twin.
	f.Add([]byte{0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58}, true, int64(0))
	f.Add([]byte{0x81, 0x05, 'H', 'e', 'l', 'l', 'o'}, false, int64(0))
	// A close, a ping and a fragment, each of them a control path of its own.
	f.Add([]byte{0x88, 0x82, 0, 0, 0, 0, 0x03, 0xe8}, true, int64(0))
	f.Add([]byte{0x89, 0x80, 0, 0, 0, 0}, true, int64(16))
	f.Add([]byte{0x01, 0x81, 0, 0, 0, 0, 'a'}, true, int64(0))
	// The two extended length forms, both of them declaring more than they send.
	f.Add([]byte{0x82, 0xfe, 0xff, 0xff, 0, 0, 0, 0}, true, int64(0))
	f.Add([]byte{0x82, 0xff, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0, 0, 0, 0}, true, int64(1024))
	// Ten bytes declaring more than three exabytes.
	f.Add([]byte{0x82, 0xff, 0x2f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}, true, int64(0))

	f.Fuzz(func(t *testing.T, data []byte, isServer bool, limit int64) {
		if declaresExtendedLength(data) {
			// The reader is a fixed slice, so it can never satisfy a header
			// declaring more than the slice holds. Whatever such a header costs
			// is memory that was allocated on the peer's word alone.
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			c := fuzzConn(data, isServer, limit)
			_, _, _ = c.ReadMessage()
			runtime.ReadMemStats(&after)

			if grown := after.TotalAlloc - before.TotalAlloc; grown > uint64(len(data))+allocationSlack {
				t.Fatalf("reading %d bytes with limit %d allocated %d bytes", len(data), limit, grown)
			}
		}

		c := fuzzConn(data, isServer, limit)
		frameType, err := c.advanceFrame()
		if err != nil {
			return
		}

		switch frameType {
		case continuationFrame, TextMessage, BinaryMessage, PingMessage, PongMessage:
		default:
			// A close returns an error, so it never reaches here.
			t.Fatalf("an accepted frame carries opcode %d", frameType)
		}
		if isControl(frameType) && c.readRemaining > maxControlFramePayloadSize {
			t.Fatalf("an accepted control frame declares %d bytes", c.readRemaining)
		}
		if c.readRemaining < 0 {
			t.Fatalf("an accepted frame declares %d bytes remaining", c.readRemaining)
		}
	})
}

// FuzzReadMessage feeds arbitrary bytes to the message reader, which is the
// frame reader plus everything that joins frames into a message.
//
// The extra ground it covers is fragmentation and the read limit: a message
// assembled from frames must never come out longer than the bytes that were on
// the wire, and never longer than the limit that was set.
func FuzzReadMessage(f *testing.F) {
	f.Add([]byte{0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58}, true, int64(0))
	// A message in two fragments, and a ping between them.
	f.Add([]byte{
		0x01, 0x81, 0, 0, 0, 0, 'a',
		0x89, 0x80, 0, 0, 0, 0,
		0x80, 0x81, 0, 0, 0, 0, 'b',
	}, true, int64(0))
	// A continuation with nothing to continue.
	f.Add([]byte{0x80, 0x81, 0, 0, 0, 0, 'a'}, true, int64(0))
	// Twelve bytes declaring more than a machine has.
	f.Add([]byte{0x81, 0xff, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30}, false, int64(-33))

	f.Fuzz(func(t *testing.T, data []byte, isServer bool, limit int64) {
		c := fuzzConn(data, isServer, limit)
		frameType, payload, err := c.ReadMessage()
		if err != nil {
			return
		}

		if frameType != TextMessage && frameType != BinaryMessage {
			t.Fatalf("ReadMessage returned opcode %d", frameType)
		}
		if len(payload) > len(data) {
			t.Fatalf("a message of %d bytes came out of %d bytes of input", len(payload), len(data))
		}
		if limit > 0 && int64(len(payload)) > limit {
			t.Fatalf("a message of %d bytes passed a limit of %d", len(payload), limit)
		}
		if frameType == TextMessage && !utf8.Valid(payload) {
			t.Fatalf("an accepted text message is not UTF-8: %q", payload)
		}
	})
}

// FuzzParseClose feeds arbitrary bytes to the close frame reader.
//
// A close payload arrives before anything else is known about the peer, so what
// it must never do is hand an application a code the RFC forbids on the wire, or
// a reason that is not UTF-8.
func FuzzParseClose(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte{0x03, 0xe8})
	f.Add([]byte{0x03})
	f.Add([]byte{0x03, 0xed})
	f.Add([]byte{0x03, 0xe8, 'b', 'y', 'e'})
	f.Add([]byte{0x03, 0xe8, 0xc0, 0xaf})
	f.Add([]byte{0x0f, 0xa0, 0xc3, 0xa9})

	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > maxControlFramePayloadSize {
			// Longer than a control frame may carry, so it can never be read
			// back out of one.
			return
		}

		var wire bytes.Buffer
		writer := newTestConn(nil, &wire, true)
		if err := writer.WriteControl(CloseMessage, payload, time.Time{}); err != nil {
			t.Fatalf("writing a %d byte close returned %v", len(payload), err)
		}

		reader := newTestConn(bytes.NewReader(wire.Bytes()), io.Discard, false)
		_, _, err := reader.ReadMessage()

		var closeErr *CloseError
		if !errors.As(err, &closeErr) {
			// A payload the reader refused. It must have refused it by failing
			// the connection, not by handing the application a close.
			return
		}

		if !isValidReceivedCloseCode(closeErr.Code) && closeErr.Code != CloseNoStatusReceived {
			t.Fatalf("an accepted close carries code %d", closeErr.Code)
		}
		if !utf8.ValidString(closeErr.Text) {
			t.Fatalf("an accepted close carries a reason that is not UTF-8: %q", closeErr.Text)
		}
		if len(payload) >= 2 && closeErr.Text != string(payload[2:]) {
			t.Fatalf("the reason %q is not the payload's tail %q", closeErr.Text, payload[2:])
		}
	})
}

// FuzzFormatCloseMessage is the other half: what the formatter builds must fit a
// control frame and must read back.
//
// The reason is shortened to fit the 125-byte limit, and the peer holds it to the
// same UTF-8 rule as any other text -- so a cut landing inside a rune produces a
// close frame the peer must fail the connection over.
func FuzzFormatCloseMessage(f *testing.F) {
	f.Add(CloseNormalClosure, "")
	f.Add(CloseGoingAway, "leaving")
	f.Add(CloseNoStatusReceived, "ignored")
	f.Add(CloseInternalServerErr, strings.Repeat("e", 200))
	f.Add(CloseProtocolError, strings.Repeat("é", 40))
	// Two reasons whose cut lands inside a rune, one two bytes wide and one
	// four. Coverage does not lead here on its own: the branch that shortens is
	// already reached by plain ASCII, and what is wrong with these is the byte
	// the cut falls on rather than the branch.
	f.Add(ClosePolicyViolation, strings.Repeat("é", 90))
	f.Add(CloseMessageTooBig, strings.Repeat("\U0001f4a1", 40))
	f.Add(4000, "aplicação")

	f.Fuzz(func(t *testing.T, closeCode int, text string) {
		payload := FormatCloseMessage(closeCode, text)
		if len(payload) > maxControlFramePayloadSize {
			t.Fatalf("formatting %d with a %d byte reason gave %d bytes", closeCode, len(text), len(payload))
		}

		if closeCode == CloseNoStatusReceived {
			if len(payload) != 0 {
				t.Fatalf("formatting the no-status code gave %v, want nothing to send", payload)
			}
			return
		}

		if len(payload) < 2 {
			t.Fatalf("formatting %d gave %d bytes, want at least the code", closeCode, len(payload))
		}
		reason := string(payload[2:])
		if !strings.HasPrefix(text, reason) {
			t.Fatalf("the reason %q came back as %q, which is not a prefix of it", text, reason)
		}
		if utf8.ValidString(text) && !utf8.ValidString(reason) {
			t.Fatalf("a valid reason of %d bytes was cut inside a rune", len(text))
		}
		if len(text) <= maxControlFramePayloadSize-2 && reason != text {
			t.Fatalf("a reason of %d bytes was shortened to %q", len(text), reason)
		}
	})
}

// FuzzMaskBytes asserts the property the RFC leans on: the mask is its own
// inverse, at every length and every offset.
//
// Length and offset both matter because the key repeats every four bytes and the
// masking runs a word at a time. A payload whose length is not a multiple of the
// word size, or that starts at an offset the key is not aligned to, is where an
// implementation stops agreeing with itself.
func FuzzMaskBytes(f *testing.F) {
	f.Add([]byte("Hello, mask"), uint32(0x37fa213d), 0)
	f.Add([]byte(nil), uint32(0), 0)
	f.Add([]byte{0}, uint32(0xffffffff), 3)
	f.Add(bytes.Repeat([]byte("x"), 1024), uint32(0x01020304), 2)

	f.Fuzz(func(t *testing.T, payload []byte, key uint32, pos int) {
		if pos < 0 {
			pos = -pos
		}
		pos &= 3
		k := [4]byte{byte(key >> 24), byte(key >> 16), byte(key >> 8), byte(key)}

		got := append([]byte(nil), payload...)
		end := maskBytes(k, pos, got)
		if again := maskBytes(k, pos, got); again != end {
			t.Fatalf("masking twice from offset %d ended at %d and then %d", pos, end, again)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("masking %d bytes twice from offset %d did not return the original", len(payload), pos)
		}
		if want := (pos + len(payload)) & 3; end != want {
			t.Fatalf("masking %d bytes from offset %d ended at %d, want %d", len(payload), pos, end, want)
		}
	})
}

// FuzzUTF8Validator holds the incremental check to what the standard library
// says about the same bytes.
//
// The split is fuzzed along with the bytes, because the whole reason this
// validator exists is that a rune may arrive in pieces: a decoder that agrees
// with utf8.Valid on whole buffers and disagrees on split ones is the bug.
func FuzzUTF8Validator(f *testing.F) {
	f.Add([]byte("hello"), 2)
	f.Add([]byte("Ação"), 2)
	f.Add([]byte{0xf0, 0x9f, 0x9c, 0x82}, 1)
	f.Add([]byte{0xc0, 0xaf}, 1)
	f.Add([]byte{0xed, 0xa0, 0x80}, 2)
	f.Add([]byte{0xf4, 0x90, 0x80, 0x80}, 3)

	f.Fuzz(func(t *testing.T, data []byte, split int) {
		if split < 0 {
			split = -split
		}
		if len(data) > 0 {
			split %= len(data) + 1
		} else {
			split = 0
		}

		var v utf8Validator
		ok := v.write(data[:split]) && v.write(data[split:]) && v.complete()
		if ok != utf8.Valid(data) {
			t.Fatalf("the validator says %v for %d bytes split at %d, utf8.Valid says %v",
				ok, len(data), split, utf8.Valid(data))
		}
	})
}

// FuzzUpgrade feeds arbitrary bytes to the handshake, as a request off the wire.
//
// net/http parses the request first, the way it does in production, so what is
// under test is the negotiation and not the HTTP grammar: the token lists, the
// version, the origin, the key, and the response written by hand onto a
// connection net/http no longer owns.
//
// The extra header is the caller's, and it is fuzzed because that is the one
// string in the response this package does not construct. A line break in it
// would end the 101 and start a response the caller never wrote.
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

		writer := &hijackableWriter{
			header:   http.Header{},
			reader:   buffered,
			hijacked: &recordingConn{},
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
		if !strings.Contains(response, "Sec-WebSocket-Accept: "+computeAcceptKey(key)+"\r\n") {
			t.Fatalf("the response does not answer the key %q: %q", key, response)
		}

		// Everything the upgrade insisted on before it answered.
		if request.Method != http.MethodGet {
			t.Fatalf("a %s request was upgraded", request.Method)
		}
		if !tokenListContainsValue(request.Header, "Sec-Websocket-Version", "13") {
			t.Fatalf("version %q was upgraded", request.Header.Get("Sec-Websocket-Version"))
		}
		if !isValidChallengeKey(key) {
			t.Fatalf("the key %q was upgraded", key)
		}
		if !tokenListContainsValue(request.Header, "Connection", "upgrade") ||
			!tokenListContainsValue(request.Header, "Upgrade", "websocket") {
			t.Fatal("a request that did not ask to upgrade was upgraded")
		}
		if origin := request.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			if err != nil || !equalASCIIFold(u.Host, request.Host) {
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

// recordingConn keeps what was written to it, which is how the 101 is inspected
// after the socket has been taken over.
type recordingConn struct {
	fakeNetConn
	written bytes.Buffer
}

func (c *recordingConn) Write(b []byte) (int, error) { return c.written.Write(b) }

// hijackableWriter is an http.ResponseWriter whose connection can be taken over.
//
// The recorder of net/http/httptest cannot be: it has no connection under it,
// and a handshake that cannot hijack is refused before it reaches anything worth
// fuzzing.
type hijackableWriter struct {
	header   http.Header
	status   int
	body     bytes.Buffer
	reader   *bufio.Reader
	hijacked *recordingConn
}

func (w *hijackableWriter) Header() http.Header         { return w.header }
func (w *hijackableWriter) Write(b []byte) (int, error) { return w.body.Write(b) }
func (w *hijackableWriter) WriteHeader(status int)      { w.status = status }

func (w *hijackableWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.hijacked, bufio.NewReadWriter(w.reader, bufio.NewWriter(w.hijacked)), nil
}
