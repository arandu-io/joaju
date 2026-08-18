package ws

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
	"unicode/utf8"
)

// scriptedAddr stands in for the address of a connection that has none.
type scriptedAddr struct{}

func (scriptedAddr) Network() string { return "scripted" }
func (scriptedAddr) String() string  { return "scripted" }

// scriptedConn is a net.Conn that replays a fixed slice of bytes and throws
// away everything written to it.
//
// A real socket would do as well and it would be slower by three orders of
// magnitude: a fuzz target runs the whole thing millions of times, and a
// listener, a dial and an accept per run buys nothing here -- what is under
// test reads bytes and writes bytes.
type scriptedConn struct {
	r *bytes.Reader
}

func (c *scriptedConn) Read(b []byte) (int, error)       { return c.r.Read(b) }
func (c *scriptedConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *scriptedConn) Close() error                     { return nil }
func (c *scriptedConn) LocalAddr() net.Addr              { return scriptedAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr             { return scriptedAddr{} }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

// FuzzReadMessage drives the whole read half with an arbitrary stream.
//
// This is what a socket actually is: not one frame but a sequence of them, with
// the fragmentation state, the UTF-8 state and the control frames interleaved
// between the fragments. Those three carry across calls, and a state machine is
// where an input that is legal frame by frame stops being legal as a stream.
func FuzzReadMessage(f *testing.F) {
	// A masked text message, and the same one split into two fragments with a
	// ping between them.
	f.Add([]byte{0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58}, false, int64(0))
	f.Add([]byte{
		0x01, 0x81, 0, 0, 0, 0, 'a',
		0x89, 0x80, 0, 0, 0, 0,
		0x80, 0x81, 0, 0, 0, 0, 'b',
	}, false, int64(0))
	// A text message whose rune is cut across two fragments, which is legal,
	// and a close that ends the stream.
	f.Add([]byte{
		0x01, 0x82, 0, 0, 0, 0, 0xf0, 0x9f,
		0x80, 0x82, 0, 0, 0, 0, 0x92, 0xa1,
		0x88, 0x82, 0, 0, 0, 0, 0x03, 0xe8,
	}, false, int64(0))
	f.Add([]byte{0x81, 0x05, 'H', 'e', 'l', 'l', 'o'}, true, int64(0))
	f.Add([]byte{0x82, 0x84, 0, 0, 0, 0, 1, 2, 3, 4}, false, int64(2))

	f.Fuzz(func(t *testing.T, data []byte, isClient bool, limit int64) {
		// One stream behind both halves, which is what newConn is handed in
		// production: the connection, and the buffer net/http already filled
		// from it.
		stream := &scriptedConn{r: bytes.NewReader(data)}
		conn := newConn(stream, bufio.NewReader(stream), 0, isClient)
		conn.SetReadLimit(limit)

		// Every message costs at least a two-byte header, so a stream of n bytes
		// cannot yield more than n/2 of them. A loop that gets past this is a
		// loop that is not consuming its input.
		bound := len(data)/2 + 2

		for read := 0; ; read++ {
			if read > bound {
				t.Fatalf("%d messages came out of %d bytes", read, len(data))
			}

			kind, message, err := conn.ReadMessage()
			if err != nil {
				// A websocket whose framing failed cannot be resynchronised, so
				// every later call has to say the same thing rather than block
				// or return a message.
				if _, _, again := conn.ReadMessage(); !errors.Is(again, err) {
					t.Fatalf("a second read returned %v, want %v", again, err)
				}

				return
			}

			switch kind {
			case TextMessage:
				if !utf8.Valid(message) {
					t.Fatalf("a text message of %d bytes is not UTF-8", len(message))
				}
			case BinaryMessage:
			default:
				t.Fatalf("ReadMessage returned opcode %d, which is not a data message", kind)
			}

			if limit > 0 && int64(len(message)) > limit {
				t.Fatalf("a message of %d bytes passed a limit of %d", len(message), limit)
			}
		}
	})
}
