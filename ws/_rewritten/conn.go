package ws

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// ErrCloseSent is a write attempted after the close frame went out.
//
// Section 5.5.1 is explicit: after sending a close, an endpoint must not send
// any more data frames. Returning an error rather than writing anyway is what
// keeps a shutdown from racing a broadcast into a frame the peer must reject.
var ErrCloseSent = errors.New("ws: the close frame was already sent")

// CloseError is the peer closing, with the code and reason it gave.
//
// It comes out of [Conn.ReadMessage] as an error because that is the only way a
// reader finds out, and it carries the code because the difference between 1000
// and 1009 is the difference between a client that finished and a client that
// was cut off.
type CloseError struct {
	// Code is one of the Close* constants, or an application code in the
	// 3000-4999 range. It is [CloseNoStatusReceived] when the close frame
	// carried no code, and [CloseAbnormalClosure] when the connection ended
	// with no close frame at all.
	Code int
	// Reason is the peer's text, which is often empty.
	Reason string
}

func (e *CloseError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("ws: the peer closed the connection with code %d", e.Code)
	}

	return fmt.Sprintf("ws: the peer closed the connection with code %d: %s", e.Code, e.Reason)
}

// IsUnexpectedClose reports whether err is the peer closing with a code that is
// not in expected.
//
// It is the guard around logging. A client navigating away sends 1001 and a
// client finishing sends 1000, and neither is worth a line in a log -- on a
// server with ten thousand sockets they are the log. What is worth a line is
// 1002, 1009 or a connection that ended with no close frame at all.
//
// Anything that is not a [CloseError] returns false: a read that failed on a
// deadline or a reset is a transport error, and the caller already has it.
func IsUnexpectedClose(err error, expected ...int) bool {
	var closeErr *CloseError
	if !errors.As(err, &closeErr) {
		return false
	}
	for _, code := range expected {
		if closeErr.Code == code {
			return false
		}
	}

	return true
}

// Conn is one WebSocket connection.
//
// # Concurrency
//
// One goroutine reads and any number write. [Conn.ReadMessage] is NOT safe to
// call from two goroutines -- it carries the state of a fragmented message and
// of the UTF-8 check across calls -- and every write path takes a mutex, because
// two frames interleaved on one socket is a protocol violation the peer cannot
// recover from.
//
// The asymmetry is deliberate and it is what the read loop needs: answering a
// ping is a write, and it happens on the reading goroutine while another
// goroutine may be broadcasting.
type Conn struct {
	conn     net.Conn
	br       *bufio.Reader
	bw       *bufio.Writer
	isClient bool

	// The read half. Owned by the one goroutine that calls ReadMessage.
	readLimit  int64
	fragOpcode int
	validator  utf8Validator
	readErr    error
	pong       func(string) error

	// The write half, guarded by mu.
	mu        sync.Mutex
	closeSent bool
}

// newConn wraps a hijacked or dialled connection.
//
// br is the reader the hijack returned, and using it rather than a fresh one is
// not an optimisation: net/http may have already read bytes past the request
// into that buffer, and a new reader would start after them. On a websocket the
// client is allowed to send its first frame immediately, so those bytes are
// often the first frame.
func newConn(conn net.Conn, br *bufio.Reader, writeBuffer int, isClient bool) *Conn {
	if writeBuffer <= 0 {
		writeBuffer = defaultBufferSize
	}

	return &Conn{
		conn:     conn,
		br:       br,
		bw:       bufio.NewWriterSize(conn, writeBuffer),
		isClient: isClient,
	}
}

// defaultBufferSize is what a connection uses when none is configured.
//
// Four kilobytes, because the traffic this server carries is JSON events of a
// few hundred bytes: a bigger buffer per socket is memory multiplied by the
// connection count for no fewer syscalls.
const defaultBufferSize = 4096

// SetReadLimit caps one message, counting every fragment of it.
//
// Zero is no limit. The limit is checked against the length the peer declared in
// the header, so an oversized message costs a refusal and never an allocation --
// which is the point: without it there is nothing to refuse the message with,
// and a peer that declares a terabyte holds the connection open for as long as
// it keeps sending one.
func (c *Conn) SetReadLimit(n int64) { c.readLimit = n }

// SetReadDeadline is the net.Conn deadline, and it is how a dead peer is
// noticed.
//
// A TCP connection to a machine that vanished stays open until the OS gives up,
// which can be hours. A read deadline that the caller pushes forward on every
// frame turns silence into an error in seconds.
func (c *Conn) SetReadDeadline(t time.Time) error { return c.conn.SetReadDeadline(t) }

// SetWriteDeadline is the net.Conn deadline for writes.
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

// SetPongHandler registers what runs when a pong arrives, with its payload.
//
// A pong is the answer to this server's ping and it is the proof the peer is
// alive, so the handler is where the read deadline gets pushed forward. It runs
// on the reading goroutine, before [Conn.ReadMessage] returns, and an error from
// it fails the read -- a deadline that could not be set is a connection that
// would hang.
func (c *Conn) SetPongHandler(h func(appData string) error) { c.pong = h }

// LocalAddr and RemoteAddr are the underlying connection's.
func (c *Conn) LocalAddr() net.Addr  { return c.conn.LocalAddr() }
func (c *Conn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }

// Close closes the underlying connection without a close frame.
//
// It is the abrupt end, for a caller that already gave up. The orderly one is
// WriteMessage with [CloseMessage] followed by this.
func (c *Conn) Close() error { return c.conn.Close() }

// ReadMessage reads the next data message, joining its fragments.
//
// Control frames are handled here and never returned: a ping is answered, a
// pong reaches the handler, and a close is echoed and becomes a [CloseError].
// The caller sees messages, which is what a caller wants -- the alternative is
// every caller in the codebase writing the same switch.
//
// After any error, every later call returns that same error. A failed websocket
// cannot be resynchronised: the framing is a stream, and a frame boundary once
// lost is lost.
func (c *Conn) ReadMessage() (messageType int, p []byte, err error) {
	if c.readErr != nil {
		return 0, nil, c.readErr
	}

	var (
		message []byte
		opcode  int
	)

	for {
		limit := c.readLimit
		if limit > 0 {
			limit -= int64(len(message))
			if limit <= 0 {
				return 0, nil, c.failRead(ErrTooLarge)
			}
		}

		frame, err := ReadFrame(c.br, !c.isClient, limit)
		if err != nil {
			return 0, nil, c.failRead(err)
		}

		if frame.Control() {
			if err := c.control(frame); err != nil {
				return 0, nil, err
			}
			continue
		}

		switch {
		case frame.Opcode == ContinuationFrame:
			if c.fragOpcode == 0 {
				return 0, nil, c.failRead(fmt.Errorf("%w: a continuation arrived with no message open", ErrBadFragment))
			}
			opcode = c.fragOpcode
		case c.fragOpcode != 0:
			return 0, nil, c.failRead(fmt.Errorf("%w: a new data frame arrived while a message was still open", ErrBadFragment))
		default:
			opcode = frame.Opcode
			c.validator.reset()
		}

		// Text is checked as it arrives, fragment by fragment, so that a bad
		// byte fails the connection at once instead of after the last fragment
		// of a message that may be enormous (section 8.1).
		if opcode == TextMessage && !c.validator.write(frame.Payload) {
			return 0, nil, c.failRead(ErrBadUTF8)
		}

		if message == nil && frame.Final {
			// The whole message in one frame, which is the common case. The
			// payload is already a slice of its own, so it is handed over
			// rather than copied.
			message = frame.Payload
		} else {
			message = append(message, frame.Payload...)
		}

		if !frame.Final {
			c.fragOpcode = opcode
			continue
		}

		c.fragOpcode = 0
		if opcode == TextMessage && !c.validator.complete() {
			// Every byte was legal and the message still ends mid-rune.
			return 0, nil, c.failRead(ErrBadUTF8)
		}

		return opcode, message, nil
	}
}

// control handles a ping, a pong or a close.
func (c *Conn) control(frame Frame) error {
	switch frame.Opcode {
	case PingMessage:
		// The pong must carry the ping's payload back, byte for byte
		// (section 5.5.3). A write failure here is not fatal to the read: the
		// peer will time out its own ping, and the next read reports whatever
		// actually broke.
		_ = c.write(Frame{Final: true, Opcode: PongMessage, Payload: frame.Payload})

		return nil

	case PongMessage:
		if c.pong == nil {
			return nil
		}
		if err := c.pong(string(frame.Payload)); err != nil {
			c.readErr = err

			return err
		}

		return nil
	}

	code, reason, err := ParseClose(frame.Payload)
	if err != nil {
		return c.failRead(err)
	}

	// The close is echoed before the socket goes away, which is what completes
	// the handshake of section 5.5.1. The code is sent back unless the peer sent
	// none, in which case there is nothing to echo and 1000 is the answer.
	echo := code
	if echo == CloseNoStatusReceived {
		echo = CloseNormalClosure
	}
	_ = c.write(Frame{Final: true, Opcode: CloseMessage, Payload: FormatClose(echo, "")})
	_ = c.conn.Close()

	c.readErr = &CloseError{Code: code, Reason: reason}

	return c.readErr
}

// failRead ends the connection the way section 7.1.7 requires: a close frame
// carrying the code that says what was wrong, then the socket shut.
//
// Telling the peer WHY is not politeness. A client whose frames are refused with
// no explanation retries the same frames; one that is told 1007 knows its
// encoder is broken, and one that is told 1009 knows to fragment. This is also
// exactly what the Autobahn suite measures -- most of its failures are servers
// that close with the right timing and the wrong code.
func (c *Conn) failRead(err error) error {
	code := CloseProtocolError
	switch {
	case errors.Is(err, ErrBadUTF8):
		code = CloseInvalidFramePayloadData
	case errors.Is(err, ErrTooLarge):
		code = CloseMessageTooBig
	case isTransport(err):
		// Nothing to say and nobody to say it to: the read failed on a deadline,
		// a reset or an EOF, so the connection is already gone.
		c.readErr = err
		_ = c.conn.Close()

		return err
	}

	_ = c.write(Frame{Final: true, Opcode: CloseMessage, Payload: FormatClose(code, "")})
	_ = c.conn.Close()
	c.readErr = err

	return err
}

// isTransport reports whether the error came from the network rather than from
// the protocol, which decides whether a close frame is worth attempting.
func isTransport(err error) bool {
	switch {
	case errors.Is(err, ErrReservedBits), errors.Is(err, ErrUnmaskedClient),
		errors.Is(err, ErrMaskedServer), errors.Is(err, ErrBadOpcode),
		errors.Is(err, ErrBadCloseCode), errors.Is(err, ErrBadUTF8),
		errors.Is(err, ErrTooLarge), errors.Is(err, ErrBadFragment),
		errors.Is(err, ErrControlTooLong):
		return false
	}

	return true
}

// WriteMessage writes one message as a single frame.
//
// One frame and not several: fragmentation exists for a sender that does not
// know the length in advance, and here the message is a byte slice whose length
// is known. A control opcode is accepted too, which is how a close is sent.
//
// Safe to call from several goroutines. Each call holds the connection's write
// mutex for its whole frame, because a frame split by another writer is a frame
// the peer must fail the connection over.
func (c *Conn) WriteMessage(messageType int, data []byte) error {
	switch messageType {
	case TextMessage, BinaryMessage, CloseMessage, PingMessage, PongMessage:
	default:
		return fmt.Errorf("%w: %d may not be written as a message", ErrBadOpcode, messageType)
	}

	return c.write(Frame{Final: true, Opcode: messageType, Payload: data})
}

// write is the one path to the socket, and everything goes through it.
func (c *Conn) write(frame Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closeSent {
		return ErrCloseSent
	}
	if frame.Opcode == CloseMessage {
		c.closeSent = true
	}

	if err := WriteFrame(c.bw, frame, c.isClient); err != nil {
		return err
	}

	return c.bw.Flush()
}
