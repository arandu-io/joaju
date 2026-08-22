package ws

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
)

// connPair is a client and a server on a real loopback socket.
//
// Loopback rather than net.Pipe because net.Pipe is unbuffered: every write
// blocks until the other side reads it, so a close handshake -- where one side
// writes an echo the other has not asked for yet -- deadlocks on the pipe and
// works on a socket. The socket is what production has.
func connPair(t *testing.T) (client, server *Conn) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening = %v", err)
	}
	defer func() { _ = listener.Close() }()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			accepted <- nil

			return
		}
		accepted <- conn
	}()

	dialed, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dialling = %v", err)
	}

	serverSide := <-accepted
	if serverSide == nil {
		t.Fatal("the listener did not accept")
	}

	t.Cleanup(func() {
		_ = dialed.Close()
		_ = serverSide.Close()
	})

	return newConn(dialed, bufio.NewReader(dialed), 0, true),
		newConn(serverSide, bufio.NewReader(serverSide), 0, false)
}

// readCode reads until the connection ends and returns the close code, which is
// how every failure test here checks that the peer was told WHY.
func readCode(t *testing.T, c *Conn) int {
	t.Helper()

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := c.ReadMessage()

	var closeErr *CloseError
	if !errors.As(err, &closeErr) {
		t.Fatalf("the connection ended with %v, want a close frame", err)
	}

	return closeErr.Code
}

func TestConnCarriesATextMessage(t *testing.T) {
	client, server := connPair(t)

	want := "não é o desenvolvedor que garante"
	if err := client.WriteMessage(TextMessage, []byte(want)); err != nil {
		t.Fatalf("writing = %v", err)
	}

	kind, got, err := server.ReadMessage()
	if err != nil {
		t.Fatalf("reading = %v", err)
	}
	if kind != TextMessage || string(got) != want {
		t.Fatalf("read opcode %d %q, want %d %q", kind, got, TextMessage, want)
	}
}

// TestConnJoinsTheFragmentsOfOneMessage is section 5.4, and the opcode of the
// joined message comes from the FIRST frame -- the continuations carry 0.
func TestConnJoinsTheFragmentsOfOneMessage(t *testing.T) {
	client, server := connPair(t)

	for _, frame := range []Frame{
		{Final: false, Opcode: TextMessage, Payload: []byte("uma ")},
		{Final: false, Opcode: ContinuationFrame, Payload: []byte("forma ")},
		{Final: true, Opcode: ContinuationFrame, Payload: []byte("só")},
	} {
		if err := client.write(frame); err != nil {
			t.Fatalf("writing a fragment = %v", err)
		}
	}

	kind, got, err := server.ReadMessage()
	if err != nil {
		t.Fatalf("reading = %v", err)
	}
	if kind != TextMessage || string(got) != "uma forma só" {
		t.Fatalf("read opcode %d %q", kind, got)
	}
}

// TestConnAnswersAPingWithItsPayload is section 5.5.3: the pong carries the
// ping's application data back, byte for byte.
func TestConnAnswersAPingWithItsPayload(t *testing.T) {
	client, server := connPair(t)

	pongs := make(chan string, 1)
	client.SetPongHandler(func(data string) error {
		pongs <- data

		return nil
	})

	go func() { _, _, _ = server.ReadMessage() }()
	go func() { _, _, _ = client.ReadMessage() }()

	if err := client.WriteMessage(PingMessage, []byte("still here")); err != nil {
		t.Fatalf("writing the ping = %v", err)
	}

	select {
	case got := <-pongs:
		if got != "still here" {
			t.Fatalf("the pong carried %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no pong answered the ping")
	}
}

// TestConnEchoesACloseAndReportsItToBothSides is the handshake of section 5.5.1.
func TestConnEchoesACloseAndReportsItToBothSides(t *testing.T) {
	client, server := connPair(t)

	echoed := make(chan int, 1)
	go func() { echoed <- readCode(t, client) }()

	if err := client.WriteMessage(CloseMessage, FormatClose(CloseGoingAway, "leaving")); err != nil {
		t.Fatalf("writing the close = %v", err)
	}

	_, _, err := server.ReadMessage()
	var closeErr *CloseError
	if !errors.As(err, &closeErr) {
		t.Fatalf("the server read %v, want a close", err)
	}
	if closeErr.Code != CloseGoingAway || closeErr.Reason != "leaving" {
		t.Fatalf("the server read close %d %q", closeErr.Code, closeErr.Reason)
	}

	select {
	case code := <-echoed:
		if code != CloseGoingAway {
			t.Fatalf("the echo carried %d, want %d", code, CloseGoingAway)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the close was never echoed")
	}
}

// TestConnRefusesToWriteAfterTheCloseFrame is section 5.5.1, and it is what
// keeps a shutdown from racing a broadcast onto a socket that is finished.
func TestConnRefusesToWriteAfterTheCloseFrame(t *testing.T) {
	client, _ := connPair(t)

	if err := client.WriteMessage(CloseMessage, FormatClose(CloseNormalClosure, "")); err != nil {
		t.Fatalf("writing the close = %v", err)
	}
	if err := client.WriteMessage(TextMessage, []byte("late")); !errors.Is(err, ErrCloseSent) {
		t.Fatalf("writing after the close = %v, want %v", err, ErrCloseSent)
	}
}

// TestConnFailsTheConnectionWithTheCodeThatSaysWhy is what the Autobahn suite
// spends most of its cases on: not whether the connection ends, but whether the
// peer is told which rule it broke.
func TestConnFailsTheConnectionWithTheCodeThatSaysWhy(t *testing.T) {
	for _, one := range []struct {
		name  string
		limit int64
		send  func(t *testing.T, client *Conn)
		want  int
	}{
		{
			name: "text that is not UTF-8",
			send: func(t *testing.T, client *Conn) {
				if err := client.write(Frame{Final: true, Opcode: TextMessage, Payload: []byte{0xc0, 0xaf}}); err != nil {
					t.Fatalf("writing = %v", err)
				}
			},
			want: CloseInvalidFramePayloadData,
		},
		{
			name: "text that ends halfway through a rune",
			send: func(t *testing.T, client *Conn) {
				if err := client.write(Frame{Final: true, Opcode: TextMessage, Payload: []byte{0xf0, 0x9f}}); err != nil {
					t.Fatalf("writing = %v", err)
				}
			},
			want: CloseInvalidFramePayloadData,
		},
		{
			name:  "a message over the limit",
			limit: 4,
			send: func(t *testing.T, client *Conn) {
				if err := client.WriteMessage(BinaryMessage, bytes.Repeat([]byte{0}, 64)); err != nil {
					t.Fatalf("writing = %v", err)
				}
			},
			want: CloseMessageTooBig,
		},
		{
			name:  "fragments that add up to more than the limit",
			limit: 4,
			send: func(t *testing.T, client *Conn) {
				for _, frame := range []Frame{
					{Final: false, Opcode: BinaryMessage, Payload: []byte{1, 2, 3}},
					{Final: true, Opcode: ContinuationFrame, Payload: []byte{4, 5, 6}},
				} {
					if err := client.write(frame); err != nil {
						t.Fatalf("writing = %v", err)
					}
				}
			},
			want: CloseMessageTooBig,
		},
		{
			name: "a reserved bit with no extension negotiated",
			send: func(t *testing.T, client *Conn) {
				if _, err := client.conn.Write([]byte{0xc1, 0x80, 0, 0, 0, 0}); err != nil {
					t.Fatalf("writing = %v", err)
				}
			},
			want: CloseProtocolError,
		},
		{
			name: "an unmasked frame from a client",
			send: func(t *testing.T, client *Conn) {
				if _, err := client.conn.Write([]byte{0x81, 0x00}); err != nil {
					t.Fatalf("writing = %v", err)
				}
			},
			want: CloseProtocolError,
		},
		{
			name: "a continuation with nothing to continue",
			send: func(t *testing.T, client *Conn) {
				if err := client.write(Frame{Final: true, Opcode: ContinuationFrame, Payload: []byte("x")}); err != nil {
					t.Fatalf("writing = %v", err)
				}
			},
			want: CloseProtocolError,
		},
		{
			name: "a new message while one is still open",
			send: func(t *testing.T, client *Conn) {
				for _, frame := range []Frame{
					{Final: false, Opcode: TextMessage, Payload: []byte("a")},
					{Final: true, Opcode: TextMessage, Payload: []byte("b")},
				} {
					if err := client.write(frame); err != nil {
						t.Fatalf("writing = %v", err)
					}
				}
			},
			want: CloseProtocolError,
		},
		{
			name: "a close carrying a code that may not be sent",
			send: func(t *testing.T, client *Conn) {
				if err := client.write(Frame{Final: true, Opcode: CloseMessage, Payload: []byte{0x03, 0xed}}); err != nil {
					t.Fatalf("writing = %v", err)
				}
			},
			want: CloseProtocolError,
		},
	} {
		t.Run(one.name, func(t *testing.T) {
			client, server := connPair(t)
			server.SetReadLimit(one.limit)

			codes := make(chan int, 1)
			go func() { codes <- readCode(t, client) }()

			one.send(t, client)

			_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, _, err := server.ReadMessage(); err == nil {
				t.Fatal("the server accepted the frame")
			}

			select {
			case code := <-codes:
				if code != one.want {
					t.Fatalf("the server closed with %d, want %d", code, one.want)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("the server closed without saying why")
			}
		})
	}
}

// TestConnKeepsReturningTheSameErrorAfterAFailure is what stops a caller's read
// loop from spinning: a websocket whose framing failed cannot be resynchronised,
// so every later call has to say so rather than block or return nothing.
func TestConnKeepsReturningTheSameErrorAfterAFailure(t *testing.T) {
	client, server := connPair(t)

	go func() { _, _, _ = client.ReadMessage() }()

	if err := client.write(Frame{Final: true, Opcode: TextMessage, Payload: []byte{0xc0, 0xaf}}); err != nil {
		t.Fatalf("writing = %v", err)
	}

	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	first, _, err := server.ReadMessage()
	if err == nil {
		t.Fatalf("the server accepted an invalid message as opcode %d", first)
	}

	if _, _, again := server.ReadMessage(); !errors.Is(again, err) {
		t.Fatalf("a second read returned %v, want %v", again, err)
	}
}

// TestConnCarriesAMessageLargerThanTheBuffer proves the buffer is a buffer and
// not a limit.
func TestConnCarriesAMessageLargerThanTheBuffer(t *testing.T) {
	client, server := connPair(t)

	want := bytes.Repeat([]byte{'x'}, defaultBufferSize*3)
	go func() { _ = client.WriteMessage(BinaryMessage, want) }()

	_ = server.SetReadDeadline(time.Now().Add(5 * time.Second))
	kind, got, err := server.ReadMessage()
	if err != nil {
		t.Fatalf("reading = %v", err)
	}
	if kind != BinaryMessage || !bytes.Equal(got, want) {
		t.Fatalf("read opcode %d of %d bytes, want %d of %d", kind, len(got), BinaryMessage, len(want))
	}
}

// TestIsUnexpectedCloseSortsTheOrdinaryEndingsFromTheRest is the guard around
// logging: on a server holding ten thousand sockets, 1000 and 1001 ARE the log.
func TestIsUnexpectedCloseSortsTheOrdinaryEndingsFromTheRest(t *testing.T) {
	expected := []int{CloseNormalClosure, CloseGoingAway}

	if IsUnexpectedClose(&CloseError{Code: CloseNormalClosure}, expected...) {
		t.Error("a normal closure was called unexpected")
	}
	if IsUnexpectedClose(&CloseError{Code: CloseGoingAway}, expected...) {
		t.Error("a client navigating away was called unexpected")
	}
	if !IsUnexpectedClose(&CloseError{Code: CloseMessageTooBig}, expected...) {
		t.Error("a message that was too big was not called unexpected")
	}
	if IsUnexpectedClose(errors.New("read tcp: connection reset by peer"), expected...) {
		t.Error("a transport error was reported as a close code")
	}
}

// TestCloseErrorSaysTheCodeAndTheReason keeps the message useful with and
// without the peer's text, because most peers send none.
func TestCloseErrorSaysTheCodeAndTheReason(t *testing.T) {
	bare := (&CloseError{Code: CloseNormalClosure}).Error()
	if bare == "" || bytes.Contains([]byte(bare), []byte(": :")) {
		t.Fatalf("a close with no reason reads %q", bare)
	}
	if with := (&CloseError{Code: ClosePolicyViolation, Reason: "no"}).Error(); with == bare {
		t.Fatal("the reason did not reach the message")
	}
}
