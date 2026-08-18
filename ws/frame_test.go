package ws

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// TestReadFrameAcceptsEveryLengthForm walks the three ways RFC 6455 encodes a
// length, and the boundaries between them.
//
// The boundaries are where a hand-written framer is wrong: 125 is the last
// one-byte length, 126 is the first two-byte one, and 65535/65536 is the second
// step. An implementation that is off by one here talks to itself and to nothing
// else.
func TestReadFrameAcceptsEveryLengthForm(t *testing.T) {
	for _, size := range []int{0, 1, 125, 126, 127, 65535, 65536} {
		payload := bytes.Repeat([]byte{'x'}, size)

		var buf bytes.Buffer
		if err := WriteFrame(&buf, Frame{Final: true, Opcode: BinaryMessage, Payload: payload}, true); err != nil {
			t.Fatalf("writing %d bytes = %v", size, err)
		}

		frame, err := ReadFrame(&buf, true, 0)
		if err != nil {
			t.Fatalf("reading %d bytes = %v", size, err)
		}
		if !bytes.Equal(frame.Payload, payload) {
			t.Fatalf("a frame of %d bytes did not survive the round trip", size)
		}
	}
}

// TestWriteFrameMasksFromTheClientAndNotFromTheServer is section 5.1, and
// getting it backwards is the mistake that passes every test written against
// oneself.
func TestWriteFrameMasksFromTheClientAndNotFromTheServer(t *testing.T) {
	payload := []byte("mask me")

	var fromClient bytes.Buffer
	if err := WriteFrame(&fromClient, Frame{Final: true, Opcode: TextMessage, Payload: payload}, true); err != nil {
		t.Fatalf("writing as a client = %v", err)
	}
	if fromClient.Bytes()[1]&0x80 == 0 {
		t.Fatal("a client frame went out with the mask bit clear")
	}
	if bytes.Contains(fromClient.Bytes(), payload) {
		t.Fatal("a client frame carried its payload unmasked")
	}

	var fromServer bytes.Buffer
	if err := WriteFrame(&fromServer, Frame{Final: true, Opcode: TextMessage, Payload: payload}, false); err != nil {
		t.Fatalf("writing as a server = %v", err)
	}
	if fromServer.Bytes()[1]&0x80 != 0 {
		t.Fatal("a server frame went out with the mask bit set")
	}
	if !bytes.Contains(fromServer.Bytes(), payload) {
		t.Fatal("a server frame masked a payload it must send in the clear")
	}
}

// TestReadFrameRefusesWhatSectionFiveForbids is one case per protocol error, and
// each one is a connection this server must fail rather than tolerate.
func TestReadFrameRefusesWhatSectionFiveForbids(t *testing.T) {
	for _, one := range []struct {
		name       string
		bytes      []byte
		fromClient bool
		want       error
	}{
		{
			name:       "a reserved bit with no extension negotiated",
			bytes:      []byte{0xc1, 0x80, 0, 0, 0, 0},
			fromClient: true,
			want:       ErrReservedBits,
		},
		{
			name:       "a client frame with no mask",
			bytes:      []byte{0x81, 0x00},
			fromClient: true,
			want:       ErrUnmaskedClient,
		},
		{
			name:       "a server frame with a mask",
			bytes:      []byte{0x81, 0x80, 0, 0, 0, 0},
			fromClient: false,
			want:       ErrMaskedServer,
		},
		{
			name:       "an opcode the RFC does not define",
			bytes:      []byte{0x83, 0x80, 0, 0, 0, 0},
			fromClient: true,
			want:       ErrBadOpcode,
		},
		{
			name:       "a control frame over 125 bytes",
			bytes:      append([]byte{0x89, 0xfe, 0x00, 0x7e, 0, 0, 0, 0}, bytes.Repeat([]byte{0}, 126)...),
			fromClient: true,
			want:       ErrControlTooLong,
		},
		{
			name:       "a fragmented control frame",
			bytes:      []byte{0x09, 0x80, 0, 0, 0, 0},
			fromClient: true,
			want:       ErrControlTooLong,
		},
		{
			name:       "a length whose high bit is set",
			bytes:      []byte{0x82, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			fromClient: true,
			want:       ErrTooLarge,
		},
	} {
		t.Run(one.name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(one.bytes), one.fromClient, 0)
			if !errors.Is(err, one.want) {
				t.Fatalf("reading %s = %v, want %v", one.name, err, one.want)
			}
		})
	}
}

// TestReadFrameRefusesAnOversizedHeaderWithoutAllocating is the limit doing its
// actual job: the refusal happens on the length the peer DECLARED, so a header
// claiming a gigabyte costs nothing.
//
// The proof is that the reader holds two header bytes and eight length bytes and
// no payload at all: if the check ran after the read, io.ReadFull would have
// asked for a gigabyte and failed with an unexpected EOF instead.
func TestReadFrameRefusesAnOversizedHeaderWithoutAllocating(t *testing.T) {
	header := []byte{0x82, 0xff, 0, 0, 0, 0x40, 0, 0, 0, 0, 0, 0, 0}

	_, err := ReadFrame(bytes.NewReader(header), true, 1024)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("a header declaring a gigabyte against a 1 KB limit = %v, want %v", err, ErrTooLarge)
	}
}

// TestReadFrameDoesNotBelieveADeclaredLengthWithNoLimit is the same refusal
// where there is no limit to refuse with.
//
// A connection with no read limit is not exotic: it is what [Dialer.Dial]
// returns, and it is what a caller gets by not calling SetReadLimit. The header
// below is nine bytes and declares three exabytes, and the only reason it may
// not be believed is that nothing arrived to back it up.
func TestReadFrameDoesNotBelieveADeclaredLengthWithNoLimit(t *testing.T) {
	header := []byte{0x82, 0x7f, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30}

	for _, limit := range []int64{0, -1} {
		_, err := ReadFrame(bytes.NewReader(header), false, limit)
		if err == nil {
			t.Fatalf("a header declaring three exabytes was accepted with limit %d", limit)
		}
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("reading it with limit %d = %v, want the stream to have ended", limit, err)
		}
	}
}

// TestReadFrameAppliesTheLimitToDataAndNotToControl keeps a nearly spent message
// limit from closing a healthy connection over a ping.
func TestReadFrameAppliesTheLimitToDataAndNotToControl(t *testing.T) {
	var buf bytes.Buffer
	payload := bytes.Repeat([]byte{'p'}, 100)
	if err := WriteFrame(&buf, Frame{Final: true, Opcode: PingMessage, Payload: payload}, true); err != nil {
		t.Fatalf("writing the ping = %v", err)
	}

	frame, err := ReadFrame(&buf, true, 10)
	if err != nil {
		t.Fatalf("a 100 byte ping against a 10 byte limit = %v, want it accepted", err)
	}
	if frame.Opcode != PingMessage || len(frame.Payload) != 100 {
		t.Fatalf("the ping came back as opcode %d of %d bytes", frame.Opcode, len(frame.Payload))
	}
}

// TestCloseCodeValidRefusesTheCodesThatMayNotBeSent covers section 7.4.1,
// including the three that exist only to be reported to an application.
func TestCloseCodeValidRefusesTheCodesThatMayNotBeSent(t *testing.T) {
	for _, one := range []struct {
		code int
		want bool
	}{
		{999, false},
		{1000, true},
		{1001, true},
		{1004, false},
		{CloseNoStatusReceived, false},
		{CloseAbnormalClosure, false},
		{1011, true},
		{1012, false},
		{CloseTLSHandshake, false},
		{2999, false},
		{3000, true},
		{4999, true},
		{5000, false},
	} {
		if got := CloseCodeValid(one.code); got != one.want {
			t.Errorf("CloseCodeValid(%d) = %v, want %v", one.code, got, one.want)
		}
	}
}

// TestParseCloseReadsTheCodeAndTheReason includes the empty payload, which is a
// legal close with nothing in it.
func TestParseCloseReadsTheCodeAndTheReason(t *testing.T) {
	code, reason, err := ParseClose(nil)
	if err != nil || code != CloseNoStatusReceived || reason != "" {
		t.Fatalf("an empty close = %d %q %v, want %d", code, reason, err, CloseNoStatusReceived)
	}

	code, reason, err = ParseClose(FormatClose(CloseGoingAway, "leaving"))
	if err != nil || code != CloseGoingAway || reason != "leaving" {
		t.Fatalf("a close with a reason = %d %q %v", code, reason, err)
	}

	if _, _, err := ParseClose([]byte{0x03}); !errors.Is(err, ErrBadCloseCode) {
		t.Fatalf("a one byte close = %v, want %v", err, ErrBadCloseCode)
	}
	if _, _, err := ParseClose([]byte{0x03, 0xed}); !errors.Is(err, ErrBadCloseCode) {
		t.Fatalf("a close carrying 1005 = %v, want %v", err, ErrBadCloseCode)
	}
	if _, _, err := ParseClose([]byte{0x03, 0xe8, 0xc0, 0xaf}); !errors.Is(err, ErrBadUTF8) {
		t.Fatalf("a close whose reason is not UTF-8 = %v, want %v", err, ErrBadUTF8)
	}
}

// TestFormatCloseTruncatesTheReasonToFitAControlFrame keeps a long explanation
// from becoming no explanation.
func TestFormatCloseTruncatesTheReasonToFitAControlFrame(t *testing.T) {
	payload := FormatClose(CloseInternalServerErr, strings.Repeat("e", 200))
	if len(payload) != maxControlPayload {
		t.Fatalf("a 200 byte reason produced %d bytes, want %d", len(payload), maxControlPayload)
	}

	if got := FormatClose(CloseNoStatusReceived, "ignored"); got != nil {
		t.Fatalf("formatting 1005 produced %v, want nothing to send", got)
	}
}

// TestMaskIsItsOwnInverse is the property the RFC relies on, and the reason
// unmasking on read and masking on write are the same function.
func TestMaskIsItsOwnInverse(t *testing.T) {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	want := []byte("Hello, mask")

	got := append([]byte(nil), want...)
	Mask(got, key)
	if bytes.Equal(got, want) {
		t.Fatal("masking changed nothing")
	}
	Mask(got, key)
	if !bytes.Equal(got, want) {
		t.Fatal("masking twice did not return the original")
	}
}
