package ws

import (
	"bytes"
	"runtime"
	"strings"
	"testing"
	"unicode/utf8"
)

// allocationSlack is how much one frame read may allocate on top of the bytes
// it was handed.
//
// A reader holding n bytes cannot produce a payload longer than n, so anything
// far above n is memory the peer's declared length bought without sending a
// byte for it. The slack covers the reader, the frame, the scratch buffer and
// whatever the runtime charges to the goroutine while it works.
const allocationSlack = 8 << 20

// declaresExtendedLength reports whether the header uses the eight-byte length
// form of section 5.2, which is the only one that can name more bytes than a
// machine has.
//
// The allocation invariant is checked on those headers alone: reading the
// memory statistics stops the world, and the two shorter length forms cannot
// declare more than 65535 bytes.
func declaresExtendedLength(data []byte) bool {
	return len(data) >= 10 && data[1]&0x7f == 127
}

// FuzzReadFrame feeds arbitrary bytes to the frame reader.
//
// This is the first thing a client's bytes reach, before any authentication, so
// what is asserted is what the reader promises about untrusted input: it
// returns rather than panicking, it never allocates on a length nobody sent the
// bytes for, an accepted frame obeys the rules of section 5, and an accepted
// frame written back out reads back the same.
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

	f.Fuzz(func(t *testing.T, data []byte, fromClient bool, limit int64) {
		if declaresExtendedLength(data) {
			// The reader is a fixed slice, so it can never satisfy a header that
			// declares more than the slice holds. Whatever such a header costs
			// is memory that was allocated on the peer's word alone.
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, _ = ReadFrame(bytes.NewReader(data), fromClient, limit)
			runtime.ReadMemStats(&after)

			if grown := after.TotalAlloc - before.TotalAlloc; grown > uint64(len(data))+allocationSlack {
				t.Fatalf("reading %d bytes with limit %d allocated %d bytes", len(data), limit, grown)
			}
		}

		frame, err := ReadFrame(bytes.NewReader(data), fromClient, limit)
		if err != nil {
			return
		}

		switch frame.Opcode {
		case ContinuationFrame, TextMessage, BinaryMessage, CloseMessage, PingMessage, PongMessage:
		default:
			t.Fatalf("an accepted frame carries opcode %d", frame.Opcode)
		}

		if len(frame.Payload) > len(data) {
			t.Fatalf("a payload of %d bytes came out of %d bytes of input", len(frame.Payload), len(data))
		}
		if frame.Control() && (len(frame.Payload) > maxControlPayload || !frame.Final) {
			t.Fatalf("an accepted control frame carries %d bytes and final %v", len(frame.Payload), frame.Final)
		}
		if !frame.Control() && limit > 0 && int64(len(frame.Payload)) > limit {
			t.Fatalf("an accepted data frame of %d bytes passed a limit of %d", len(frame.Payload), limit)
		}

		// What was decoded, encoded again, decodes to the same frame. The mask
		// is fresh on every client write, so the wire bytes differ and the frame
		// does not.
		var out bytes.Buffer
		if err := WriteFrame(&out, frame, fromClient); err != nil {
			t.Fatalf("writing back a frame that was just read = %v", err)
		}
		again, err := ReadFrame(&out, fromClient, 0)
		if err != nil {
			t.Fatalf("rereading a frame that was just written = %v", err)
		}
		if again.Final != frame.Final || again.Opcode != frame.Opcode || !bytes.Equal(again.Payload, frame.Payload) {
			t.Fatalf("the round trip turned %v %d of %d bytes into %v %d of %d bytes",
				frame.Final, frame.Opcode, len(frame.Payload),
				again.Final, again.Opcode, len(again.Payload))
		}
	})
}

// FuzzParseClose feeds arbitrary bytes to the close payload reader.
//
// A close payload is 125 bytes at most and it arrives before anything else is
// known about the peer, so what it must never do is hand an application a code
// the RFC forbids on the wire, or a reason that is not UTF-8.
func FuzzParseClose(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte{0x03, 0xe8})
	f.Add([]byte{0x03})
	f.Add([]byte{0x03, 0xed})
	f.Add([]byte{0x03, 0xe8, 'b', 'y', 'e'})
	f.Add([]byte{0x03, 0xe8, 0xc0, 0xaf})
	f.Add([]byte{0x0f, 0xa0, 0xc3, 0xa9})

	f.Fuzz(func(t *testing.T, data []byte) {
		code, reason, err := ParseClose(data)
		if err != nil {
			if code != 0 || reason != "" {
				t.Fatalf("a refused close still returned %d %q", code, reason)
			}

			return
		}

		if len(data) == 0 {
			if code != CloseNoStatusReceived || reason != "" {
				t.Fatalf("an empty close returned %d %q", code, reason)
			}

			return
		}

		if !CloseCodeValid(code) {
			t.Fatalf("an accepted close carries code %d", code)
		}
		if !utf8.ValidString(reason) {
			t.Fatalf("an accepted close carries a reason that is not UTF-8: %q", reason)
		}
		if reason != string(data[2:]) {
			t.Fatalf("the reason %q is not the payload's tail %q", reason, data[2:])
		}

		// A close payload that fits a control frame is exactly what the
		// formatter would have produced for the same code and reason.
		if len(data) <= maxControlPayload {
			if got := FormatClose(code, reason); !bytes.Equal(got, data) {
				t.Fatalf("formatting %d %q gave %v, want %v", code, reason, got, data)
			}
		}
	})
}

// FuzzFormatClose is the other half: what the formatter builds, the reader must
// accept.
//
// The reason is truncated to fit the 125-byte control limit, and the peer holds
// a truncated reason to the same UTF-8 rule as any other text -- so a cut that
// lands inside a rune produces a close frame the peer must fail the connection
// over.
func FuzzFormatClose(f *testing.F) {
	f.Add(CloseNormalClosure, "")
	f.Add(CloseGoingAway, "leaving")
	f.Add(CloseNoStatusReceived, "ignored")
	f.Add(CloseInternalServerErr, strings.Repeat("e", 200))
	f.Add(CloseProtocolError, strings.Repeat("é", 40))
	// Two reasons whose truncation lands inside a rune, one two bytes wide and
	// one four. Coverage does not lead here on its own: the branch that
	// truncates is already reached by a reason of plain ASCII, and what is
	// wrong with these is the byte the cut falls on rather than the branch.
	f.Add(ClosePolicyViolation, strings.Repeat("é", 90))
	f.Add(CloseMessageTooBig, strings.Repeat("💡", 40))
	f.Add(4000, "aplicação")

	f.Fuzz(func(t *testing.T, code int, reason string) {
		payload := FormatClose(code, reason)
		if len(payload) > maxControlPayload {
			t.Fatalf("formatting %d with a %d byte reason gave %d bytes", code, len(reason), len(payload))
		}

		if code == CloseNoStatusReceived {
			if payload != nil {
				t.Fatalf("formatting the no-status code gave %v, want nothing to send", payload)
			}

			return
		}

		// A code the RFC forbids on the wire, or a reason that was never valid
		// UTF-8, is the caller's bug and not this function's contract.
		if !CloseCodeValid(code) || !utf8.ValidString(reason) {
			return
		}

		gotCode, gotReason, err := ParseClose(payload)
		if err != nil {
			t.Fatalf("reading back %d with a %d byte reason = %v", code, len(reason), err)
		}
		if gotCode != code {
			t.Fatalf("the code %d came back as %d", code, gotCode)
		}
		if !strings.HasPrefix(reason, gotReason) {
			t.Fatalf("the reason %q came back as %q, which is not a prefix of it", reason, gotReason)
		}
		if len(reason) <= maxControlPayload-2 && gotReason != reason {
			t.Fatalf("a reason of %d bytes was shortened to %q", len(reason), gotReason)
		}
	})
}

// FuzzMask asserts the property the RFC leans on: the mask is its own inverse,
// at every length and every offset.
//
// Length matters because the key repeats every four bytes, so a payload whose
// length is not a multiple of four is where an implementation that unrolls the
// loop stops agreeing with itself.
func FuzzMask(f *testing.F) {
	f.Add([]byte("Hello, mask"), uint32(0x37fa213d))
	f.Add([]byte(nil), uint32(0))
	f.Add([]byte{0}, uint32(0xffffffff))

	f.Fuzz(func(t *testing.T, payload []byte, key uint32) {
		k := [4]byte{byte(key >> 24), byte(key >> 16), byte(key >> 8), byte(key)}

		got := append([]byte(nil), payload...)
		Mask(got, k)
		Mask(got, k)

		if !bytes.Equal(got, payload) {
			t.Fatalf("masking %d bytes twice did not return the original", len(payload))
		}
	})
}
