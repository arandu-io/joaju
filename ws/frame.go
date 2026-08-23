// Package ws is the WebSocket protocol of RFC 6455, written for this server.
//
// It is not a fork and it borrows no code. The RFC is the specification, and
// what is written here is written here.
//
// # What it is, and what it deliberately is not
//
// It is a server. There is a [Dialer.Dial] for tests and for one instance talking to
// another, and there is nothing else on the client side: no proxy support, no
// redirect following, no cookie jar. A library carries those because it does not
// know who is calling; this one does.
//
// It does not do permessage-deflate. Compression on a websocket is a per-message
// negotiation, a second framing path, and a CRIME-shaped hazard when the payload
// mixes attacker-controlled text with a session identifier. The Autobahn cases
// for it are 12.* and 13.*, and those two groups are outside the conformance
// target.
package ws

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"unicode/utf8"
)

// The frame opcodes of RFC 6455 section 5.2.
//
// The names are the ones every websocket library uses, because a reader who has
// seen one has seen these.
const (
	// ContinuationFrame carries the rest of a message whose first frame said it
	// was not final.
	ContinuationFrame = 0
	// TextMessage is UTF-8. The RFC requires the server to check that, and
	// [ValidUTF8] is where it is checked.
	TextMessage = 1
	// BinaryMessage is bytes with no meaning attached.
	BinaryMessage = 2
	// CloseMessage carries an optional code and reason and ends the connection.
	CloseMessage = 8
	// PingMessage asks for a PongMessage with the same payload.
	PingMessage = 9
	// PongMessage answers a ping, or arrives unsolicited as a keepalive.
	PongMessage = 10
)

// The close codes of RFC 6455 section 7.4.1.
const (
	CloseNormalClosure           = 1000
	CloseGoingAway               = 1001
	CloseProtocolError           = 1002
	CloseUnsupportedData         = 1003
	CloseNoStatusReceived        = 1005
	CloseAbnormalClosure         = 1006
	CloseInvalidFramePayloadData = 1007
	ClosePolicyViolation         = 1008
	CloseMessageTooBig           = 1009
	CloseMandatoryExtension      = 1010
	CloseInternalServerErr       = 1011
	CloseTLSHandshake            = 1015
)

// maxControlPayload is the ceiling RFC 6455 section 5.5 puts on a control
// frame: 125 bytes, and it may not be fragmented.
//
// The limit is what lets a close, a ping and a pong be read without allocating
// on what the peer said the length is.
const maxControlPayload = 125

var (
	// ErrControlTooLong is a control frame over 125 bytes, or fragmented.
	ErrControlTooLong = errors.New("ws: a control frame may not exceed 125 bytes or be fragmented")
	// ErrReservedBits is a frame with RSV1, RSV2 or RSV3 set.
	//
	// Those bits mean an extension negotiated in the handshake, and none is
	// negotiated here. The RFC requires failing the connection rather than
	// ignoring them: a bit whose meaning was never agreed is a frame whose
	// meaning is not known.
	ErrReservedBits = errors.New("ws: the frame sets a reserved bit and no extension was negotiated")
	// ErrUnmaskedClient is a frame from a client that carries no mask.
	//
	// Section 5.1 requires it, and the reason is not confidentiality -- the mask
	// key travels in the clear. It exists so that a payload a client controls
	// cannot be made to look like a valid HTTP request to a proxy sitting
	// between them, which is a cache poisoning attack the masking defeats.
	ErrUnmaskedClient = errors.New("ws: a frame from a client must be masked")
	// ErrMaskedServer is a frame from a server that carries a mask, which
	// section 5.1 forbids.
	ErrMaskedServer = errors.New("ws: a frame from a server must not be masked")
	// ErrBadOpcode is an opcode the RFC does not define.
	ErrBadOpcode = errors.New("ws: unknown opcode")
	// ErrBadCloseCode is a close frame whose code the RFC reserves or forbids
	// on the wire.
	ErrBadCloseCode = errors.New("ws: the close frame carries a code that may not be sent")
	// ErrBadUTF8 is a text message, or a close reason, that is not valid UTF-8.
	ErrBadUTF8 = errors.New("ws: a text payload must be valid UTF-8")
	// ErrTooLarge is a message over the reader's limit.
	ErrTooLarge = errors.New("ws: the message is larger than the limit")
	// ErrBadFragment is a continuation with nothing to continue, or a new data
	// frame arriving in the middle of a fragmented message.
	ErrBadFragment = errors.New("ws: the frame does not continue anything, or interrupts a fragmented message")
)

// Frame is one WebSocket frame, as section 5.2 lays it out.
//
// It is the wire unit and not the message unit: a message may arrive as several
// frames, and [Reader] is what joins them. Control frames may be interleaved
// between the fragments of a data message, which is why reading a message means
// reading frames rather than reading bytes until the end.
type Frame struct {
	// Final is the FIN bit: this is the last frame of its message.
	Final bool
	// Opcode is one of the constants above.
	Opcode int
	// Payload is the unmasked application data, in a slice of this frame's own.
	//
	// [ReadFrame] allocates it rather than handing out a window into a shared
	// buffer, because unmasking is done in place and a caller that keeps a
	// frame -- a ping whose payload has to be echoed back as a pong -- would
	// otherwise be reading whatever the next frame overwrote it with.
	Payload []byte
}

// Control reports whether the opcode is a control frame, which may not be
// fragmented and may not exceed 125 bytes.
func (f Frame) Control() bool { return f.Opcode >= CloseMessage }

// ReadFrame reads one frame, validating everything section 5 requires.
//
// fromClient says which side sent it, and it decides the masking rule: a client
// must mask and a server must not. Getting that backwards is the mistake that
// makes an implementation interoperate with itself and nothing else.
//
// limit bounds the payload. It is checked against the length the peer declared,
// BEFORE anything is allocated -- a header claiming eight exabytes must cost a
// refusal and not an allocation.
//
// Zero or less is no limit, and then the declared length is nothing but the
// peer's word: the payload is assembled as its bytes arrive, so the memory
// follows what was actually sent rather than what was announced.
func ReadFrame(r io.Reader, fromClient bool, limit int64) (Frame, error) {
	var head [2]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return Frame{}, err
	}

	final := head[0]&0x80 != 0
	if head[0]&0x70 != 0 {
		return Frame{}, ErrReservedBits
	}
	opcode := int(head[0] & 0x0f)

	switch opcode {
	case ContinuationFrame, TextMessage, BinaryMessage, CloseMessage, PingMessage, PongMessage:
	default:
		return Frame{}, fmt.Errorf("%w: %d", ErrBadOpcode, opcode)
	}

	masked := head[1]&0x80 != 0
	if fromClient && !masked {
		return Frame{}, ErrUnmaskedClient
	}
	if !fromClient && masked {
		return Frame{}, ErrMaskedServer
	}

	length := int64(head[1] & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return Frame{}, err
		}
		length = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return Frame{}, err
		}
		n := binary.BigEndian.Uint64(ext[:])
		// The high bit must be zero (section 5.2), and the value has to fit an
		// int64 before it is one.
		if n > 1<<62 {
			return Frame{}, ErrTooLarge
		}
		length = int64(n)
	}

	// A control frame is checked before the length is trusted for anything
	// else: it may not be fragmented and may not exceed 125 bytes, and both are
	// protocol errors rather than size errors.
	if opcode >= CloseMessage {
		if length > maxControlPayload || !final {
			return Frame{}, ErrControlTooLong
		}
	} else if limit > 0 && length > limit {
		// The limit is the caller's, and it applies only to data. A control
		// frame is already bounded at 125 bytes above, and refusing a ping
		// because a message limit is nearly spent would close a healthy
		// connection over a frame that costs nothing to read.
		return Frame{}, ErrTooLarge
	}

	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(r, mask[:]); err != nil {
			return Frame{}, err
		}
	}

	// The limit, when there is one, has already accepted this length, and that
	// is what makes it safe to allocate it in one go.
	payload, err := readPayload(r, length, limit > 0)
	if err != nil {
		return Frame{}, err
	}
	if masked {
		Mask(payload, mask)
	}

	return Frame{Final: final, Opcode: opcode, Payload: payload}, nil
}

// payloadChunk is how much of an unvouched payload is read at a time.
//
// Thirty-two kilobytes: large enough that a message of any ordinary size costs
// a handful of reads, small enough that a peer which declares a payload and
// sends nothing has bought thirty-two kilobytes and not a terabyte.
const payloadChunk = 32 << 10

// readPayload reads n bytes of application data.
//
// vouched says a limit has already accepted n, so the buffer may be allocated
// at that size in one call. Without it n is only what the peer wrote in a
// header nine bytes long, and the value that header can carry is four
// exabytes -- so the buffer grows with the bytes that actually arrive, and a
// declared length nobody sends the bytes for costs one chunk.
func readPayload(r io.Reader, n int64, vouched bool) ([]byte, error) {
	if vouched || n <= payloadChunk {
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}

		return payload, nil
	}

	payload := make([]byte, 0, payloadChunk)
	chunk := make([]byte, payloadChunk)
	for int64(len(payload)) < n {
		want := n - int64(len(payload))
		if want > payloadChunk {
			want = payloadChunk
		}

		if _, err := io.ReadFull(r, chunk[:want]); err != nil {
			if len(payload) > 0 && errors.Is(err, io.EOF) {
				// The stream ended between two chunks, which for the caller is
				// the same short frame as one that ended inside a chunk.
				err = io.ErrUnexpectedEOF
			}

			return nil, err
		}
		payload = append(payload, chunk[:want]...)
	}

	return payload, nil
}

// WriteFrame writes one frame.
//
// fromClient decides masking, the same way [ReadFrame] decides validation. A
// server never masks, so the common path here allocates no key and copies
// nothing.
func WriteFrame(w io.Writer, f Frame, fromClient bool) error {
	if f.Control() && (len(f.Payload) > maxControlPayload || !f.Final) {
		return ErrControlTooLong
	}

	header := make([]byte, 0, 14)

	first := byte(f.Opcode)
	if f.Final {
		first |= 0x80
	}
	header = append(header, first)

	n := len(f.Payload)
	var lengthByte byte
	switch {
	case n <= 125:
		lengthByte = byte(n)
	case n <= 0xffff:
		lengthByte = 126
	default:
		lengthByte = 127
	}
	if fromClient {
		lengthByte |= 0x80
	}
	header = append(header, lengthByte)

	switch {
	case n > 0xffff:
		header = binary.BigEndian.AppendUint64(header, uint64(n))
	case n > 125:
		header = binary.BigEndian.AppendUint16(header, uint16(n))
	}

	if !fromClient {
		if _, err := w.Write(header); err != nil {
			return err
		}
		_, err := w.Write(f.Payload)
		return err
	}

	// A client masks with a fresh key per frame. math/rand/v2 rather than
	// crypto/rand: the key is not a secret -- it travels in the clear, four
	// bytes ahead of the payload it masks -- and what it has to be is
	// unpredictable to the party that would abuse it, which is the intermediary
	// and not the peer. RFC 6455 section 5.3 asks for exactly that.
	mask := [4]byte{}
	binary.LittleEndian.PutUint32(mask[:], rand.Uint32())
	header = append(header, mask[:]...)

	masked := make([]byte, n)
	copy(masked, f.Payload)
	Mask(masked, mask)

	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(masked)
	return err
}

// Mask applies the four-byte key of section 5.3, in place.
//
// It is its own function because it is the one piece of this package that runs
// per byte of every client frame, and because a mask applied with the wrong
// offset produces a payload that decodes to garbage with no error anywhere.
func Mask(b []byte, key [4]byte) {
	for i := range b {
		b[i] ^= key[i&3]
	}
}

// CloseCodeValid reports whether a close frame may carry this code on the wire.
//
// 1005 and 1006 exist to be reported to an application and are never sent:
// "no status received" and "abnormal closure" describe the absence of a close
// frame, so putting either INSIDE one is a contradiction the RFC forbids. 1015
// is the same for TLS. 1004 is a hole in the middle of the defined range -- an
// earlier draft used it for "frame too large", the final RFC does not define it,
// and section 7.4.1 leaves it reserved. Below 1000 and in the 1016-2999 range
// the RFC reserves the space; 3000 and up is for applications and libraries.
func CloseCodeValid(code int) bool {
	switch {
	case code == CloseNoStatusReceived, code == CloseAbnormalClosure,
		code == CloseTLSHandshake, code == 1004:
		return false
	case code >= 1000 && code <= 1011:
		return true
	case code >= 3000 && code <= 4999:
		return true
	default:
		return false
	}
}

// ParseClose reads the code and reason out of a close payload.
//
// An empty payload is a close with no code, which is legal and means
// [CloseNoStatusReceived] to the application. Anything else must be at least two
// bytes, must carry a code that may be sent, and its reason must be UTF-8 --
// the RFC applies the same text rule to a close reason as to a text message.
func ParseClose(payload []byte) (code int, reason string, err error) {
	switch {
	case len(payload) == 0:
		return CloseNoStatusReceived, "", nil
	case len(payload) == 1:
		return 0, "", fmt.Errorf("%w: a close payload of one byte carries half a code", ErrBadCloseCode)
	}

	code = int(binary.BigEndian.Uint16(payload[:2]))
	if !CloseCodeValid(code) {
		return 0, "", fmt.Errorf("%w: %d", ErrBadCloseCode, code)
	}

	reason = string(payload[2:])
	if !ValidUTF8(payload[2:]) {
		return 0, "", ErrBadUTF8
	}
	return code, reason, nil
}

// FormatClose builds a close payload.
//
// The reason is truncated to fit the 125-byte control limit rather than
// refused. A close that fails to send because its explanation was too long
// leaves the peer with no explanation at all, which is worse than a short one.
//
// It is truncated at a rune boundary, and the RFC is why: a close reason is
// held to the same UTF-8 rule as a text message, so a cut that lands inside a
// rune turns an explanation into a frame the peer must fail the connection
// over. The reason gets shorter by up to three bytes; nothing else changes.
func FormatClose(code int, reason string) []byte {
	if code == CloseNoStatusReceived {
		return nil
	}
	payload := make([]byte, 2, 2+len(reason))
	binary.BigEndian.PutUint16(payload, uint16(code))

	room := maxControlPayload - 2
	if len(reason) > room {
		reason = reason[:room]
		for len(reason) > 0 {
			// A lone RuneError of one byte is the cut showing: the rune it ends
			// in is not there any more. One that is three bytes long was in the
			// text to begin with, and stays.
			if r, size := utf8.DecodeLastRuneInString(reason); r != utf8.RuneError || size > 1 {
				break
			}
			reason = reason[:len(reason)-1]
		}
	}
	return append(payload, reason...)
}
