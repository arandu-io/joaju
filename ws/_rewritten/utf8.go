package ws

import "unicode/utf8"

// ValidUTF8 reports whether b is well-formed UTF-8.
//
// It is the whole-payload check, for a close reason and for a text message that
// arrived in one frame. A message split across frames uses [utf8Validator]
// instead, because the split may fall in the middle of a rune.
func ValidUTF8(b []byte) bool { return utf8.Valid(b) }

// utf8Validator checks UTF-8 across frame boundaries.
//
// RFC 6455 section 5.6 requires a text message to be valid UTF-8, and section
// 8.1 requires the server to fail the connection with 1007 as soon as it knows
// -- not after the last fragment. A four-byte rune may be split over two
// frames, so the check has to carry state: bytes seen, bytes still owed, and
// what the value so far means for the range checks.
//
// The reason it cannot just buffer and call [ValidUTF8] at the end: a client may
// send a 100 MB text message in 125-byte fragments, and the whole point of the
// incremental check is to refuse it on the first bad byte instead of after the
// last good one.
type utf8Validator struct {
	// pending is how many continuation bytes are still owed.
	pending int
	// value is the code point assembled so far, used for the overlong and
	// surrogate checks that only the full value can decide.
	value rune
	// need is the total length of the rune in progress, kept because the
	// minimum legal value depends on it.
	need int
	// bad latches: once invalid, always invalid.
	bad bool
}

// reset starts a new message.
func (v *utf8Validator) reset() { *v = utf8Validator{} }

// write feeds the next bytes of the message and reports whether everything so
// far is still valid UTF-8.
//
// This is a decoder written by hand rather than a call to utf8.DecodeRune in a
// loop, and the reason is the state: DecodeRune needs the whole rune in one
// slice, and here the rune may be one byte in this frame and three in the next.
func (v *utf8Validator) write(b []byte) bool {
	if v.bad {
		return false
	}

	for _, c := range b {
		if v.pending == 0 {
			switch {
			case c < 0x80:
				continue
			case c&0xe0 == 0xc0:
				v.pending, v.need, v.value = 1, 2, rune(c&0x1f)
			case c&0xf0 == 0xe0:
				v.pending, v.need, v.value = 2, 3, rune(c&0x0f)
			case c&0xf8 == 0xf0:
				v.pending, v.need, v.value = 3, 4, rune(c&0x07)
			default:
				// 0x80-0xbf is a continuation with nothing to continue;
				// 0xf8-0xff is a five- or six-byte form that Unicode retired.
				v.bad = true
				return false
			}
			continue
		}

		if c&0xc0 != 0x80 {
			v.bad = true
			return false
		}
		v.value = v.value<<6 | rune(c&0x3f)
		v.pending--
		if v.pending > 0 {
			continue
		}

		// The rune is complete, and three things can still be wrong with it:
		// it can be encoded in more bytes than it needs (an overlong form,
		// which is how a slash gets past a naive filter), it can be a surrogate
		// half, which UTF-8 does not encode, or it can be above the Unicode
		// maximum.
		switch {
		case v.value < minForLength[v.need]:
			v.bad = true
		case v.value >= 0xd800 && v.value <= 0xdfff:
			v.bad = true
		case v.value > utf8.MaxRune:
			v.bad = true
		}
		if v.bad {
			return false
		}
	}

	return true
}

// complete reports whether the message ended on a rune boundary.
//
// A text message whose last frame stops halfway through a rune is invalid, and
// this is the only place that can tell -- every byte in it was legal on its own.
func (v *utf8Validator) complete() bool { return !v.bad && v.pending == 0 }

// minForLength is the smallest code point each encoded length may carry, which
// is what makes an overlong form detectable.
var minForLength = [5]rune{0, 0, 0x80, 0x800, 0x10000}
