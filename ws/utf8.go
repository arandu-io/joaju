// Copyright 2013 The HYZIS WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ws

import "unicode/utf8"

// utf8Validator checks UTF-8 across frame boundaries.
//
// RFC 6455 section 5.6 requires a text message to be valid UTF-8, and section
// 8.1 requires the connection to be failed as soon as that is known -- not after
// the last fragment. A four-byte rune may be split over two frames, so the check
// has to carry state: the bytes still owed, the value assembled so far, and the
// length the rune claimed.
//
// The reason it cannot buffer the message and call utf8.Valid at the end: a peer
// may send a hundred megabytes of text in 125-byte fragments, and the point of
// the incremental check is to refuse it on the first bad byte instead of after
// the last good one.
type utf8Validator struct {
	// pending is how many continuation bytes are still owed.
	pending int
	// value is the code point assembled so far, used for the overlong and
	// surrogate checks that only the full value can decide.
	value rune
	// need is the total length the rune in progress claimed, kept because the
	// smallest legal value depends on it.
	need int
	// bad latches: once invalid, always invalid.
	bad bool
}

// reset starts a new message.
func (v *utf8Validator) reset() { *v = utf8Validator{} }

// write feeds the next bytes and reports whether everything so far is still
// valid UTF-8.
//
// This is a decoder written out rather than a call to utf8.DecodeRune in a loop,
// and the state is why: DecodeRune needs the whole rune in one slice, and here
// the rune may be one byte in this frame and three in the next.
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

		// The rune is complete, and three things can still be wrong with it: it
		// can be encoded in more bytes than it needs, which is an overlong form
		// and is how a slash gets past a naive filter; it can be a surrogate
		// half, which UTF-8 does not encode; or it can be above the Unicode
		// maximum.
		switch {
		case v.value < minValueForLength[v.need]:
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

// complete reports whether what has been written ends on a rune boundary.
//
// A text message whose last frame stops halfway through a rune is invalid, and
// this is the only place that can tell -- every byte in it was legal on its own.
func (v *utf8Validator) complete() bool { return !v.bad && v.pending == 0 }

// minValueForLength is the smallest code point each encoded length may carry,
// which is what makes an overlong form detectable.
var minValueForLength = [5]rune{0, 0, 0x80, 0x800, 0x10000}
