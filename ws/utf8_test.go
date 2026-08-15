package ws

import "testing"

// TestUTF8ValidatorAcceptsWhatIsWellFormed covers the four encoded lengths and
// the two edges of the Unicode range.
func TestUTF8ValidatorAcceptsWhatIsWellFormed(t *testing.T) {
	for _, one := range []struct {
		name  string
		bytes []byte
	}{
		{"empty", nil},
		{"ascii", []byte("hello")},
		{"two bytes", []byte("Ação")},
		{"three bytes", []byte("κόσμε")},
		{"four bytes", []byte("🜂")},
		{"the last code point", []byte{0xf4, 0x8f, 0xbf, 0xbf}},
		{"the byte order mark", []byte{0xef, 0xbb, 0xbf}},
	} {
		t.Run(one.name, func(t *testing.T) {
			var v utf8Validator
			if !v.write(one.bytes) || !v.complete() {
				t.Fatalf("%s was refused", one.name)
			}
		})
	}
}

// TestUTF8ValidatorRefusesWhatSectionFivePointSixForbids is the set of vectors
// the Autobahn suite spends its 6.* cases on.
//
// Each one is a way a decoder that only checks the leading byte lets bytes
// through: an overlong form encodes a slash in two bytes and slips past a filter
// comparing strings, and a surrogate half is a code point UTF-8 does not have.
func TestUTF8ValidatorRefusesWhatSectionFivePointSixForbids(t *testing.T) {
	for _, one := range []struct {
		name  string
		bytes []byte
	}{
		{"a lone continuation byte", []byte{0x80}},
		{"a five byte form", []byte{0xf8, 0x88, 0x80, 0x80, 0x80}},
		{"a six byte form", []byte{0xfc, 0x84, 0x80, 0x80, 0x80, 0x80}},
		{"the overlong slash", []byte{0xc0, 0xaf}},
		{"an overlong null", []byte{0xc0, 0x80}},
		{"an overlong three byte form", []byte{0xe0, 0x80, 0xaf}},
		{"an overlong four byte form", []byte{0xf0, 0x80, 0x80, 0xaf}},
		{"a high surrogate", []byte{0xed, 0xa0, 0x80}},
		{"a low surrogate", []byte{0xed, 0xbf, 0xbf}},
		{"a code point above the maximum", []byte{0xf4, 0x90, 0x80, 0x80}},
		{"a leading byte the range retired", []byte{0xf5, 0x80, 0x80, 0x80}},
		{"a leading byte where a continuation belongs", []byte{0xc2, 0xc2}},
	} {
		t.Run(one.name, func(t *testing.T) {
			var v utf8Validator
			if v.write(one.bytes) && v.complete() {
				t.Fatalf("%s was accepted", one.name)
			}
		})
	}
}

// TestUTF8ValidatorCarriesARuneAcrossWrites is the reason this exists at all
// rather than a call to utf8.Valid.
//
// A four-byte rune split one byte per frame is valid UTF-8 that a per-frame check
// refuses, and it is exactly what a client sending a long message in small
// fragments produces.
func TestUTF8ValidatorCarriesARuneAcrossWrites(t *testing.T) {
	rune4 := []byte{0xf0, 0x9f, 0x9c, 0x82}

	var v utf8Validator
	for i, b := range rune4 {
		if !v.write([]byte{b}) {
			t.Fatalf("byte %d of a split rune was refused", i)
		}
		if i < len(rune4)-1 && v.complete() {
			t.Fatalf("the message was called complete after byte %d of 4", i)
		}
	}
	if !v.complete() {
		t.Fatal("a rune split across four writes did not complete")
	}
}

// TestUTF8ValidatorRefusesAMessageThatEndsMidRune is the case where every byte
// was legal and the message is still invalid, which only complete can tell.
func TestUTF8ValidatorRefusesAMessageThatEndsMidRune(t *testing.T) {
	var v utf8Validator
	if !v.write([]byte{0xf0, 0x9f}) {
		t.Fatal("the first half of a rune was refused, and on its own it is fine")
	}
	if v.complete() {
		t.Fatal("a message ending halfway through a rune was accepted")
	}
}

// TestUTF8ValidatorLatchesOnceInvalid keeps a bad message from being rescued by
// the frames that follow it.
func TestUTF8ValidatorLatchesOnceInvalid(t *testing.T) {
	var v utf8Validator
	if v.write([]byte{0xc0, 0xaf}) {
		t.Fatal("an overlong form was accepted")
	}
	if v.write([]byte("plain ascii")) {
		t.Fatal("a later valid frame cleared the failure")
	}
	if v.complete() {
		t.Fatal("a failed message reported itself complete")
	}

	v.reset()
	if !v.write([]byte("plain ascii")) || !v.complete() {
		t.Fatal("reset did not start a new message")
	}
}
