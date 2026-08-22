package ws

import (
	"testing"
	"unicode/utf8"
)

// FuzzUTF8Validator checks the incremental decoder against the standard
// library, over every way of splitting the same bytes.
//
// The chunk size is part of the input because the whole reason this decoder
// exists is the split: a four-byte rune may arrive one byte in this frame and
// three in the next, and a verdict that depends on where the frames were cut is
// a connection that fails on payload size rather than on content.
func FuzzUTF8Validator(f *testing.F) {
	f.Add([]byte("não é o desenvolvedor"), uint8(1))
	f.Add([]byte{0xc0, 0xaf}, uint8(1))
	f.Add([]byte{0xf0, 0x9f}, uint8(3))
	f.Add([]byte{0xed, 0xa0, 0x80}, uint8(2))
	f.Add([]byte{0xf4, 0x90, 0x80, 0x80}, uint8(4))
	f.Add([]byte{0xf0, 0x9f, 0x92, 0xa1}, uint8(1))
	f.Add([]byte(nil), uint8(0))

	f.Fuzz(func(t *testing.T, data []byte, chunk uint8) {
		size := int(chunk%16) + 1

		var v utf8Validator
		v.reset()

		accepted := true
		for start := 0; start < len(data); start += size {
			end := min(start+size, len(data))
			if !v.write(data[start:end]) {
				accepted = false

				break
			}
		}

		got := accepted && v.complete()
		if want := utf8.Valid(data); got != want {
			t.Fatalf("validating %d bytes in chunks of %d = %v, want %v", len(data), size, got, want)
		}
		if got != ValidUTF8(data) {
			t.Fatalf("the incremental verdict %v disagrees with the whole-payload one", got)
		}

		// Once invalid, always invalid: the connection is being failed, and a
		// later frame that happens to be well formed may not un-fail it.
		if !accepted {
			if v.write([]byte("a")) || v.complete() {
				t.Fatal("a validator that rejected a byte accepted the next one")
			}
		}
	})
}
