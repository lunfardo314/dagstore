package api

import (
	"bytes"
	"testing"
)

// prefixEnd bounds the prefix scans of the DAG explorer adapter: every key with the prefix must
// sort below it, and the first key past the prefix must not.
func TestPrefixEnd(t *testing.T) {
	for _, c := range []struct{ prefix, end []byte }{
		{[]byte{0x00, 0x03, 0x8a}, []byte{0x00, 0x03, 0x8b}},
		{[]byte{0x00, 0x03, 0xff}, []byte{0x00, 0x04}}, // carry drops the trailing 0xff
		{[]byte{0xff, 0xff}, nil},                      // no key is greater: scan to the end
	} {
		if got := prefixEnd(c.prefix); !bytes.Equal(got, c.end) {
			t.Errorf("prefixEnd(%x) = %x, want %x", c.prefix, got, c.end)
		}
	}
}

// A slot range [from, to] is scanned as txid >= slotKey(from) AND txid < slotEndKey(to): txids
// begin with the big-endian slot, so a 32-byte txid of slot s sorts between the two keys of s.
func TestSlotKeys(t *testing.T) {
	txid := append([]byte{0x00, 0x03, 0x8a, 0x40}, bytes.Repeat([]byte{0xff}, 28)...) // slot 232000
	if bytes.Compare(txid, slotKey(232000)) < 0 || bytes.Compare(txid, slotEndKey(232000)) >= 0 {
		t.Errorf("txid of slot 232000 is outside its slot keys")
	}
	if bytes.Compare(txid, slotEndKey(231999)) < 0 {
		t.Errorf("txid of slot 232000 sorts inside slot 231999")
	}
	if bytes.Compare(bytes.Repeat([]byte{0xff}, 32), slotEndKey(^uint32(0))) >= 0 {
		t.Errorf("the last slot has no end key")
	}
}

// Cursors are passed back by clients, so a malformed one must be a client error, and the empty
// cursor must start before every row, including rows whose second component is 0.
func TestPairCursor(t *testing.T) {
	if a, b, err := parsePairCursor(pairCursor(232000, 17)); err != nil || a != 232000 || b != 17 {
		t.Errorf("round trip: %d %d %v", a, b, err)
	}
	if a, b, err := parsePairCursor(""); err != nil || a != 0 || b != -1 {
		t.Errorf("empty cursor: %d %d %v", a, b, err)
	}
	for _, bad := range []string{"x", "1-", "-1-2x", "12"} {
		if _, _, err := parsePairCursor(bad); err == nil {
			t.Errorf("cursor %q accepted", bad)
		}
	}
}
