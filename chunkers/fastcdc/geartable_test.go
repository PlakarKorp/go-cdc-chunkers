package fastcdc

import (
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

// TestGearTable_UnkeyedShared asserts that unkeyed chunkers alias the single
// shared static table rather than each holding a 2 KiB copy. This is what
// keeps per-chunker memory tiny when many chunkers run concurrently.
func TestGearTable_UnkeyedShared(t *testing.T) {
	mk := func() *FastCDC {
		c := &FastCDC{normalLevel: 2}
		if err := c.Setup(&chunkers.ChunkerOpts{MinSize: 2048, MaxSize: 65536, NormalSize: 8192}); err != nil {
			t.Fatal(err)
		}
		return c
	}
	a, b := mk(), mk()
	if a.G != &G {
		t.Fatal("unkeyed chunker did not alias the shared static table")
	}
	if a.G != b.G {
		t.Fatal("two unkeyed chunkers point at different tables")
	}
}

// TestGearTable_KeyedDerivedAndIsolated asserts that keyed chunkers get a
// freshly derived table (per key) that neither aliases the shared static table
// nor another key's table — i.e. moving G to a pointer did not weaken keying.
func TestGearTable_KeyedDerivedAndIsolated(t *testing.T) {
	derive := func(key []byte) *[256]uint64 {
		c := &FastCDC{normalLevel: 2, keyed: true, legacy: true}
		if err := c.Setup(&chunkers.ChunkerOpts{MinSize: 2048, MaxSize: 65536, NormalSize: 8192, Key: key}); err != nil {
			t.Fatal(err)
		}
		return c.G
	}
	k1 := make([]byte, 32)
	k1[0] = 1
	k2 := make([]byte, 32)
	k2[0] = 2

	g1, g2 := derive(k1), derive(k2)
	if g1 == &G || g2 == &G {
		t.Fatal("keyed table aliases the shared static table")
	}
	if *g1 == G {
		t.Fatal("keyed table has the same contents as the static table")
	}
	if *g1 == *g2 {
		t.Fatal("two different keys produced identical tables")
	}
}
