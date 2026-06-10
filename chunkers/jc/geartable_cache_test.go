package jc

import (
	"sync"
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

// TestGetGearTable_NilIsStatic: a nil key returns the shared static table.
func TestGetGearTable_NilIsStatic(t *testing.T) {
	g, err := getGearTable(nil)
	if err != nil {
		t.Fatal(err)
	}
	if g != &G {
		t.Fatal("nil key did not return the shared static table")
	}
}

// TestGetGearTable_CachedByKey: equal key bytes resolve to the same cached
// pointer, distinct from the static table.
func TestGetGearTable_CachedByKey(t *testing.T) {
	key := []byte("cache-test-key-0123456789abcdef!")
	a, err := getGearTable(key)
	if err != nil {
		t.Fatal(err)
	}
	b, err := getGearTable(append([]byte(nil), key...))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("equal keys returned different table pointers (cache miss)")
	}
	if a == &G {
		t.Fatal("keyed table aliases the shared static table")
	}
}

// TestGetGearTable_DistinctKeys: different keys yield different tables.
func TestGetGearTable_DistinctKeys(t *testing.T) {
	a, _ := getGearTable([]byte("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"))
	b, _ := getGearTable([]byte("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"))
	if a == b {
		t.Fatal("different keys shared a table pointer")
	}
	if *a == *b {
		t.Fatal("different keys produced identical table contents")
	}
}

// TestGetGearTable_ConcurrentConverge: many goroutines deriving the same fresh
// key all converge on one shared table.
func TestGetGearTable_ConcurrentConverge(t *testing.T) {
	key := []byte("concurrent-key-zzzzzzzzzzzzzzzz!")
	const n = 64
	ptrs := make([]*[256]uint64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			g, _ := getGearTable(append([]byte(nil), key...))
			ptrs[i] = g
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if ptrs[i] != ptrs[0] {
			t.Fatalf("goroutine %d got a different table pointer for the same key", i)
		}
	}
}

// TestGetGearTable_SharedViaSetup: two keyed chunkers built with the same key
// share the cached table.
func TestGetGearTable_SharedViaSetup(t *testing.T) {
	key := []byte("setup-shared-key-qqqqqqqqqqqqqq!")
	mk := func() *[256]uint64 {
		c := &JC{legacy: true}
		if err := c.Setup(&chunkers.ChunkerOpts{MinSize: 2048, MaxSize: 65536, NormalSize: 8192, Key: key}); err != nil {
			t.Fatal(err)
		}
		return c.G
	}
	if mk() != mk() {
		t.Fatal("two keyed Setups with the same key did not share the cached table")
	}
}
