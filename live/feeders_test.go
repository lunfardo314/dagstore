package live

import (
	"encoding/binary"
	"testing"

	"github.com/lunfardo314/proxima/ledger/base"
)

// branchAt is a stand-in transaction ID of a branch at a slot; tag tells apart branches of one slot.
func branchAt(slot uint32, tag byte) base.TransactionID {
	var id base.TransactionID
	binary.BigEndian.PutUint32(id[:4], slot)
	id[31] = tag
	return id
}

// lineages is a branch tree for tests: each branch's predecessor.
type lineages map[base.TransactionID]base.TransactionID

func (l lineages) has(a, b base.TransactionID) bool {
	for cur, ok := b, true; ok; cur, ok = l[cur] {
		if cur == a {
			return true
		}
	}
	return false
}

// pickLRB takes the newest LRB that a majority of the feeders have on their lineage; a feeder a slot
// behind on the same lineage agrees, a split without majority gives no answer.
func TestPickLRB(t *testing.T) {
	// main lineage 100 <- 101 <- 102 <- 103, side lineage 101 <- 102b <- 103b
	b100, b101, b102, b103 := branchAt(100, 0), branchAt(101, 0), branchAt(102, 0), branchAt(103, 0)
	s102, s103 := branchAt(102, 1), branchAt(103, 1)
	l := lineages{b101: b100, b102: b101, b103: b102, s102: b101, s103: s102}

	cases := []struct {
		name string
		lrbs []base.TransactionID
		want base.TransactionID
		ok   bool
	}{
		{"one feeder", []base.TransactionID{b103}, b103, true},
		{"same LRB", []base.TransactionID{b103, b103, b103}, b103, true},
		// two of three at 103, one a slot behind: 103 is on two lineages of three, 102 on all three
		{"one feeder behind", []base.TransactionID{b103, b103, b102}, b103, true},
		// one at 103, two behind: 103 has one of three, 102 has all three
		{"two feeders behind", []base.TransactionID{b103, b102, b102}, b102, true},
		// two on the side lineage, one on the main: the newest the majority shares is the side tip
		{"majority on another lineage", []base.TransactionID{s103, s103, b103}, s103, true},
		// one each way: only the common ancestor 101 would have both, and it is no one's LRB
		{"split, no majority", []base.TransactionID{s103, b103}, base.TransactionID{}, false},
	}
	for _, c := range cases {
		got, ok := pickLRB(c.lrbs, l.has)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("%s: got %v %v, want %v %v", c.name, got.Slot(), ok, c.want.Slot(), c.ok)
		}
	}
}

// pullFeeder spreads first pulls over the feeders by slot, and the ladder visits every feeder once
// per round from there.
func TestPullFeeder(t *testing.T) {
	const feeders = 3
	first := make(map[int]int)
	for slot := uint32(1000); slot < 1030; slot++ {
		first[pullFeeder(branchAt(slot, 0), 0, feeders)]++
	}
	for k := 0; k < feeders; k++ {
		if first[k] != 10 {
			t.Errorf("feeder %d gets %d of 30 first pulls, want 10", k, first[k])
		}
	}
	tx := branchAt(1001, 0)
	seen := make(map[int]bool)
	for tried := 0; tried < feeders; tried++ {
		seen[pullFeeder(tx, tried, feeders)] = true
	}
	if len(seen) != feeders {
		t.Errorf("a round visits %d feeders, want %d", len(seen), feeders)
	}
}

// branchListFeeder asks an alive feeder whose txstore reaches the slot, in turn from start, and
// falls back to any alive one when none is known to.
func TestBranchListFeeder(t *testing.T) {
	lrb := branchAt(500, 0)
	fs := []*feeder{
		{lrb: &lrb, oldest: 300}, // reaches back to 300
		{lrb: &lrb, oldest: 100}, // reaches back to 100
		{lrb: &lrb, oldest: 100}, // reaches back to 100, but dead in some cases below
	}
	alive := func(dead ...int) func(*feeder) bool {
		return func(f *feeder) bool {
			for _, d := range dead {
				if f == fs[d] {
					return false
				}
			}
			return true
		}
	}
	cases := []struct {
		name  string
		start int
		slot  uint32
		alive func(*feeder) bool
		want  int
	}{
		{"all reach it: the one in turn", 0, 400, alive(), 0},
		{"the one in turn does not reach it", 0, 200, alive(), 1},
		{"turn moves on", 2, 200, alive(), 2},
		{"the one reaching it is dead: the next that does", 2, 200, alive(2), 1},
		{"none reaches it: any alive", 0, 50, alive(), 0},
		{"none alive", 0, 200, alive(0, 1, 2), -1},
	}
	for _, c := range cases {
		if got := branchListFeeder(fs, c.start, c.slot, c.alive); got != c.want {
			t.Errorf("%s: feeder %d, want %d", c.name, got, c.want)
		}
	}
}
