package live

import (
	"bytes"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/lunfardo314/proxima/ledger/base"
)

// feeder is one feeder as the loop knows it. The counters are written by peering's goroutines too.
type feeder struct {
	id         peer.ID
	lrb        *base.TransactionID // its latest LRB answer
	lrbAt      time.Time
	oldest     uint32 // oldest slot its txstore holds, and its purge floor
	floor      uint32
	budgetAt   time.Time // the second the pull budget below counts in
	budgetUsed int

	pushes, pulledIn, pullsSent, timeouts atomic.Int64
}

// lrbFresh is how old a feeder's LRB answer may be and still count: three polls.
const lrbFresh = 3 * time.Minute

// pickLRB returns the LRB the feeders agree on (SPEC.md, tip and branch table): a candidate is
// supported by every feeder whose own LRB has it on its lineage, so feeders a slot or two apart on
// one lineage agree; the newest candidate supported by a majority wins. Without a majority, as when
// the feeders are split between lineages, there is no answer and the tip stays where it is.
// onLineage(a, b) tells whether a is b or one of its predecessors.
func pickLRB(lrbs []base.TransactionID, onLineage func(a, b base.TransactionID) bool) (base.TransactionID, bool) {
	var best base.TransactionID
	found := false
	for _, c := range lrbs {
		support := 0
		for _, f := range lrbs {
			if onLineage(c, f) {
				support++
			}
		}
		if 2*support <= len(lrbs) {
			continue
		}
		if !found || c.Slot() > best.Slot() || (c.Slot() == best.Slot() && bytes.Compare(c[:], best[:]) < 0) {
			best, found = c, true
		}
	}
	return best, found
}

// pullFeeder returns the feeder a pull goes to after tried attempts: the ladder starts at a feeder
// chosen by the transaction's slot, so the pulls of one slot go to one feeder and the next slot's to
// the next (SPEC.md, pull policy), and walks the feeders in turn from there.
func pullFeeder(txid base.TransactionID, tried, feeders int) int {
	return (int(txid.Slot()) + tried) % feeders
}

// branchListFeeder picks, from start in turn, the first alive feeder whose txstore reaches slot; if
// none is known to, the first alive one. -1 if none is alive.
func branchListFeeder(fs []*feeder, start int, slot uint32, alive func(*feeder) bool) int {
	fallback := -1
	for i := range fs {
		k := (start + i) % len(fs)
		if !alive(fs[k]) {
			continue
		}
		if fs[k].lrb != nil && fs[k].oldest <= slot {
			return k
		}
		if fallback < 0 {
			fallback = k
		}
	}
	return fallback
}
