package live

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger/base"
)

// The pull ladder (SPEC.md section 6.3): the feeders first, starting at the one the slot picks,
// then the other dagstores among the peers, then the broad pull, then the round ends. A step that
// cannot serve the transaction is skipped: a peer not alive, a feeder whose txstore starts above
// the slot or was purged above it, and the broad pull for a transaction no canonical branch waits
// for, since a broad pull costs every peer a lookup.
func TestPullLadder(t *testing.T) {
	f := newForkNode(t, "ladder") // a branch table: canonical branches, waiting rows added below
	n := f.n
	a, b, c := &feeder{id: peer.ID("a")}, &feeder{id: peer.ID("b")}, &feeder{id: peer.ID("c")}
	n.feeders = []*feeder{a, b}
	n.others = []*feeder{c}
	down := map[peer.ID]bool{}
	n.alive = func(id peer.ID) bool { return !down[id] }

	txid := func(name string, slot uint32) base.TransactionID {
		var id base.TransactionID
		binary.BigEndian.PutUint32(id[:], slot)
		copy(id[6:], name)
		return id
	}
	// slot 7 starts at feeder 7 % 2 = 1 (b), then a, then the other dagstore c, then broad
	x := txid("x", 7)
	want := []*feeder{b, a, c, nil}
	for i, w := range want {
		got, ok := n.ladderStep(x, i)
		if !ok || got != w {
			t.Fatalf("step %d: %v, want %v", i, got, w)
		}
	}
	if _, ok := n.ladderStep(x, len(want)); ok {
		t.Fatalf("the ladder goes on past the broad pull")
	}

	now := time.Now()
	p := &pullState{}
	if !n.servable(x, p, a, now) {
		t.Errorf("an alive feeder with no known txstore bounds is skipped")
	}
	down["a"] = true
	if n.servable(x, p, a, now) {
		t.Errorf("a feeder not alive is asked")
	}
	b.oldest = 8
	if n.servable(x, p, b, now) {
		t.Errorf("a feeder whose txstore starts after the slot is asked")
	}
	b.oldest, b.floor = 1, 8
	if n.servable(x, p, b, now) {
		t.Errorf("a feeder that purged the slot is asked")
	}
	b.floor = 7
	if !n.servable(x, p, b, now) {
		t.Errorf("a feeder holding the slot is skipped")
	}

	// broad: only for a transaction a canonical branch waits for, or a branch of the LRB's lineage
	f.follow(t, "A300")
	res, err := n.exec(`INSERT INTO tx(txid, slot, tick, state) VALUES (?, 7, 0, ?)`, x[:], store.StateMissing)
	must(t, err)
	xid, _ := res.LastInsertId()
	if n.servable(x, p, nil, now) {
		t.Errorf("a broad pull for a transaction nobody waits for")
	}
	_, err = n.exec(`INSERT INTO waiting(tx, branch) VALUES (?, ?)`, xid, f.ids["L150"])
	must(t, err)
	if n.servable(x, p, nil, now) {
		t.Errorf("a broad pull for a transaction only a losing branch waits for")
	}
	_, err = n.exec(`INSERT INTO waiting(tx, branch) VALUES (?, ?)`, xid, f.ids["A120"])
	must(t, err)
	if !n.servable(x, p, nil, now) {
		t.Errorf("no broad pull for a transaction a canonical branch waits for")
	}
	y := txid("y", 9)
	if !n.servable(y, &pullState{lineage: true}, nil, now) {
		t.Errorf("no broad pull for a branch of the LRB's lineage")
	}
}
