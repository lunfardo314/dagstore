package live

import (
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/peering"
)

// A dagstore feeds another the way a node does (SPEC.md section 5.4, replication): it pushes what
// its intake stores for the first time to the dagstores it declares in peering.dagstores, answers
// their pulls from its database, and answers their lineage requests. Only transactions travel: the
// replica builds its own branch table, walks and seals, so the two check each other. These
// handlers run on peering's goroutines and read only committed data.

// setLRBReply refreshes the answer to PullLRB, after each commit: the LRB this dagstore's own
// feeders agree on, and its starting slot as the oldest slot it holds. Nothing is answered before
// both are known.
func (n *Node) setLRBReply() {
	if n.lrb == nil || n.startSlot == 0 {
		return
	}
	n.lrbReply.Store(&peering.LRBReply{LRB: *n.lrb, OldestSlot: n.startSlot})
}

func (n *Node) answerPullLRB(from peer.ID) {
	if r := n.lrbReply.Load(); r != nil {
		n.peers.SendLRBReply(from, *r)
	}
}

// answerPullBranchList lists the branches of the slot range whose bytes are stored, with the
// predecessor their stem names: claims from a branch list are not passed on. A range as long as a
// node refuses is refused the same way.
func (n *Node) answerPullBranchList(from peer.ID, fromSlot, toSlot uint32) {
	if toSlot < fromSlot || toSlot-fromSlot >= uint32(n.params.BranchListMaxSlots) {
		return
	}
	rows, err := n.readDB().Query(`SELECT t.txid, p.txid FROM branch b JOIN tx t ON t.id = b.tx JOIN tx p ON p.id = b.predecessor
		WHERE b.slot BETWEEN ? AND ? AND t.state IN (?, ?) AND t.parsed = 1 ORDER BY b.slot`,
		fromSlot, toSlot, store.StateLinked, store.StateIntake)
	if err != nil {
		return
	}
	defer rows.Close()
	entries := make([]peering.BranchListEntry, 0)
	for rows.Next() {
		var b, p []byte
		if rows.Scan(&b, &p) != nil {
			return
		}
		var e peering.BranchListEntry
		var err1, err2 error
		e.Branch, err1 = base.TransactionIDFromBytes(b)
		e.Predecessor, err2 = base.TransactionIDFromBytes(p)
		if err1 != nil || err2 != nil {
			return
		}
		entries = append(entries, e)
	}
	if rows.Err() == nil {
		n.peers.SendBranchListReply(from, fromSlot, toSlot, entries)
	}
}
