package live

import (
	"bytes"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/ledger/transaction"
	"github.com/lunfardo314/proxima/peering"
)

type (
	lrbMsg struct {
		from peer.ID
		r    peering.LRBReply
	}
	branchListMsg struct {
		from peer.ID
		r    peering.BranchListReply
	}

	// branchFetch is the progress of reading the feeders' branch lists (SPEC.md section 6.2): from
	// the oldest slot their txstore holds to the slot of the first LRB reply. After that, branches
	// arrive by push. A restart fetches the gap again from where it stopped.
	branchFetch struct {
		started, done  bool
		firstAnswerAt  time.Time // the first LRB answer; the fetch starts when all feeders answered or a while after it
		next, target   uint32
		inFlight       bool
		sentAt         time.Time
		feeder         int
		feederOldest   uint32
		feederFloor    uint32
		entriesFetched int
	}
)

const (
	branchListPause   = 3 * time.Second  // between two requests, as in the spec's pace
	branchListTimeout = 30 * time.Second // a request unanswered this long is sent again
	branchListWait    = 15 * time.Second // how long the fetch waits for every feeder's first LRB answer
)

func (n *Node) pollLRB() {
	n.lrbAskedAt = time.Now()
	for _, f := range n.feeders {
		n.peers.SendPullLRB(f.id)
	}
}

func (n *Node) onLineage(m any) error {
	switch m := m.(type) {
	case lrbMsg:
		return n.onLRB(m.from, m.r)
	case branchListMsg:
		return n.onBranchList(m.r)
	case librariesFetched:
		return n.onLibraries(m)
	}
	return nil
}

// onLRB records a feeder's LRB answer and moves the tip to the LRB the feeders agree on.
func (n *Node) onLRB(from peer.ID, r peering.LRBReply) error {
	fd := n.feederByID[from]
	if fd == nil {
		return nil
	}
	lrb := r.LRB
	fd.lrb, fd.lrbAt, fd.oldest, fd.floor = &lrb, time.Now(), r.OldestSlot, r.PurgeFloor
	if n.branchFetch.firstAnswerAt.IsZero() {
		n.branchFetch.firstAnswerAt = time.Now()
	}
	if _, _, err := n.rowOf(lrb); err != nil {
		return err
	}
	if _, state, _ := n.lookup(lrb); state == store.StateMissing {
		n.wantPullFirst(lrb)
	}
	var lrbs []base.TransactionID
	for _, f := range n.feeders {
		if f.lrb != nil && time.Since(f.lrbAt) < lrbFresh {
			lrbs = append(lrbs, *f.lrb)
		}
	}
	picked, ok := pickLRB(lrbs, n.lineageHas)
	if !ok {
		if time.Since(n.noMajorityLogged) > 10*time.Minute {
			n.noMajorityLogged = time.Now()
			slog.Warn("the feeders do not agree on an LRB: tip held", "feeders_answering", len(lrbs))
		}
		return n.startBranchFetch()
	}
	n.lrb = &picked
	if err := n.startBranchFetch(); err != nil {
		return err
	}
	return n.updateCanonical()
}

// lineageHas tells whether branch a is branch b or one of its predecessors in the branch table.
// A lineage broken by a branch not stored yet does not reach a.
func (n *Node) lineageHas(a, b base.TransactionID) bool {
	if a == b {
		return true
	}
	cur, _, err := n.lookup(b)
	if err != nil || cur == 0 {
		return false
	}
	for {
		var slot uint32
		var pred sql.NullInt64
		if err = n.queryRow(`SELECT slot, predecessor FROM branch WHERE tx = ?`, cur).Scan(&slot, &pred); err != nil || !pred.Valid || slot <= a.Slot() {
			return false
		}
		var txid []byte
		if err = n.queryRow(`SELECT txid FROM tx WHERE id = ?`, pred.Int64).Scan(&txid); err != nil {
			return false
		}
		if bytes.Equal(txid, a[:]) {
			return true
		}
		cur = pred.Int64
	}
}

// startBranchFetch starts the branch list fetch once every alive feeder has answered PullLRB, or a
// while after the first answer: from the oldest slot any feeder's txstore holds to the newest LRB.
func (n *Node) startBranchFetch() error {
	f := &n.branchFetch
	if f.started || f.firstAnswerAt.IsZero() {
		return nil
	}
	oldest, target, answered, alive := ^uint32(0), uint32(0), 0, 0
	for _, fd := range n.feeders {
		if n.peers.IsAlive(fd.id) {
			alive++
		}
		if fd.lrb != nil {
			answered++
			oldest, target = min(oldest, fd.oldest), max(target, fd.lrb.Slot())
		}
	}
	if answered < alive && time.Since(f.firstAnswerAt) < branchListWait {
		return nil
	}
	f.started, f.target = true, target
	if f.next == 0 || f.next < oldest {
		f.next = oldest
	}
	f.feederOldest = oldest
	slog.Info("branch list fetch", "from_slot", f.next, "to_slot", f.target, "feeders_answered", answered)
	return nil
}

// refetchBranches fetches the branch list again from the canonical tip up to slot to, unless a
// fetch is under way.
func (n *Node) refetchBranches(to uint32) {
	f := &n.branchFetch
	tip, err := n.metaTxID("canonical_tip")
	if !f.done || err != nil || tip == nil || to <= tip.Slot() {
		return
	}
	f.done, f.next, f.target = false, tip.Slot()+1, to
	slog.Info("lineage gap: branch list fetched again", "from_slot", f.next, "to_slot", to)
}

// fetchBranchesStep sends the next branch list request when none is in flight, to a feeder whose
// txstore reaches the slots asked.
func (n *Node) fetchBranchesStep() error {
	f := &n.branchFetch
	if !f.started {
		return n.startBranchFetch()
	}
	if f.done {
		return nil
	}
	if f.inFlight && time.Since(f.sentAt) < branchListTimeout {
		return nil
	}
	if f.inFlight {
		f.feeder = (f.feeder + 1) % len(n.feeders)
	} else if time.Since(f.sentAt) < branchListPause {
		return nil
	}
	if f.next > f.target {
		f.done = true
		slog.Info("branch list fetch done", "entries", f.entriesFetched)
		return n.updateCanonical()
	}
	k := branchListFeeder(n.feeders, f.feeder, f.next, func(fd *feeder) bool { return n.peers.IsAlive(fd.id) })
	if k < 0 {
		return nil
	}
	f.feeder = k
	to := min(f.next+uint32(n.params.BranchListMaxSlots)-1, f.target)
	f.inFlight, f.sentAt = n.peers.SendPullBranchList(n.feeders[k].id, f.next, to), time.Now()
	return nil
}

// onBranchList records the listed branches. An entry is a claim until the branch's bytes arrive,
// when its stem names the predecessor itself (branchArrived).
func (n *Node) onBranchList(r peering.BranchListReply) error {
	f := &n.branchFetch
	for _, e := range r.Entries {
		if err := n.claimBranch(e.Branch, e.Predecessor); err != nil {
			return err
		}
	}
	f.entriesFetched += len(r.Entries)
	if r.Last && f.inFlight && r.FromSlot == f.next {
		f.inFlight = false
		f.next = r.ToSlot + 1
		f.feeder = (f.feeder + 1) % len(n.feeders)
		return n.setMeta("branch_list_next", int64(f.next))
	}
	return nil
}

// claimBranch adds a listed branch to the branch table with the predecessor the feeder names.
func (n *Node) claimBranch(branch, pred base.TransactionID) error {
	bid, _, err := n.rowOf(branch)
	if err != nil {
		return err
	}
	pid, _, err := n.rowOf(pred)
	if err != nil {
		return err
	}
	_, err = n.exec(`INSERT OR IGNORE INTO branch(tx, slot, predecessor) VALUES (?, ?, ?)`, bid, branch.Slot(), pid)
	return err
}

// branchArrived records a branch whose bytes are stored: its stem gives the predecessor and the
// totals. A listed predecessor that differs from the stem's is a wrong claim, logged.
func (n *Node) branchArrived(id int64, tx *transaction.Transaction) error {
	txid := tx.ID()
	stem := store.Stem(tx)
	pred, ok := tx.StemPredecessor()
	if stem == nil || !ok {
		return nil // parses as a branch but has no stem: linked or not, it never enters the branch table
	}
	pid, _, err := n.rowOf(pred)
	if err != nil {
		return err
	}
	var seq any
	if chainID, ok := store.SequencerID(tx); ok {
		if seq, err = n.chainRow(chainID, store.ChainSequencer); err != nil {
			return err
		}
	}
	var claimed sql.NullInt64
	err = n.queryRow(`SELECT predecessor FROM branch WHERE tx = ?`, id).Scan(&claimed)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if claimed.Valid && claimed.Int64 != pid {
		slog.Warn("branch list claimed another predecessor", "branch", tx.IDShortString())
	}
	_, err = n.exec(`INSERT INTO branch(tx, slot, sequencer, predecessor, total_coverage, total_supply, slot_inflation) VALUES (?,?,?,?,?,?,?)
		ON CONFLICT(tx) DO UPDATE SET sequencer = excluded.sequencer, predecessor = excluded.predecessor,
		total_coverage = excluded.total_coverage, total_supply = excluded.total_supply, slot_inflation = excluded.slot_inflation`,
		id, txid.Slot(), seq, pid, int64(stem.TotalCoverage), int64(stem.TotalSupply), int64(stem.SlotInflation))
	return err
}

// updateCanonical moves the canonical tip to the branch of the LRB's lineage at least
// CanonicalDepthSlots older than the LRB and numbers the lineage up to it. The first time, the
// oldest branch of the lineage the branch table knows becomes the starting branch, number 1; so it
// waits until the first branch list fetch is done.
func (n *Node) updateCanonical() error {
	if n.lrb == nil || !n.branchFetch.done {
		return nil
	}
	lrbID, _, err := n.lookup(*n.lrb)
	if err != nil || lrbID == 0 {
		return err
	}
	depth := uint32(max(n.params.CanonicalDepthSlots, 64))
	if n.lrb.Slot() < depth {
		return nil
	}
	limitSlot := n.lrb.Slot() - depth
	// walk back from the LRB to the tip, then on to the newest numbered branch or the lineage's end
	var path []int64
	var anchorNo int64
	cur, broken, breakSlot := lrbID, false, uint32(0)
	for {
		var slot uint32
		var pred, no sql.NullInt64
		err = n.queryRow(`SELECT slot, predecessor, canonical_no FROM branch WHERE tx = ?`, cur).Scan(&slot, &pred, &no)
		if errors.Is(err, sql.ErrNoRows) {
			// cur is a predecessor with no branch row: the lineage's end, or a branch not received
			// yet, which is pulled
			var txid []byte
			var state int
			if err = n.queryRow(`SELECT txid, state, slot FROM tx WHERE id = ?`, cur).Scan(&txid, &state, &breakSlot); err != nil {
				return err
			}
			if state == store.StateMissing {
				id, err := base.TransactionIDFromBytes(txid)
				if err != nil {
					return err
				}
				n.wantPullFirst(id)
			}
			broken = true
			break
		}
		if err != nil {
			return err
		}
		if slot <= limitSlot {
			if no.Valid {
				anchorNo = no.Int64
				break
			}
			path = append(path, cur)
		}
		if !pred.Valid {
			break
		}
		cur = pred.Int64
	}
	if len(path) == 0 {
		return nil
	}
	var maxNo sql.NullInt64
	if err = n.queryRow(`SELECT max(canonical_no) FROM branch`).Scan(&maxNo); err != nil {
		return err
	}
	if broken && maxNo.Valid {
		// a branch of the lineage is not stored yet: wait for it rather than renumber, and fetch
		// the branch list between the canonical tip and the gap, so that one request fills the
		// lineage instead of one pull per missing branch
		n.refetchBranches(breakSlot)
		return nil
	}
	if maxNo.Valid && anchorNo != maxNo.Int64 {
		// the feeders' lineage leaves the canonical one below its tip: a real fork (SPEC.md
		// section 6.6), whose renumbering is not built; the tip stays where it is
		slog.Error("the LRB's lineage forks below the canonical tip; canonical tip not moved",
			"fork_point_no", anchorNo, "canonical_tip_no", maxNo.Int64)
		return nil
	}
	if anchorNo == 0 {
		start := path[len(path)-1]
		var txid []byte
		if err = n.queryRow(`SELECT txid, slot FROM tx WHERE id = ?`, start).Scan(&txid, &n.startSlot); err != nil {
			return err
		}
		if err = n.setMeta("starting_branch", txid); err != nil {
			return err
		}
		slog.Info("starting branch", "slot", n.startSlot)
	}
	for i := len(path) - 1; i >= 0; i-- {
		anchorNo++
		if _, err = n.exec(`UPDATE branch SET canonical_no = ?, fork = NULL WHERE tx = ?`, anchorNo, path[i]); err != nil {
			return err
		}
	}
	var tip []byte
	if err = n.queryRow(`SELECT txid FROM tx WHERE id = ?`, path[0]).Scan(&tip); err != nil {
		return err
	}
	return n.setMeta("canonical_tip", tip)
}
