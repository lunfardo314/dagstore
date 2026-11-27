package live

import (
	"bytes"
	"cmp"
	"database/sql"
	"errors"
	"log/slog"
	"slices"
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
	heartbeatMsg struct {
		from peer.ID
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
		// the trailing check: the branch list fetched again behind the canonical tip, so that a
		// branch whose push a full send queue dropped, and which nothing references, still enters
		// the branch table; checked is the slot it has reached
		checking  bool
		checked   uint32
		checkedAt time.Time
	}
)

const (
	branchListPause   = 3 * time.Second  // between two requests, as in the spec's pace
	branchListTimeout = 30 * time.Second // a request unanswered this long is sent again
	branchListWait    = 15 * time.Second // how long the fetch waits for every feeder's first LRB answer
	branchCheckPeriod = time.Minute      // the trailing check: at most one request this often
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
	case heartbeatMsg:
		n.onDAGStorePeer(m.from)
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
		cntTipHeld.Add(1)
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

// onDAGStorePeer records a peer announcing itself as a dagstore: unless it is a feeder, it joins
// the pull ladder after the feeders.
func (n *Node) onDAGStorePeer(id peer.ID) {
	if fd := n.feederByID[id]; fd != nil {
		if !fd.isDAGStore {
			fd.isDAGStore = true
			slog.Info("a feeder is a dagstore: pulled from at the source pace", "peer", peering.ShortPeerIDString(id))
		}
		return
	}
	if n.otherByID[id] != nil {
		return
	}
	fd := &feeder{id: id, isDAGStore: true}
	n.others = append(n.others, fd)
	n.otherByID[id] = fd
	slog.Info("another dagstore among the peers: asked for what the feeders lack", "peer", peering.ShortPeerIDString(id))
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

// startBranchCheck starts the trailing check over the slots after the last checked, up to the
// canonical tip and at most one request's range, once a minute while the tip moves.
func (n *Node) startBranchCheck() error {
	f := &n.branchFetch
	if time.Since(f.checkedAt) < branchCheckPeriod {
		return nil
	}
	tip, err := n.metaTxID("canonical_tip")
	if err != nil || tip == nil || f.checked >= tip.Slot() {
		return err
	}
	f.checkedAt = time.Now()
	f.done, f.checking = false, true
	f.next, f.target = f.checked+1, min(tip.Slot(), f.checked+uint32(n.params.BranchListMaxSlots))
	return nil
}

// fetchBranchesStep sends the next branch list request when none is in flight, to a feeder whose
// txstore reaches the slots asked.
func (n *Node) fetchBranchesStep() error {
	f := &n.branchFetch
	if !f.started {
		return n.startBranchFetch()
	}
	if f.done {
		return n.startBranchCheck()
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
		if f.checking {
			f.checking, f.checked = false, f.target
			return n.setMeta("branch_list_checked", int64(f.checked))
		}
		if f.checked == 0 {
			// what the first fetch listed this deep behind its target had arrived at the feeders
			f.checked = f.target - min(f.target, uint32(max(n.params.CanonicalDepthSlots, 64)))
		}
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
		if f.checking {
			return nil
		}
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
	if n.importing {
		depth = 0 // an archive segment gives its canonical lineage: its LRB is already canonical
	}
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
	var demoted []int64
	if maxNo.Valid && anchorNo != maxNo.Int64 {
		if anchorNo == 0 {
			// the feeders' lineage does not meet the stored one at all: it leaves it below the
			// starting branch, deeper than the history held
			slog.Error("the LRB's lineage does not reach the canonical lineage; canonical tip not moved",
				"canonical_tip_no", maxNo.Int64)
			return nil
		}
		// the feeders' lineage leaves the canonical one below its tip: a real fork (SPEC.md
		// section 6.6). The old lineage above the fork point loses its numbers and becomes a fork,
		// and the new one is numbered from the fork point on.
		if demoted, err = n.demoteAbove(anchorNo); err != nil {
			return err
		}
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
		// the walk back to the lineage's end asked for the predecessor of the starting branch:
		// it is below the history held, pruned, not to be pulled
		var pred []byte
		err = n.queryRow(`SELECT p.txid FROM branch b JOIN tx p ON p.id = b.predecessor WHERE b.tx = ? AND p.state = ?`, start, store.StateMissing).Scan(&pred)
		if err == nil {
			if _, err = n.exec(`UPDATE tx SET state = ? WHERE txid = ?`, store.StatePruned, pred); err != nil {
				return err
			}
			if id, err := base.TransactionIDFromBytes(pred); err == nil {
				delete(n.pulls, id)
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	forkPoint := anchorNo
	var forks []int64 // forks holding branches numbered now: only after a real fork
	for i := len(path) - 1; i >= 0; i-- {
		anchorNo++
		var fork sql.NullInt64
		if err = n.queryRow(`SELECT fork FROM branch WHERE tx = ?`, path[i]).Scan(&fork); err != nil {
			return err
		}
		if fork.Valid {
			forks = append(forks, fork.Int64)
		}
		// walked = 0: a branch walked as non-canonical before a real fork is walked again as
		// canonical, to confirm its cone
		if _, err = n.exec(`UPDATE branch SET canonical_no = ?, fork = NULL, walked = 0 WHERE tx = ?`, anchorNo, path[i]); err != nil {
			return err
		}
	}
	if demoted != nil || forks != nil {
		if err = n.rebuildForks(demoted, forks, forkPoint); err != nil {
			return err
		}
	}
	var tip []byte
	if err = n.queryRow(`SELECT txid FROM tx WHERE id = ?`, path[0]).Scan(&tip); err != nil {
		return err
	}
	return n.setMeta("canonical_tip", tip)
}

// demoteAbove takes the canonical numbers from the branches numbered above fork point forkNo,
// and their confirmations from what they confirmed: the canonical walk of the new lineage
// confirms it again, as far as its cones reach. It returns the demoted branches, oldest first.
// Everything they stored stays, sealed or not.
func (n *Node) demoteAbove(forkNo int64) ([]int64, error) {
	rows, err := n.query(`SELECT tx FROM branch WHERE canonical_no > ? ORDER BY canonical_no`, forkNo)
	if err != nil {
		return nil, err
	}
	var demoted []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		demoted = append(demoted, id)
	}
	rows.Close()
	for _, b := range demoted {
		if err = n.unconfirm(b); err != nil {
			return nil, err
		}
	}
	if _, err = n.exec(`UPDATE branch SET canonical_no = NULL WHERE canonical_no > ?`, forkNo); err != nil {
		return nil, err
	}
	cntRenumbered.Add(1)
	slog.Warn("real fork: the feeders' lineage leaves the canonical one below its tip, canonical lineage renumbered",
		"fork_point_no", forkNo, "demoted_branches", len(demoted))
	return demoted, nil
}

// unconfirm clears the confirmations of branch b. What b confirmed is reached from b through
// transactions b confirmed, so the walk visits those only, not the whole table.
func (n *Node) unconfirm(b int64) error {
	stack := []int64{b}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		res, err := n.exec(`UPDATE tx SET confirming_branch = NULL WHERE id = ? AND confirming_branch = ?`, id, b)
		if err != nil {
			return err
		}
		if k, _ := res.RowsAffected(); k == 0 {
			continue
		}
		deps, err := n.deps(id)
		if err != nil {
			return err
		}
		stack = append(stack, deps...)
	}
	return nil
}

// rebuildForks recomputes, after a real fork, the fork rows it changed: the forks of the branches
// numbered canonical now, the forks leaving from a demoted branch, and the demoted branches
// themselves. Their rows are deleted and every walked non-canonical branch among them is assigned
// again in slot order, as the walk assigns them. Walk, sealing and canonical tips move back to
// the fork point, from where the new lineage is walked.
func (n *Node) rebuildForks(demoted, forks []int64, forkNo int64) error {
	for _, b := range demoted {
		rows, err := n.query(`SELECT id FROM fork WHERE fork_point = ?`, b)
		if err != nil {
			return err
		}
		for rows.Next() {
			var f int64
			if err = rows.Scan(&f); err != nil {
				rows.Close()
				return err
			}
			forks = append(forks, f)
		}
		rows.Close()
	}
	members := append([]int64(nil), demoted...)
	for _, f := range forks {
		rows, err := n.query(`SELECT tx FROM branch WHERE fork = ?`, f)
		if err != nil {
			return err
		}
		for rows.Next() {
			var b int64
			if err = rows.Scan(&b); err != nil {
				rows.Close()
				return err
			}
			members = append(members, b)
		}
		rows.Close()
		if _, err = n.exec(`UPDATE branch SET fork = NULL WHERE fork = ?`, f); err != nil {
			return err
		}
		if _, err = n.exec(`DELETE FROM fork WHERE id = ?`, f); err != nil {
			return err
		}
	}
	type member struct {
		tx   int64
		slot uint32
	}
	var again []member
	seen := make(map[int64]bool)
	for _, b := range members {
		if seen[b] {
			continue
		}
		seen[b] = true
		var m member
		var canonical, walked bool
		if err := n.queryRow(`SELECT tx, slot, canonical_no IS NOT NULL, walked FROM branch WHERE tx = ?`, b).Scan(&m.tx, &m.slot, &canonical, &walked); err != nil {
			return err
		}
		if !canonical && walked {
			again = append(again, m)
		}
	}
	slices.SortFunc(again, func(a, b member) int { return cmp.Compare(a.slot, b.slot) })
	// taken after the deletes: SQLite reuses the rowids of deleted rows
	var firstNew int64
	if err := n.queryRow(`SELECT coalesce(max(id), 0) + 1 FROM fork`).Scan(&firstNew); err != nil {
		return err
	}
	for _, m := range again {
		if err := n.assignFork(m.tx); err != nil {
			return err
		}
	}
	if _, err := n.exec(`UPDATE fork SET sealed_tip = (SELECT b.tx FROM branch b WHERE b.fork = fork.id AND b.sealed = 1 ORDER BY b.slot DESC LIMIT 1)
		WHERE id >= ?`, firstNew); err != nil {
		return err
	}

	var fpSlot uint32
	if err := n.queryRow(`SELECT slot FROM branch WHERE canonical_no = ?`, forkNo).Scan(&fpSlot); err != nil {
		return err
	}
	if n.walkedNo > forkNo {
		n.walkedNo, n.walkedSlot = forkNo, fpSlot
	}
	var sealed []byte
	err := n.queryRow(`SELECT t.txid FROM branch b JOIN tx t ON t.id = b.tx WHERE b.canonical_no IS NOT NULL AND b.sealed = 1
		ORDER BY b.canonical_no DESC LIMIT 1`).Scan(&sealed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return n.setMeta("sealed_tip", sealed)
}
