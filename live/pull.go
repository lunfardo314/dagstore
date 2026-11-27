package live

import (
	"bytes"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lunfardo314/dagstore/api"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/peering"
)

var cntLinked, cntSigFailed, cntPullsSent, cntBroadPulls, cntLost, cntPurged, cntTipHeld, cntRenumbered atomic.Int64

// pullState is the pull ladder of one missing transaction (SPEC.md section 6.3): the feeders in
// turn, then the other dagstores among the peers, each given PullTimeout, then, for a transaction a
// canonical branch waits for, one broad pull to every peer. A round is the whole ladder,
// MissingRetryInterval apart, and after MissingRetryRounds rounds the transaction is declared lost.
type pullState struct {
	tried    int     // steps of the ladder tried in this round
	to       *feeder // the peer of the pull in flight, nil for a broad pull
	inFlight bool
	sentAt   time.Time
	nextAt   time.Time
	rounds   int
	asked    []string // the peers asked in this round, for the missing record
	// unavailable: a step of this round was skipped because its peer was not alive or paused,
	// which may pass: such a round does not count towards declaring the transaction lost
	unavailable bool
	lineage     bool // a branch of the LRB's lineage: the canonical tip waits for it, so it goes first
}

func (n *Node) wantPull(txid base.TransactionID) {
	if _, ok := n.pulls[txid]; !ok {
		n.pulls[txid] = &pullState{nextAt: time.Now()}
	}
}

// wantPullFirst pulls a branch the canonical tip waits for ahead of everything else.
func (n *Node) wantPullFirst(txid base.TransactionID) {
	n.wantPull(txid)
	n.pulls[txid].lineage = true
}

// ladderStep returns where attempt tried of txid's ladder goes: a feeder, another dagstore, or the broad
// pull (nil, true); false past the end of the ladder.
func (n *Node) ladderStep(txid base.TransactionID, tried int) (*feeder, bool) {
	f, d := len(n.feeders), len(n.others)
	switch {
	case tried < f:
		return n.feeders[pullFeeder(txid, tried, f)], true
	case tried < f+d:
		return n.others[tried-f], true
	case tried == f+d:
		return nil, true
	}
	return nil, false
}

// pullDue sends the pulls due, as far as the budgets allow: PullsPerSecondPerFeeder per node
// feeder and HistoryPullsPerSecond over all of them, SourcePullsPerSecond per dagstore (a source is
// built to serve history, the nodes are not), BroadPullPerSecond for broad pulls. It handles
// timeouts first; then the pulls go out lineage branches first, then oldest first: sealing advances
// in slot order, so the oldest needs are the ones holding it up, and a txid begins with its slot.
// A step that cannot serve the transaction is skipped as tried: a peer not alive or paused, a
// feeder whose txstore does not reach the slot, a broad pull for a transaction no canonical branch
// waits for. A pull whose step has no budget left this second waits for the next call.
func (n *Node) pullDue() error {
	now := time.Now()
	type cand struct {
		txid base.TransactionID
		p    *pullState
		to   *feeder
	}
	var cands []cand
	for txid, p := range n.pulls {
		if p.inFlight {
			timeout := n.params.PullTimeout
			if p.to == nil {
				timeout *= 2 // a broad pull waits for the slowest of every peer
			}
			if now.Sub(p.sentAt) < timeout {
				continue
			}
			p.inFlight = false
			if p.to != nil {
				p.to.timedOut(now)
			}
			p.tried++
			p.nextAt = now
		}
		if now.Before(p.nextAt) {
			continue // not due: between rounds, nothing is tried, not even skipped
		}
		var to *feeder
		for {
			fd, ok := n.ladderStep(txid, p.tried)
			if !ok || n.servable(txid, p, fd, now) {
				to = fd
				break
			}
			if fd != nil && !n.pullable(fd, now) {
				p.unavailable = true
			}
			p.tried++
		}
		if _, ok := n.ladderStep(txid, p.tried); !ok {
			p.tried = 0
			if p.unavailable {
				// a source that may hold it could not be asked: the same round again, soon
				p.unavailable = false
				p.nextAt = now.Add(n.params.PullTimeout)
				continue
			}
			p.rounds++
			if err := n.endOfRound(txid, p); err != nil {
				return err
			}
			continue
		}
		cands = append(cands, cand{txid, p, to})
	}
	slices.SortFunc(cands, func(a, b cand) int {
		if a.p.lineage != b.p.lineage {
			if a.p.lineage {
				return -1
			}
			return 1
		}
		return bytes.Compare(a.txid[:], b.txid[:])
	})
	for _, c := range cands {
		if c.to == nil {
			if !n.broad.take(now, n.params.BroadPullPerSecond) {
				continue
			}
			n.gate.AddPulled(c.txid)
			if n.peers.PullTransactionsFromPeers(c.txid) == 0 {
				c.p.tried++
				continue
			}
			cntBroadPulls.Add(1)
			c.p.inFlight, c.p.sentAt, c.p.to = true, now, nil
			c.p.asked = append(c.p.asked, "broad")
			continue
		}
		if c.to.isDAGStore {
			if !c.to.budget.take(now, n.params.SourcePullsPerSecond) {
				continue
			}
		} else {
			if !c.to.budget.has(now, n.params.PullsPerSecondPerFeeder) || !n.history.take(now, n.params.HistoryPullsPerSecond) {
				continue
			}
			c.to.budget.take(now, n.params.PullsPerSecondPerFeeder)
		}
		n.gate.AddPulled(c.txid)
		if n.peers.PullTransactionFromPeer(c.to.id, c.txid) {
			c.p.inFlight, c.p.sentAt, c.p.to = true, now, c.to
			c.p.asked = append(c.p.asked, peering.ShortPeerIDString(c.to.id))
			c.to.pullsSent.Add(1)
			cntPullsSent.Add(1)
		}
	}
	return nil
}

// budget counts what was used in the current second.
type budget struct {
	at   time.Time
	used int
}

func (b *budget) has(now time.Time, perSecond int) bool {
	return now.Sub(b.at) >= time.Second || b.used < perSecond
}

// take uses one unit if there is one left this second.
func (b *budget) take(now time.Time, perSecond int) bool {
	if now.Sub(b.at) >= time.Second {
		b.at, b.used = now, 0
	}
	if b.used >= perSecond {
		return false
	}
	b.used++
	return true
}

// servable tells whether a step of the ladder can serve the transaction: a peer alive and not
// paused whose txstore, as far as it told, reaches the slot; for the broad pull, a transaction a
// canonical branch waits for, since every peer pays for a broad pull.
func (n *Node) servable(txid base.TransactionID, p *pullState, fd *feeder, now time.Time) bool {
	if fd == nil {
		return p.lineage || n.canonicalWaits(txid)
	}
	slot := txid.Slot()
	return n.pullable(fd, now) && (fd.oldest == 0 || fd.oldest <= slot) && (fd.floor == 0 || fd.floor <= slot)
}

// canonicalWaits tells whether a canonical branch waits for the transaction.
func (n *Node) canonicalWaits(txid base.TransactionID) bool {
	var one int
	err := n.queryRow(`SELECT 1 FROM tx t JOIN waiting w ON w.tx = t.id JOIN branch b ON b.tx = w.branch
		WHERE t.txid = ? AND b.canonical_no IS NOT NULL LIMIT 1`, txid[:]).Scan(&one)
	return err == nil
}

// pullable tells whether pulls may go to the feeder: alive for peering, and not paused for not
// answering.
func (n *Node) pullable(f *feeder, now time.Time) bool {
	return n.alive(f.id) && !now.Before(f.pausedUntil)
}

// endOfRound records a round of the ladder that found nothing, with the peers it asked, and
// declares the transaction lost after the last round. A lost record is kept forever; it no longer
// holds up sealing.
func (n *Node) endOfRound(txid base.TransactionID, p *pullState) error {
	id, _, err := n.lookup(txid)
	if err != nil {
		return err
	}
	tried := fmt.Sprintf("round %d: %s", p.rounds, strings.Join(p.asked, ", "))
	if len(p.asked) == 0 {
		tried = fmt.Sprintf("round %d: no peer could be asked", p.rounds)
	}
	p.asked = nil
	if _, err = n.exec(`UPDATE missing SET rounds = ?, tried = ? WHERE tx = ?`, p.rounds, tried, id); err != nil {
		return err
	}
	if p.rounds < n.params.MissingRetryRounds {
		p.nextAt = time.Now().Add(n.params.MissingRetryInterval)
		return nil
	}
	delete(n.pulls, txid)
	cntLost.Add(1)
	slog.Warn("transaction lost", "tx", txid.StringShort(), "rounds", p.rounds)
	_, err = n.exec(`UPDATE tx SET state = ? WHERE id = ?`, store.StateLost, id)
	return err
}

// restore reads back what the loop keeps in memory: the starting slot, the branch list fetch's
// position and the pending pulls.
func (n *Node) restore() error {
	if v, err := n.metaTxID("starting_branch"); err != nil {
		return err
	} else if v != nil {
		n.startSlot = v.Slot()
	}
	if err := n.queryRow(`SELECT coalesce(max(canonical_no), 0), coalesce(max(slot), 0) FROM branch WHERE walked = 1 AND canonical_no IS NOT NULL`).
		Scan(&n.walkedNo, &n.walkedSlot); err != nil {
		return err
	}
	var next int64
	if err := n.queryRow(`SELECT value FROM meta WHERE key = 'branch_list_next'`).Scan(&next); err == nil {
		n.branchFetch.next = uint32(next)
	}
	if err := n.queryRow(`SELECT value FROM meta WHERE key = 'branch_list_checked'`).Scan(&next); err == nil {
		n.branchFetch.checked = uint32(next)
	}
	rows, err := n.query(`SELECT t.txid, m.rounds FROM missing m JOIN tx t ON t.id = m.tx WHERE t.state = ?`, store.StateMissing)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var txid []byte
		var rounds int
		if err = rows.Scan(&txid, &rounds); err != nil {
			return err
		}
		id, err := base.TransactionIDFromBytes(txid)
		if err != nil {
			return err
		}
		n.pulls[id] = &pullState{nextAt: time.Now(), rounds: rounds}
	}
	return rows.Err()
}

// updateInfo refreshes what the API shows of the live state, after each commit.
func (n *Node) updateInfo() {
	info := api.LiveStatus{
		Feeders:         len(n.feeders),
		FeedersAlive:    0,
		PendingPulls:    len(n.pulls),
		Pushed:          cntPushed.Load(),
		PulledIn:        cntPulledIn.Load(),
		Duplicates:      cntDuplicate.Load(),
		NotFromFeeders:  cntNotFeeder.Load(),
		BadBytes:        cntBadBytes.Load(),
		Stored:          cntStored.Load(),
		Linked:          cntLinked.Load(),
		SigFailed:       cntSigFailed.Load(),
		PullsSent:       cntPullsSent.Load(),
		BroadPulls:      cntBroadPulls.Load(),
		Lost:            cntLost.Load(),
		Purged:          cntPurged.Load(),
		VersionMismatch: cntVersionMismatch.Load(),
		Unparsed:        cntUnparsed.Load(),
		TipHeld:         cntTipHeld.Load(),
		Renumbered:      cntRenumbered.Load(),
		Intake:          len(n.in),
	}
	status := func(fd *feeder) api.FeederStatus {
		fs := api.FeederStatus{Peer: fd.id.String(), Alive: n.peers.IsAlive(fd.id), OldestSlot: fd.oldest, PurgeFloor: fd.floor,
			Pushes: fd.pushes.Load(), PulledIn: fd.pulledIn.Load(), PullsSent: fd.pullsSent.Load(), Timeouts: fd.timeouts.Load()}
		if fd.lrb != nil {
			fs.LRBSlot = fd.lrb.Slot()
			fs.LRBAge = time.Since(fd.lrbAt).Round(time.Second).String()
		}
		return fs
	}
	for _, fd := range n.feeders {
		fs := status(fd)
		if fs.Alive {
			info.FeedersAlive++
		}
		info.FeederList = append(info.FeederList, fs)
	}
	for _, fd := range n.others {
		info.OtherDAGStores = append(info.OtherDAGStores, status(fd))
	}
	for _, id := range n.replicas {
		info.Replicas = append(info.Replicas, api.FeederStatus{Peer: id.String(), Alive: n.alive(id)})
	}
	info.ToReplicas = cntToReplicas.Load()
	if n.lrb != nil {
		info.LRB = n.lrb.StringHex()
		info.LRBSlot = n.lrb.Slot()
	}
	f := &n.branchFetch
	info.BranchFetch = api.BranchFetch{Started: f.started, Done: f.done, NextSlot: f.next, TargetSlot: f.target,
		FeederOldestSlot: f.feederOldest, FeederPurgeFloor: f.feederFloor, Entries: f.entriesFetched}
	if n.walkedSlot > 0 {
		walkedSlot := n.walkedSlot
		info.WalkerSlot = &walkedSlot
	}
	n.setLRBReply()
	n.mu.Lock()
	n.info = info
	n.mu.Unlock()
}

func deref(p *uint32) any {
	if p == nil {
		return "-"
	}
	return *p
}

// Status is what the API shows of the live state.
func (n *Node) Status() *api.LiveStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	info := n.info
	return &info
}

func (n *Node) logReport() {
	s := n.Status()
	var tip any = "-"
	if v, _ := n.metaTxID("canonical_tip"); v != nil {
		tip = v.Slot()
	}
	var sealed any = "-"
	if v, _ := n.metaTxID("sealed_tip"); v != nil {
		sealed = v.Slot()
	}
	slog.Info("live", "feeders_alive", s.FeedersAlive, "lrb_slot", s.LRBSlot, "canonical_tip", tip, "sealed_tip", sealed,
		"walker_slot", deref(s.WalkerSlot), "pushed", s.Pushed, "pulled", s.PulledIn, "dup", s.Duplicates, "linked", s.Linked,
		"pending_pulls", s.PendingPulls, "lost", s.Lost, "now_slot", ledger.TimeNow().Slot)
}
