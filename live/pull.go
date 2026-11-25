package live

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/lunfardo314/dagstore/api"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/ledger/base"
)

var cntLinked, cntSigFailed, cntPullsSent, cntLost, cntPurged atomic.Int64

// pullState is the pull ladder of one missing transaction (SPEC.md section 6.3): the feeders in
// turn, each given PullTimeout; a round is all of them, MissingRetryInterval apart, and after
// MissingRetryRounds rounds the transaction is declared lost. Broad pulls are not built.
type pullState struct {
	tried    int // feeders tried in this round; the next is pullFeeder(txid, tried)
	inFlight bool
	sentAt   time.Time
	nextAt   time.Time
	rounds   int
	lineage  bool // a branch of the LRB's lineage: the canonical tip waits for it, so it goes first
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

// pullOne sends at most one pull, the pace being set by the caller's ticker. It handles timeouts
// first, then pulls a lineage branch due if there is one, else the oldest transaction due: sealing
// advances in slot order, so the oldest needs are the ones holding it up, and a txid begins with
// its slot. A feeder that is not alive is skipped, as tried; a feeder whose budget for this second
// is spent is passed over until the next.
func (n *Node) pullOne() error {
	now := time.Now()
	var due *base.TransactionID
	var dueFeeder *feeder
	for txid, p := range n.pulls {
		if p.inFlight {
			if now.Sub(p.sentAt) < n.params.PullTimeout {
				continue
			}
			p.inFlight = false
			n.feeders[pullFeeder(txid, p.tried, len(n.feeders))].timeouts.Add(1)
			p.tried++
			p.nextAt = now
		}
		if now.Before(p.nextAt) {
			continue // not due: between rounds, nothing is tried, not even skipped
		}
		for p.tried < len(n.feeders) && !n.peers.IsAlive(n.feeders[pullFeeder(txid, p.tried, len(n.feeders))].id) {
			p.tried++
		}
		if p.tried >= len(n.feeders) {
			p.tried = 0
			p.rounds++
			if err := n.endOfRound(txid, p); err != nil {
				return err
			}
			continue
		}
		fd := n.feeders[pullFeeder(txid, p.tried, len(n.feeders))]
		if now.Sub(fd.budgetAt) < time.Second && fd.budgetUsed >= n.params.PullsPerSecondPerFeeder {
			continue
		}
		if due == nil || p.lineage && !n.pulls[*due].lineage || p.lineage == n.pulls[*due].lineage && bytes.Compare(txid[:], due[:]) < 0 {
			id := txid
			due, dueFeeder = &id, fd
		}
	}
	if due == nil {
		return nil
	}
	p := n.pulls[*due]
	n.gate.AddPulled(*due)
	if n.peers.PullTransactionFromPeer(dueFeeder.id, *due) {
		p.inFlight, p.sentAt = true, now
		if now.Sub(dueFeeder.budgetAt) >= time.Second {
			dueFeeder.budgetAt, dueFeeder.budgetUsed = now, 0
		}
		dueFeeder.budgetUsed++
		dueFeeder.pullsSent.Add(1)
		cntPullsSent.Add(1)
	}
	return nil
}

// endOfRound records a round of the ladder that found nothing, and declares the transaction lost
// after the last round. A lost record is kept forever; it no longer holds up sealing.
func (n *Node) endOfRound(txid base.TransactionID, p *pullState) error {
	id, _, err := n.lookup(txid)
	if err != nil {
		return err
	}
	if _, err = n.exec(`UPDATE missing SET rounds = ?, tried = ? WHERE tx = ?`, p.rounds, fmt.Sprintf("%d feeders x %d rounds", len(n.feeders), p.rounds), id); err != nil {
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
		Lost:            cntLost.Load(),
		Purged:          cntPurged.Load(),
		VersionMismatch: cntVersionMismatch.Load(),
		Unparsed:        cntUnparsed.Load(),
		Intake:          len(n.in),
	}
	for _, fd := range n.feeders {
		fs := api.FeederStatus{Peer: fd.id.String(), Alive: n.peers.IsAlive(fd.id), OldestSlot: fd.oldest, PurgeFloor: fd.floor,
			Pushes: fd.pushes.Load(), PulledIn: fd.pulledIn.Load(), PullsSent: fd.pullsSent.Load(), Timeouts: fd.timeouts.Load()}
		if fs.Alive {
			info.FeedersAlive++
		}
		if fd.lrb != nil {
			fs.LRBSlot = fd.lrb.Slot()
			fs.LRBAge = time.Since(fd.lrbAt).Round(time.Second).String()
		}
		info.FeederList = append(info.FeederList, fs)
	}
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
