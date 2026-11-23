package live

import (
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/ledger/transaction"
)

// branchesPerStep bounds the walk's work per commit, so intake is never held up long.
const branchesPerStep = 50

// walkStep walks the next branches in slot order (SPEC.md section 6.3): first every non-canonical
// branch of a slot already passed by the canonical walk, then the next canonical branch.
func (n *Node) walkStep() error {
	for i := 0; i < branchesPerStep; i++ {
		var bid int64
		var slot uint32
		canonical := false
		err := n.queryRow(`SELECT tx, slot FROM branch WHERE walked = 0 AND canonical_no IS NULL AND slot <= ? ORDER BY slot LIMIT 1`, n.walkedSlot).Scan(&bid, &slot)
		if errors.Is(err, sql.ErrNoRows) {
			err = n.queryRow(`SELECT tx, slot FROM branch WHERE canonical_no = ?`, n.walkedNo+1).Scan(&bid, &slot)
			canonical = true
		}
		if errors.Is(err, sql.ErrNoRows) {
			return nil // nothing to walk until the canonical tip moves
		}
		if err != nil {
			return err
		}
		if !canonical {
			if err = n.assignFork(bid); err != nil {
				return err
			}
		}
		if err = n.walk(bid, canonical, []int64{bid}); err != nil {
			return err
		}
		if _, err = n.exec(`UPDATE branch SET walked = 1 WHERE tx = ?`, bid); err != nil {
			return err
		}
		if canonical {
			n.walkedNo, n.walkedSlot = n.walkedNo+1, slot
		}
	}
	return nil
}

// walk goes through the cone of branch bid from the given transactions, linking what is in intake
// and stopping at what the walk need not cross again: anything already linked, or, for a canonical
// branch, anything already confirmed by it or an earlier canonical branch. A transaction not
// stored stops the walk there; the branch waits for it, and the walk resumes from it when it
// arrives. Meanwhile a later canonical branch may have reached part of the cone by other paths:
// the resumed walk takes those over, since a transaction is confirmed by the first canonical
// branch whose cone holds it. A later branch cannot be sealed before this one, so nothing sealed
// changes.
func (n *Node) walk(bid int64, canonical bool, from []int64) error {
	var bNo int64
	if canonical {
		if err := n.queryRow(`SELECT canonical_no FROM branch WHERE tx = ?`, bid).Scan(&bNo); err != nil {
			return err
		}
	}
	stack := append([]int64(nil), from...)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		var (
			txid         []byte
			slot         uint32
			state        int
			confirmingNo sql.NullInt64
		)
		if err := n.queryRow(`SELECT t.txid, t.slot, t.state, cb.canonical_no FROM tx t LEFT JOIN branch cb ON cb.tx = t.confirming_branch
			WHERE t.id = ?`, id).Scan(&txid, &slot, &state, &confirmingNo); err != nil {
			return err
		}
		switch state {
		case store.StatePruned:
			continue
		case store.StateMissing, store.StateLost:
			if n.startSlot != 0 && slot < n.startSlot {
				if _, err := n.exec(`UPDATE tx SET state = ? WHERE id = ?`, store.StatePruned, id); err != nil {
					return err
				}
				continue
			}
			if err := n.wait(id, bid, txid, state); err != nil {
				return err
			}
			continue
		case store.StateIntake:
			deps, ok, err := n.link(id, bid, canonical)
			if err != nil {
				return err
			}
			if !ok {
				if err = n.wait(id, bid, txid, store.StateMissing); err != nil {
					return err
				}
				continue
			}
			stack = append(stack, deps...)
		case store.StateLinked:
			if !canonical || (confirmingNo.Valid && confirmingNo.Int64 <= bNo) {
				continue
			}
			if _, err := n.exec(`UPDATE tx SET confirming_branch = ? WHERE id = ?`, bid, id); err != nil {
				return err
			}
			deps, err := n.deps(id)
			if err != nil {
				return err
			}
			stack = append(stack, deps...)
		}
	}
	return nil
}

// wait records that branch bid waits for the transaction, and pulls it if it is still pending.
func (n *Node) wait(id, bid int64, txid []byte, state int) error {
	if _, err := n.exec(`INSERT OR IGNORE INTO waiting(tx, branch) VALUES (?, ?)`, id, bid); err != nil {
		return err
	}
	if state != store.StateMissing {
		return nil
	}
	if _, err := n.exec(`INSERT OR IGNORE INTO missing(tx, referenced_by, first_needed) VALUES (?, ?, ?)`, id, bid, time.Now().Unix()); err != nil {
		return err
	}
	t, err := base.TransactionIDFromBytes(txid)
	if err != nil {
		return err
	}
	n.wantPull(t)
	return nil
}

// resumeWaiting continues, from a transaction that has just arrived, the walks that stopped at it:
// oldest branch first and, in one slot, the canonical one first, so that each transaction gets the
// confirming branch it would have got had it been there in time.
func (n *Node) resumeWaiting(id int64) error {
	rows, err := n.query(`SELECT w.branch, b.canonical_no IS NOT NULL FROM waiting w JOIN branch b ON b.tx = w.branch
		WHERE w.tx = ? ORDER BY b.slot, b.canonical_no IS NULL`, id)
	if err != nil {
		return err
	}
	type waiter struct {
		branch    int64
		canonical bool
	}
	var ws []waiter
	for rows.Next() {
		var w waiter
		if err = rows.Scan(&w.branch, &w.canonical); err != nil {
			rows.Close()
			return err
		}
		ws = append(ws, w)
	}
	rows.Close()
	if len(ws) == 0 {
		return nil
	}
	if _, err = n.exec(`DELETE FROM waiting WHERE tx = ?`, id); err != nil {
		return err
	}
	for _, w := range ws {
		if err = n.walk(w.branch, w.canonical, []int64{id}); err != nil {
			return err
		}
	}
	return nil
}

// deps are the transactions a linked one depends on: its inputs' producers, its endorsements and
// its explicit baseline.
func (n *Node) deps(id int64) ([]int64, error) {
	rows, err := n.query(`SELECT producer FROM input WHERE tx = ?1 UNION SELECT endorsed FROM endorsement WHERE tx = ?1
		UNION SELECT baseline FROM tx WHERE id = ?1 AND baseline IS NOT NULL`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ret []int64
	for rows.Next() {
		var d int64
		if err = rows.Scan(&d); err != nil {
			return nil, err
		}
		ret = append(ret, d)
	}
	return ret, rows.Err()
}

// link parses a transaction in intake, checks its signature and writes its edges and outputs. A
// transaction failing the check loses its bytes and is pulled again (SPEC.md section 6.4): ok is
// false then. It returns the transactions the walk continues to.
func (n *Node) link(id, bid int64, canonical bool) (deps []int64, ok bool, err error) {
	var raw []byte
	if err = n.queryRow(`SELECT bytes FROM tx_bytes WHERE id = ?`, id).Scan(&raw); err != nil {
		return nil, false, err
	}
	tx, err := transaction.Parse(raw)
	if err == nil {
		_, err = tx.VerifySignature()
	}
	if err != nil {
		slog.Warn("stored bytes refused, pulling again", "tx", id, "err", err)
		cntSigFailed.Add(1)
		if _, err = n.exec(`DELETE FROM tx_bytes WHERE id = ?`, id); err != nil {
			return nil, false, err
		}
		_, err = n.exec(`UPDATE tx SET state = ? WHERE id = ?`, store.StateMissing, id)
		return nil, false, err
	}
	seen := make(map[int64]bool)
	add := func(txid base.TransactionID) (int64, error) {
		d, _, err := n.rowOf(txid)
		if err == nil && !seen[d] {
			seen[d] = true
			deps = append(deps, d)
		}
		return d, err
	}
	tx.ForEachInputID(func(i byte, oid base.OutputID) bool {
		var p int64
		if p, err = add(oid.TransactionID()); err == nil {
			_, err = n.exec(`INSERT OR IGNORE INTO input(tx, idx, producer, out_idx) VALUES (?,?,?,?)`, id, i, p, oid.Index())
		}
		return err == nil
	})
	if err != nil {
		return nil, false, err
	}
	tx.ForEachEndorsement(func(i byte, e base.TransactionID) bool {
		var d int64
		if d, err = add(e); err == nil {
			_, err = n.exec(`INSERT OR IGNORE INTO endorsement(tx, idx, endorsed) VALUES (?,?,?)`, id, i, d)
		}
		return err == nil
	})
	if err != nil {
		return nil, false, err
	}
	var baseline any
	if bl, has := tx.ExplicitBaseline(); has {
		if baseline, err = add(bl); err != nil {
			return nil, false, err
		}
	}
	tx.ForEachProducedOutput(func(i byte, o *ledger.Output, oid base.OutputID) bool {
		err = n.writeOutput(id, i, o, oid)
		return err == nil
	})
	if err != nil {
		return nil, false, err
	}
	var confirming any
	if canonical {
		confirming = bid
	}
	_, err = n.exec(`UPDATE tx SET state = ?, baseline = ?, confirming_branch = ?, sig_checked = 1, parsed = 1 WHERE id = ?`,
		store.StateLinked, baseline, confirming, id)
	cntLinked.Add(1)
	return deps, err == nil, err
}

func (n *Node) writeOutput(id int64, i byte, o *ledger.Output, oid base.OutputID) error {
	f := store.OutputFactsOf(o, oid)
	var lockKind, account, chain any
	var err error
	if f.Lock != "" {
		if lockKind, err = n.dictRow("lock_kind", "name", f.Lock); err != nil {
			return err
		}
	}
	if f.Account != nil {
		if account, err = n.dictRow("account", "holder", f.Account); err != nil {
			return err
		}
	}
	if f.ChainID != nil {
		if chain, err = n.chainRow(*f.ChainID, f.ChainKind); err != nil {
			return err
		}
	}
	_, err = n.exec(`INSERT OR IGNORE INTO output(tx, idx, amount, lock_kind, account, chain) VALUES (?,?,?,?,?,?)`,
		id, i, int64(o.TokenBalance()), lockKind, account, chain)
	return err
}

// assignFork puts a non-canonical branch into a fork (SPEC.md section 6.6): it extends the fork
// whose tip is its predecessor, or starts a new one at the canonical branch it leaves from.
func (n *Node) assignFork(bid int64) error {
	var pred sql.NullInt64
	if err := n.queryRow(`SELECT predecessor FROM branch WHERE tx = ?`, bid).Scan(&pred); err != nil {
		return err
	}
	var predNo, predFork sql.NullInt64
	err := n.queryRow(`SELECT canonical_no, fork FROM branch WHERE tx = ?`, pred.Int64).Scan(&predNo, &predFork)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if predFork.Valid {
		var tip int64
		if err = n.queryRow(`SELECT tip FROM fork WHERE id = ?`, predFork.Int64).Scan(&tip); err != nil {
			return err
		}
		if tip == pred.Int64 {
			if _, err = n.exec(`UPDATE fork SET tip = ?, length = length + 1 WHERE id = ?`, bid, predFork.Int64); err != nil {
				return err
			}
			_, err = n.exec(`UPDATE branch SET fork = ? WHERE tx = ?`, predFork.Int64, bid)
			return err
		}
	}
	// a new fork: from the canonical predecessor, or branching off another fork below its tip
	forkPoint, length := pred, int64(1)
	if !predNo.Valid && predFork.Valid {
		var fp sql.NullInt64
		if err = n.queryRow(`SELECT fork_point FROM fork WHERE id = ?`, predFork.Int64).Scan(&fp); err != nil {
			return err
		}
		forkPoint = fp
		for cur := pred.Int64; ; length++ {
			var p, no sql.NullInt64
			if err = n.queryRow(`SELECT predecessor, canonical_no FROM branch WHERE tx = ?`, cur).Scan(&p, &no); err != nil || no.Valid || !p.Valid {
				break
			}
			cur = p.Int64
		}
	} else if !predNo.Valid {
		forkPoint = sql.NullInt64{} // the predecessor is outside the branch table
	}
	var fp any
	if forkPoint.Valid {
		fp = forkPoint.Int64
	}
	res, err := n.exec(`INSERT INTO fork(tip, fork_point, length) VALUES (?, ?, ?)`, bid, fp, length)
	if err != nil {
		return err
	}
	fid, err := res.LastInsertId()
	if err != nil {
		return err
	}
	_, err = n.exec(`UPDATE branch SET fork = ? WHERE tx = ?`, fid, bid)
	return err
}

// sealStep seals, in slot order, every walked branch whose predecessor is sealed (or which is the
// starting branch) and which waits for nothing still pending; what it waits for that was declared
// lost counts as its gaps (SPEC.md section 6.4).
func (n *Node) sealStep() error {
	startID := int64(-1)
	if v, err := n.metaTxID("starting_branch"); err != nil {
		return err
	} else if v != nil {
		if startID, _, err = n.lookup(*v); err != nil {
			return err
		}
	}
	rows, err := n.query(`SELECT b.tx, b.predecessor, b.canonical_no IS NOT NULL, b.fork FROM branch b
		WHERE b.sealed = 0 AND b.walked = 1 ORDER BY b.slot LIMIT 500`)
	if err != nil {
		return err
	}
	type cand struct {
		tx        int64
		pred      sql.NullInt64
		canonical bool
		fork      sql.NullInt64
	}
	var cands []cand
	for rows.Next() {
		var c cand
		if err = rows.Scan(&c.tx, &c.pred, &c.canonical, &c.fork); err != nil {
			rows.Close()
			return err
		}
		cands = append(cands, c)
	}
	rows.Close()
	for _, c := range cands {
		var pending, gaps int64
		if err = n.queryRow(`SELECT coalesce(sum(t.state = ?), 0), count(*) FROM waiting w JOIN tx t ON t.id = w.tx WHERE w.branch = ?`,
			store.StateMissing, c.tx).Scan(&pending, &gaps); err != nil {
			return err
		}
		if pending > 0 {
			continue
		}
		if c.tx != startID {
			var predSealed bool
			if err = n.queryRow(`SELECT sealed FROM branch WHERE tx = ?`, c.pred.Int64).Scan(&predSealed); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if !predSealed {
				continue
			}
		}
		if _, err = n.exec(`UPDATE branch SET sealed = 1, gaps = ? WHERE tx = ?`, gaps, c.tx); err != nil {
			return err
		}
		if c.canonical {
			var txid []byte
			if err = n.queryRow(`SELECT txid FROM tx WHERE id = ?`, c.tx).Scan(&txid); err != nil {
				return err
			}
			if err = n.setMeta("sealed_tip", txid); err != nil {
				return err
			}
		} else if c.fork.Valid {
			if _, err = n.exec(`UPDATE fork SET sealed_tip = ? WHERE id = ?`, c.tx, c.fork.Int64); err != nil {
				return err
			}
		}
	}
	return nil
}

// purgeOrphans deletes the transactions still in intake once the canonical tip is OrphanPurgeSlots
// past their slot (SPEC.md section 6.5): every branch up to the tip has been walked, so what no
// walk linked is in no stored branch's cone. Intake rows have no edges or outputs; branches are
// never purged, so a branch in intake stays.
func (n *Node) purgeOrphans() error {
	tip, err := n.metaTxID("canonical_tip")
	if err != nil || tip == nil || tip.Slot() < uint32(n.params.OrphanPurgeSlots) {
		return err
	}
	floor := base.Slot2Bytes(tip.Slot() - uint32(n.params.OrphanPurgeSlots))
	if _, err = n.exec(`DELETE FROM tx_bytes WHERE id IN (SELECT id FROM tx WHERE txid < ? AND state = ? AND kind != ?)`,
		floor, store.StateIntake, store.KindBranch); err != nil {
		return err
	}
	res, err := n.exec(`DELETE FROM tx WHERE txid < ? AND state = ? AND kind != ?`, floor, store.StateIntake, store.KindBranch)
	if err != nil {
		return err
	}
	if k, _ := res.RowsAffected(); k > 0 {
		cntPurged.Add(k)
	}
	return nil
}
