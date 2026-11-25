package live

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/ledger/transaction"
)

// counters of the intake, written by peering's goroutines and the loop, read by the API
var (
	cntPushed, cntPulledIn, cntDuplicate, cntNotFeeder, cntBadBytes, cntStored, cntVersionMismatch, cntUnparsed atomic.Int64
)

// onTxBytes is peering's gossip callback. Only feeders are heard (SPEC.md section 5.3); copies of
// a transaction already seen are dropped on the txid prefix, which can be trusted from a feeder.
// Parsing is stage 1 only: the signature is checked when the transaction is linked.
func (n *Node) onTxBytes(from peer.ID, txBytes []byte, txid base.TransactionID) {
	if !n.peers.IsFeeder(from) {
		cntNotFeeder.Add(1)
		return
	}
	pass, pulled := n.gate.CheckPass(txid)
	if !pass {
		cntDuplicate.Add(1)
		return
	}
	tx, err := transaction.Parse(txBytes)
	if errors.Is(err, transaction.ErrTxVersionMismatch) {
		// A feeder pushes only what it accepted: the network runs a library this dagstore lacks.
		// The bytes are kept under the feeder's txid, which can be trusted from a feeder, and
		// parsed after the restart that loads the library.
		n.gate.Accept(txid)
		cntVersionMismatch.Add(1)
		n.libs.Alarm("feeders send transactions of a library this dagstore does not have, stored unparsed: " + err.Error())
		n.in <- received{from: from, txid: txid, raw: txBytes, pulled: pulled}
		return
	}
	if err != nil || tx.ID() != txid {
		n.gate.Unsee(txid)
		cntBadBytes.Add(1)
		return
	}
	n.gate.Accept(txid)
	fd := n.feederByID[from]
	if pulled {
		cntPulledIn.Add(1)
		fd.pulledIn.Add(1)
	} else {
		cntPushed.Add(1)
		fd.pushes.Add(1)
	}
	n.in <- received{from: from, tx: tx, txid: txid, raw: txBytes, pulled: pulled}
}

// onPullRequest answers a peer's pull from the database: any age, since the dagstore keeps it all.
func (n *Node) onPullRequest(from peer.ID, txid base.TransactionID) {
	var raw []byte
	if err := n.readDB().QueryRow(`SELECT b.bytes FROM tx t JOIN tx_bytes b ON b.id = t.id WHERE t.txid = ?`, txid[:]).Scan(&raw); err == nil {
		n.peers.SendTxBytesToPeer(from, raw, txid)
	}
}

// intake stores a received transaction. A transaction already stored is ignored; one that was a
// placeholder (missing, lost) gets its bytes, and the walks waiting for it resume.
func (n *Node) intake(r received) error {
	txid := r.txid
	if r.tx != nil {
		txid = r.tx.ID()
	}
	delete(n.pulls, txid)
	id, state, err := n.lookup(txid)
	if err != nil {
		return err
	}
	source := store.SourcePush
	if r.pulled {
		source = store.SourcePull
	}
	switch {
	case id == 0:
		if id, err = n.insertTx(txid, store.StateIntake); err != nil {
			return err
		}
	case state == store.StateMissing || state == store.StateLost:
		if _, err = n.exec(`DELETE FROM missing WHERE tx = ?`, id); err != nil {
			return err
		}
	default:
		return nil
	}
	raw := r.raw
	if r.tx != nil {
		raw = r.tx.Bytes()
	}
	if _, err = n.exec(`UPDATE tx SET size = ?, state = ?, parsed = 0, source = ?, received_at = ? WHERE id = ?`,
		len(raw), store.StateIntake, source, time.Now().Unix(), id); err != nil {
		return err
	}
	if _, err = n.exec(`INSERT OR REPLACE INTO tx_bytes(id, bytes) VALUES (?, ?)`, id, raw); err != nil {
		return err
	}
	cntStored.Add(1)
	if r.tx == nil {
		cntUnparsed.Add(1)
		return nil
	}
	return n.parsed(id, r.tx)
}

// parsed records what parsing tells of a stored transaction: its kind and sequencer, its branch row
// for a branch, and then resumes the walks that waited for it.
func (n *Node) parsed(id int64, tx *transaction.Transaction) error {
	var seq any
	var err error
	if chainID, ok := store.SequencerID(tx); ok {
		if seq, err = n.chainRow(chainID, store.ChainSequencer); err != nil {
			return err
		}
	}
	if _, err = n.exec(`UPDATE tx SET kind = ?, sequencer = ?, parsed = 1 WHERE id = ?`, store.TxKind(tx), seq, id); err != nil {
		return err
	}
	if tx.IsBranchTransaction() {
		if err = n.branchArrived(id, tx); err != nil {
			return err
		}
	}
	return n.resumeWaiting(id)
}

// parsePending parses, at start, what was stored unparsed: the libraries loaded now may include the
// one it needs. A transaction still of a library not held stays unparsed; one whose bytes do not
// parse or do not hash to the feeder's txid is deleted, and pulled again if a walk needs it.
func (n *Node) parsePending() error {
	rows, err := n.query(`SELECT t.id, t.txid, b.bytes FROM tx t JOIN tx_bytes b ON b.id = t.id WHERE t.state = ? AND t.parsed = 0`, store.StateIntake)
	if err != nil {
		return err
	}
	type pending struct {
		id   int64
		txid []byte
		raw  []byte
	}
	var ps []pending
	for rows.Next() {
		var p pending
		if err = rows.Scan(&p.id, &p.txid, &p.raw); err != nil {
			rows.Close()
			return err
		}
		ps = append(ps, p)
	}
	rows.Close()
	var done, still, bad int
	for _, p := range ps {
		tx, err := transaction.Parse(p.raw)
		switch {
		case errors.Is(err, transaction.ErrTxVersionMismatch):
			still++
			continue
		case err != nil || !txidIs(tx, p.txid):
			bad++
			if _, err = n.exec(`DELETE FROM tx_bytes WHERE id = ?`, p.id); err != nil {
				return err
			}
			if _, err = n.exec(`UPDATE tx SET state = ? WHERE id = ?`, store.StateMissing, p.id); err != nil {
				return err
			}
			continue
		}
		if err = n.parsed(p.id, tx); err != nil {
			return err
		}
		done++
	}
	if len(ps) > 0 {
		slog.Info("unparsed transactions parsed", "parsed", done, "still_unparsed", still, "bad", bad)
	}
	if still > 0 {
		n.libs.Alarm(fmt.Sprintf("%d stored transactions are of a library this dagstore does not have", still))
	}
	return nil
}

// lookup returns the row id and state of txid, id 0 if it has no row.
func (n *Node) lookup(txid base.TransactionID) (id int64, state int, err error) {
	err = n.queryRow(`SELECT id, state FROM tx WHERE txid = ?`, txid[:]).Scan(&id, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil
	}
	return
}

// insertTx makes the row of a transaction, with no bytes yet.
func (n *Node) insertTx(txid base.TransactionID, state int) (int64, error) {
	res, err := n.exec(`INSERT INTO tx(txid, slot, tick, state) VALUES (?, ?, ?, ?)`, txid[:], txid.Slot(), txid.Tick(), state)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// rowOf returns the id of txid's row, making a placeholder if it has none: pruned below the
// starting branch, missing otherwise.
func (n *Node) rowOf(txid base.TransactionID) (id int64, state int, err error) {
	if id, state, err = n.lookup(txid); err != nil || id != 0 {
		return
	}
	state = store.StateMissing
	if n.startSlot != 0 && txid.Slot() < n.startSlot {
		state = store.StatePruned
	}
	id, err = n.insertTx(txid, state)
	return
}

func (n *Node) chainRow(chainID base.ChainID, kind int) (int64, error) {
	var id int64
	err := n.queryRow(`SELECT id FROM chain WHERE chain_id = ?`, chainID[:]).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	res, err := n.exec(`INSERT INTO chain(chain_id, kind) VALUES (?, ?)`, chainID[:], kind)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (n *Node) dictRow(table, column string, value any) (int64, error) {
	var id int64
	err := n.queryRow(`SELECT id FROM `+table+` WHERE `+column+` = ?`, value).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	res, err := n.exec(`INSERT INTO `+table+`(`+column+`) VALUES (?)`, value)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func txidIs(tx *transaction.Transaction, txid []byte) bool {
	id := tx.ID()
	return bytes.Equal(id[:], txid)
}
