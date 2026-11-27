package live

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/lunfardo314/dagstore"
	"github.com/lunfardo314/dagstore/archive"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/ledger/transaction"
	"github.com/lunfardo314/proxima/util/ingate"
)

// ImportSegment feeds an archive segment into the database through the same intake, walk and
// sealing as live sync, slot by slot, with the canonical branch the manifest gives for each slot as
// the feeders' LRB would be: the branch table, confirmations, forks and seals are built here, none
// copied. A new database starts at the segment's first canonical branch; one already holding
// history continues if the segment's lineage continues its canonical tip. Nothing is pulled: what
// the segment's source had declared lost is lost here too. The segment must be verified first.
func ImportSegment(db *sql.DB, params dagstore.Params, codec *store.Codec, m *archive.Manifest, r io.Reader) error {
	n := &Node{db: db, params: params, codec: codec, importing: true, pulls: make(map[base.TransactionID]*pullState),
		gate: ingate.New[base.TransactionID](time.Hour, 100_000, 1_000_000)}
	n.branchFetch.started, n.branchFetch.done = true, true
	if err := n.begin(); err != nil {
		return err
	}
	if err := n.restore(); err != nil {
		return err
	}
	canonical := make(map[uint32]base.TransactionID, len(m.Canonical))
	var last base.TransactionID
	for _, h := range m.Canonical {
		b, err := hex.DecodeString(h)
		if err != nil {
			return err
		}
		if last, err = base.TransactionIDFromBytes(b); err != nil {
			return err
		}
		canonical[last.Slot()] = last
	}
	if len(canonical) == 0 {
		return fmt.Errorf("segment %s has no canonical branch", m.File)
	}
	t0 := time.Now()
	slot := m.FromSlot
	endSlot := func() error {
		if c, ok := canonical[slot]; ok {
			n.lrb = &c
			if err := n.updateCanonical(); err != nil {
				return err
			}
		}
		if err := n.settle(10); err != nil {
			return err
		}
		if slot%500 == 0 {
			slog.Info("import", "slot", slot, "to_slot", m.ToSlot, "walked_slot", n.walkedSlot)
		}
		slot++
		return nil
	}
	var count int64
	err := archive.Records(r, func(raw []byte) error {
		tx, err := transaction.Parse(raw)
		if err != nil {
			return err
		}
		txid := tx.ID()
		for slot < txid.Slot() {
			if err = endSlot(); err != nil {
				return err
			}
		}
		count++
		return n.intake(received{tx: tx, txid: txid, raw: raw})
	})
	if err != nil {
		return err
	}
	for slot <= m.ToSlot {
		if err = endSlot(); err != nil {
			return err
		}
	}
	for _, h := range m.Lost {
		b, err := hex.DecodeString(h)
		if err != nil {
			return err
		}
		if _, err = n.exec(`UPDATE tx SET state = ? WHERE txid = ? AND state = ?`, store.StateLost, b, store.StateMissing); err != nil {
			return err
		}
	}
	if err = n.settle(1000); err != nil {
		return err
	}
	if err = n.purgeOrphans(); err != nil {
		return err
	}
	tip, err := n.metaTxID("canonical_tip")
	if err != nil {
		return err
	}
	if err = n.tx.Commit(); err != nil {
		return err
	}
	if tip == nil || *tip != last {
		return fmt.Errorf("after the import the canonical tip is not the segment's last canonical branch: does the segment continue the database's lineage?")
	}
	slog.Info("segment imported", "file", m.File, "transactions", count, "canonical_tip_slot", tip.Slot(),
		"took", time.Since(t0).Round(time.Second))
	return nil
}

// settle walks and seals, committing after each round, until a round moves neither or after
// rounds rounds.
func (n *Node) settle(rounds int) error {
	for i := 0; i < rounds; i++ {
		walked, sealed := n.walkedNo, n.sealedCount()
		if err := n.walkStep(); err != nil {
			return err
		}
		if err := n.sealStep(); err != nil {
			return err
		}
		if err := n.codec.TrainIfDue(n.tx); err != nil {
			return err
		}
		if err := n.tx.Commit(); err != nil {
			return err
		}
		if err := n.begin(); err != nil {
			return err
		}
		if n.walkedNo == walked && n.sealedCount() == sealed && !n.unwalkedBelowWalk() {
			return nil
		}
	}
	return nil
}

func (n *Node) sealedCount() (c int64) {
	_ = n.queryRow(`SELECT count(*) FROM branch WHERE sealed = 1`).Scan(&c)
	return
}

func (n *Node) unwalkedBelowWalk() bool {
	var one int
	return n.queryRow(`SELECT 1 FROM branch WHERE walked = 0 AND canonical_no IS NULL AND slot <= ? LIMIT 1`, n.walkedSlot).Scan(&one) == nil
}
