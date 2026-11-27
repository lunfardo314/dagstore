package archive

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger/base"
)

// Segment is an archive segment the database was pruned to, as kept in meta: what a pruned
// transaction of its slots resolves to.
type Segment struct {
	FromSlot uint32 `json:"from_slot"`
	ToSlot   uint32 `json:"to_slot"`
	File     string `json:"file"`
	TxIDHash string `json:"txid_hash"`
}

// Prune deletes the segment's range from the database, which must hold the exclusive lock (no
// other process has it open). Before deleting anything it verifies the segment and checks that it
// holds exactly the transactions the database stores in the range, that the range begins at the
// starting branch and is final (CheckSealed). Then, in one SQL transaction:
//
//   - the bytes, edges, outputs and gap records of the range are deleted, with its branches and the
//     forks ending in it;
//   - a transaction of the range still referenced from above (an input, endorsement, baseline,
//     branch predecessor or fork row) stays as a pruned placeholder, so that every edge still
//     points to a row; the others are deleted;
//   - the oldest canonical branch above the range becomes the starting branch, and the segment is
//     recorded.
//
// Free pages are then returned to the file system by an incremental vacuum.
func Prune(db *sql.DB, manifestPath string) error {
	m, err := Verify(manifestPath)
	if err != nil {
		return err
	}
	var genesis []byte
	if err = db.QueryRow(`SELECT hash FROM library WHERE activation_slot = 0`).Scan(&genesis); err != nil {
		return err
	}
	if hex.EncodeToString(genesis) != m.GenesisLibraryHash {
		return fmt.Errorf("segment of another ledger: genesis library %s, the database's %x", m.GenesisLibraryHash, genesis)
	}
	var startSlot uint32
	if err = db.QueryRow(`SELECT t.slot FROM meta m JOIN tx t ON t.txid = m.value WHERE m.key = 'starting_branch'`).Scan(&startSlot); err != nil {
		return fmt.Errorf("starting branch: %w", err)
	}
	if m.FromSlot != startSlot {
		return fmt.Errorf("segment begins at slot %d, the database at its starting branch, slot %d: pruning goes from the oldest slot up", m.FromSlot, startSlot)
	}
	if err = CheckSealed(db, m.FromSlot, m.ToSlot); err != nil {
		return err
	}
	// what the database stores in the range must be exactly the segment
	h := sha256.New()
	var count int64
	rows, err := db.Query(`SELECT t.txid FROM tx t JOIN tx_bytes b ON b.id = t.id WHERE t.txid >= ? AND t.txid < ? ORDER BY t.txid`,
		slotKey(m.FromSlot), slotKey(m.ToSlot+1))
	if err != nil {
		return err
	}
	for rows.Next() {
		var txid []byte
		if err = rows.Scan(&txid); err != nil {
			rows.Close()
			return err
		}
		h.Write(txid)
		count++
	}
	rows.Close()
	if count != m.Count || hex.EncodeToString(h.Sum(nil)) != m.TxIDHash {
		return fmt.Errorf("the database stores %d transactions in slots %d..%d, not those of the segment (%d)", count, m.FromSlot, m.ToSlot, m.Count)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	hi := slotKey(m.ToSlot + 1) // the range and the placeholders below it: txid < hi
	const inRange = `SELECT id FROM tx WHERE txid < ?1`
	for _, q := range []string{
		`DELETE FROM tx_bytes WHERE id IN (` + inRange + `)`,
		`DELETE FROM input WHERE tx IN (` + inRange + `)`,
		`DELETE FROM endorsement WHERE tx IN (` + inRange + `)`,
		`DELETE FROM output WHERE tx IN (` + inRange + `)`,
		`DELETE FROM waiting WHERE tx IN (` + inRange + `) OR branch IN (` + inRange + `)`,
		`DELETE FROM missing WHERE tx IN (` + inRange + `)`,
		`DELETE FROM fork WHERE tip IN (` + inRange + `)`,
		`DELETE FROM branch WHERE tx IN (` + inRange + `)`,
		// referenced from above: baselines (no index, one scan of what stays), branch predecessors
		// and fork rows; inputs and endorsements are checked by their indexes below
		`CREATE TEMP TABLE keep (id INTEGER PRIMARY KEY)`,
		`INSERT OR IGNORE INTO keep SELECT baseline FROM tx WHERE txid >= ?1 AND baseline IS NOT NULL`,
		`INSERT OR IGNORE INTO keep SELECT predecessor FROM branch WHERE predecessor IS NOT NULL`,
		`INSERT OR IGNORE INTO keep SELECT fork_point FROM fork WHERE fork_point IS NOT NULL`,
		`INSERT OR IGNORE INTO keep SELECT sealed_tip FROM fork WHERE sealed_tip IS NOT NULL`,
	} {
		var args []any
		if strings.Contains(q, "?1") {
			args = []any{hi}
		}
		if _, err = tx.Exec(q, args...); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}
	res, err := tx.Exec(`DELETE FROM tx WHERE txid < ? AND id NOT IN (SELECT id FROM keep)
		AND NOT EXISTS (SELECT 1 FROM input WHERE producer = tx.id) AND NOT EXISTS (SELECT 1 FROM endorsement WHERE endorsed = tx.id)`, hi)
	if err != nil {
		return err
	}
	deleted, _ := res.RowsAffected()
	if res, err = tx.Exec(`UPDATE tx SET state = ?, confirming_branch = NULL WHERE txid < ?`, store.StatePruned, hi); err != nil {
		return err
	}
	kept, _ := res.RowsAffected()
	if _, err = tx.Exec(`DROP TABLE keep`); err != nil {
		return err
	}
	var start []byte
	if err = tx.QueryRow(`SELECT t.txid FROM branch b JOIN tx t ON t.id = b.tx WHERE b.slot > ? AND b.canonical_no IS NOT NULL
		ORDER BY b.canonical_no LIMIT 1`, m.ToSlot).Scan(&start); err != nil {
		return fmt.Errorf("new starting branch: %w", err)
	}
	if _, err = tx.Exec(`UPDATE meta SET value = ? WHERE key = 'starting_branch'`, start); err != nil {
		return err
	}
	segs, err := Segments(tx)
	if err != nil {
		return err
	}
	segs = append(segs, Segment{FromSlot: m.FromSlot, ToSlot: m.ToSlot, File: filepath.Base(m.File), TxIDHash: m.TxIDHash})
	data, err := json.Marshal(segs)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO meta(key, value) VALUES ('archive_segments', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, data); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	startID, err := base.TransactionIDFromBytes(start)
	if err != nil {
		return err
	}
	slog.Info("pruned", "from_slot", m.FromSlot, "to_slot", m.ToSlot, "transactions", m.Count,
		"rows_deleted", deleted, "placeholders", kept, "starting_slot", startID.Slot())
	if _, err = db.Exec(`PRAGMA incremental_vacuum`); err != nil {
		return fmt.Errorf("incremental vacuum: %w", err)
	}
	_, err = db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}

// Segments are the archive segments the database was pruned to, oldest first.
func Segments(q interface {
	QueryRow(query string, args ...any) *sql.Row
}) ([]Segment, error) {
	var data []byte
	err := q.QueryRow(`SELECT value FROM meta WHERE key = 'archive_segments'`).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var segs []Segment
	return segs, json.Unmarshal(data, &segs)
}
