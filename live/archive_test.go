package live

import (
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/lunfardo314/dagstore"
	"github.com/lunfardo314/dagstore/api"
	"github.com/lunfardo314/dagstore/archive"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/util/ingate"
)

// TestArchiveRoundTrip exports a range of real history to an archive segment, imports it into a
// new database and prunes it from the source (SPEC.md, pruning to the archive), then exports the
// next range from the pruned source and appends it to the imported database. The imported
// database must agree with the source on the canonical lineage, the confirmations of every
// canonical branch, the kept transactions and the seals, and the pruned source must keep every
// edge pointing to a row. It runs on a scratch copy of a dagstore database, which it changes:
//
//	DAGSTORE_ARCHIVE_DB   a copy of a database, for example one the offline importer made
//	DAGSTORE_ARCHIVE_SLOTS the slots of the first segment (default 1000); the second is half as long
//
// The offline importer cannot pull, so what it left pending is declared lost first and the
// branches waiting on it sealed with gaps, as the live pull ladder would have.
func TestArchiveRoundTrip(t *testing.T) {
	srcPath := os.Getenv("DAGSTORE_ARCHIVE_DB")
	if srcPath == "" {
		t.Skip("DAGSTORE_ARCHIVE_DB not set")
	}
	slots := envUint(t, "DAGSTORE_ARCHIVE_SLOTS", 1000)
	src, err := store.Open(srcPath)
	must(t, err)
	defer src.Close()
	_, err = api.LoadLedger(src)
	must(t, err)
	srcCodec, err := store.LoadCodec(src)
	must(t, err)

	sn := offlineNode(t, src, srcCodec)
	_, err = sn.exec(`UPDATE tx SET state = ? WHERE state = ?`, store.StateLost, store.StateMissing)
	must(t, err)
	must(t, sn.settle(1000))
	must(t, sn.tx.Commit())
	start := sn.startSlot

	dir := t.TempDir()
	impPath := filepath.Join(dir, "imported.db")
	imp, err := store.Create(impPath)
	must(t, err)
	defer imp.Close()
	libs, err := store.LoadLibraries(src)
	must(t, err)
	must(t, store.SaveLibraries(imp, libs))
	impCodec, err := store.LoadCodec(imp)
	must(t, err)

	segment := func(from, to uint32) *archive.Manifest {
		t0 := time.Now()
		m, err := archive.Export(src, srcCodec, from, to, dir)
		must(t, err)
		t.Logf("exported slots %d..%d: %d transactions, %d raw bytes in %d (%.3f), %v", from, to, m.Count, m.RawBytes, m.FileBytes,
			float64(m.FileBytes)/float64(m.RawBytes), time.Since(t0).Round(time.Second))
		manifest := filepath.Join(dir, archive.Name(from, to)+".json")
		_, err = archive.Verify(manifest)
		must(t, err)
		f, err := os.Open(filepath.Join(dir, m.File))
		must(t, err)
		defer f.Close()
		t0 = time.Now()
		must(t, ImportSegment(imp, dagstore.DefaultParams, impCodec, m, f))
		t.Logf("imported in %v", time.Since(t0).Round(time.Second))
		return m
	}
	compare := func(from, to uint32) {
		if a, b := canonicalLineage(t, src, from, to), canonicalLineage(t, imp, from, to); !slices.Equal(a, b) {
			t.Fatalf("canonical lineage in %d..%d: source %d branches, imported %d", from, to, len(a), len(b))
		}
		// only transactions of the range: one below it, first included by a branch in it, went to the
		// archive with the older range and is a placeholder in the pruned source
		ca, cb := confirmedFrom(t, src, from, to), confirmedFrom(t, imp, from, to)
		diff := 0
		for k, v := range ca {
			if cb[k] != v {
				diff++
			}
		}
		// what a branch beyond the segment includes late is in the segment but linked by no walk
		// of the import: compare kept transactions away from the segment's end
		ka, kb := keptInSlots(t, src, from, to-200), keptInSlots(t, imp, from, to-200)
		sa, sb := sealedIn(t, src, from, to), sealedIn(t, imp, from, to)
		t.Logf("slots %d..%d: %d canonical branches, %d differ in confirmations; kept to %d: source %d, imported %d; sealed branches source %d, imported %d",
			from, to, len(ca), diff, to-200, ka, kb, sa, sb)
		if diff != 0 || ka != kb || sa != sb {
			t.Errorf("the imported database differs from the source")
		}
	}

	m1 := segment(start, start+slots-1)
	compare(start, m1.ToSlot)

	t0 := time.Now()
	must(t, archive.Prune(src, filepath.Join(dir, archive.Name(m1.FromSlot, m1.ToSlot)+".json")))
	t.Logf("pruned in %v", time.Since(t0).Round(time.Second))
	for _, q := range []string{
		`SELECT count(*) FROM input i LEFT JOIN tx t ON t.id = i.producer WHERE t.id IS NULL`,
		`SELECT count(*) FROM endorsement e LEFT JOIN tx t ON t.id = e.endorsed WHERE t.id IS NULL`,
		`SELECT count(*) FROM tx x LEFT JOIN tx t ON t.id = x.baseline WHERE x.baseline IS NOT NULL AND t.id IS NULL`,
		`SELECT count(*) FROM branch b LEFT JOIN tx t ON t.id = b.predecessor WHERE b.predecessor IS NOT NULL AND t.id IS NULL`,
		`SELECT count(*) FROM fork f LEFT JOIN tx t ON t.id = f.fork_point WHERE f.fork_point IS NOT NULL AND t.id IS NULL`,
		`SELECT count(*) FROM tx_bytes b JOIN tx t ON t.id = b.id WHERE t.slot <= ` + itoa(m1.ToSlot),
		`SELECT count(*) FROM branch WHERE slot <= ` + itoa(m1.ToSlot),
	} {
		var n int64
		must(t, src.QueryRow(q).Scan(&n))
		if n != 0 {
			t.Errorf("after pruning, %d rows: %s", n, q)
		}
	}
	var newStart uint32
	must(t, src.QueryRow(`SELECT t.slot FROM meta m JOIN tx t ON t.txid = m.value WHERE m.key = 'starting_branch'`).Scan(&newStart))
	if newStart <= m1.ToSlot {
		t.Fatalf("starting branch at slot %d, not above the pruned range", newStart)
	}
	segs, err := archive.Segments(src)
	must(t, err)
	if len(segs) != 1 || segs[0].ToSlot != m1.ToSlot {
		t.Errorf("archive segments recorded: %+v", segs)
	}

	// the next range, from the pruned source's new starting branch, appended to the imported one
	m2 := segment(newStart, newStart+slots/2-1)
	compare(m2.FromSlot, m2.ToSlot)
}

func offlineNode(t *testing.T, db *sql.DB, codec *store.Codec) *Node {
	n := &Node{db: db, params: dagstore.DefaultParams, codec: codec, pulls: make(map[base.TransactionID]*pullState),
		gate: ingate.New[base.TransactionID](time.Hour, 1000, 10000)}
	n.branchFetch.started, n.branchFetch.done = true, true
	must(t, n.begin())
	must(t, n.restore())
	return n
}

func confirmedFrom(t *testing.T, db *sql.DB, from, to uint32) map[string]int64 {
	rows, err := db.Query(`SELECT c.txid, count(*) FROM tx x JOIN tx c ON c.id = x.confirming_branch
		WHERE c.slot BETWEEN ?1 AND ?2 AND x.slot >= ?1 GROUP BY c.txid`, from, to)
	must(t, err)
	defer rows.Close()
	ret := make(map[string]int64)
	for rows.Next() {
		var id []byte
		var cnt int64
		must(t, rows.Scan(&id, &cnt))
		ret[string(id)] = cnt
	}
	return ret
}

func sealedIn(t *testing.T, db *sql.DB, from, to uint32) (n int64) {
	must(t, db.QueryRow(`SELECT count(*) FROM branch WHERE slot BETWEEN ? AND ? AND sealed = 1`, from, to).Scan(&n))
	return
}

func itoa(v uint32) string { return strconv.FormatUint(uint64(v), 10) }
