package live

import (
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lunfardo314/dagstore"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/ledger/transaction"
	"github.com/lunfardo314/proxima/util/ingate"
	"github.com/lunfardo314/unitrie/adaptors/badger_adaptor"
)

// TestReplay checks the live intake, walk and sealing on real history, with no network: it feeds a
// slot range of a node's txstore through intake in txid (time) order, takes the heaviest branch of
// each slot as the feeders' LRB, and compares the result with a database the offline importer
// made from the same txstore. Away from the ends of the range, where the two start and stop
// differently, both must agree on the canonical lineage, on what each canonical branch confirmed
// and on what is kept. It needs real data, so it runs only when given:
//
//	DAGSTORE_REPLAY_TXSTORE  a txstore directory (opened read-only)
//	DAGSTORE_REPLAY_IMPORTED the importer's database of that txstore, also the source of the libraries
//	DAGSTORE_REPLAY_FROM, DAGSTORE_REPLAY_SLOTS  the slot range (default 225000, 500)
//	DAGSTORE_REPLAY_DELAY_EVERY  if set to N, every N-th transaction (branches included) is held back
//	                             and delivered DAGSTORE_REPLAY_DELAY_SLOTS (default 30) slots late, as
//	                             a pull answered late would be. Above the canonical depth (120) the
//	                             walks stop at it and must resume, with the same result
func TestReplay(t *testing.T) {
	txstorePath, importedPath := os.Getenv("DAGSTORE_REPLAY_TXSTORE"), os.Getenv("DAGSTORE_REPLAY_IMPORTED")
	if txstorePath == "" || importedPath == "" {
		t.Skip("DAGSTORE_REPLAY_TXSTORE and DAGSTORE_REPLAY_IMPORTED not set")
	}
	from, slots := envUint(t, "DAGSTORE_REPLAY_FROM", 225000), envUint(t, "DAGSTORE_REPLAY_SLOTS", 500)
	delayEvery := envUint(t, "DAGSTORE_REPLAY_DELAY_EVERY", 0)
	delaySlots := envUint(t, "DAGSTORE_REPLAY_DELAY_SLOTS", 30)
	held := make(map[uint32][]*transaction.Transaction) // slot of delivery -> held transactions

	imported, err := store.OpenReadOnly(importedPath, 2)
	must(t, err)
	defer imported.Close()
	libs, err := store.LoadLibraries(imported)
	must(t, err)
	m := make(map[uint32][]byte)
	for _, l := range libs {
		m[l.ActivationSlot] = l.JSON
	}
	ledger.MustInitLibraryCacheFromMap(m)

	txDB, err := badger.Open(badger.DefaultOptions(txstorePath).WithReadOnly(true).WithLogger(nil))
	must(t, err)
	defer txDB.Close()
	src := badger_adaptor.New(txDB)

	db, err := store.Create(filepath.Join(t.TempDir(), "replay.db"))
	must(t, err)
	defer db.Close()
	n := &Node{db: db, params: dagstore.DefaultParams, pulls: make(map[base.TransactionID]*pullState),
		gate: ingate.New[base.TransactionID](time.Hour, 100_000, 1_000_000)}
	n.branchFetch.started, n.branchFetch.done = true, true
	must(t, n.begin())

	t0 := time.Now()
	var fed, delayed int
	for s := from; s < from+slots+delaySlots; s++ {
		for _, tx := range held[s] {
			must(t, n.intake(received{tx: tx}))
		}
		delete(held, s)
		var heaviest *base.TransactionID
		var heaviestCov uint64
		src.Iterator(base.Slot2Bytes(s)).Iterate(func(_, v []byte) bool {
			tx, err := transaction.Parse(v)
			if err != nil || s >= from+slots {
				return err == nil && s < from+slots
			}
			fed++
			if delayEvery > 0 && fed%int(delayEvery) == 0 {
				held[s+delaySlots] = append(held[s+delaySlots], tx)
				delayed++
				if pred, ok := tx.StemPredecessor(); ok {
					must(t, n.claimBranch(tx.ID(), pred)) // as the feeders' branch lists would
				}
			} else {
				must(t, n.intake(received{tx: tx}))
			}
			if stem := store.Stem(tx); stem != nil && stem.TotalCoverage > heaviestCov {
				id := tx.ID()
				heaviest, heaviestCov = &id, stem.TotalCoverage
			}
			return true
		})
		if heaviest != nil {
			n.lrb = heaviest
			must(t, n.updateCanonical())
		}
		for i := 0; i < 20; i++ {
			must(t, n.walkStep())
		}
		must(t, n.sealStep())
		must(t, n.tx.Commit())
		must(t, n.begin())
	}
	var intakeBefore, intakeAfter int64
	must(t, n.queryRow(`SELECT count(*) FROM tx WHERE state = ?`, store.StateIntake).Scan(&intakeBefore))
	must(t, n.purgeOrphans())
	must(t, n.queryRow(`SELECT count(*) FROM tx WHERE state = ?`, store.StateIntake).Scan(&intakeAfter))
	t.Logf("orphan purge: intake %d -> %d", intakeBefore, intakeAfter)
	must(t, n.tx.Commit())
	// link consistency: every edge points to a row, whatever was purged
	for _, q := range []string{
		`SELECT count(*) FROM input i LEFT JOIN tx t ON t.id = i.producer WHERE t.id IS NULL`,
		`SELECT count(*) FROM endorsement e LEFT JOIN tx t ON t.id = e.endorsed WHERE t.id IS NULL`,
		`SELECT count(*) FROM tx x LEFT JOIN tx t ON t.id = x.baseline WHERE x.baseline IS NOT NULL AND t.id IS NULL`,
	} {
		var dangling int64
		must(t, db.QueryRow(q).Scan(&dangling))
		if dangling != 0 {
			t.Errorf("%d dangling edges: %s", dangling, q)
		}
	}
	t.Logf("%d of them delayed by %d slots", delayed, delaySlots)
	t.Logf("fed %d transactions over %d slots in %v (%.0f tx/s)", fed, slots, time.Since(t0).Round(time.Second), float64(fed)/time.Since(t0).Seconds())

	var startSlot, tipSlot uint32
	must(t, db.QueryRow(`SELECT min(slot), max(slot) FROM branch WHERE canonical_no IS NOT NULL`).Scan(&startSlot, &tipSlot))
	var walkedTo uint32
	must(t, db.QueryRow(`SELECT coalesce(max(slot), 0) FROM branch WHERE walked = 1 AND canonical_no IS NOT NULL`).Scan(&walkedTo))
	var sealedCanon, canon, pending, lost int64
	must(t, db.QueryRow(`SELECT count(*), coalesce(sum(sealed), 0) FROM branch WHERE canonical_no IS NOT NULL`).Scan(&canon, &sealedCanon))
	must(t, db.QueryRow(`SELECT coalesce(sum(state = ?), 0), coalesce(sum(state = ?), 0) FROM tx`, store.StateMissing, store.StateLost).Scan(&pending, &lost))
	t.Logf("canonical lineage slots %d..%d (%d branches, %d sealed), walked to %d, missing %d, lost %d",
		startSlot, tipSlot, canon, sealedCanon, walkedTo, pending, lost)

	// compare inside the range, a margin away from both ends
	lo, hi := startSlot+100, walkedTo-100
	if hi <= lo {
		t.Fatalf("range too short to compare: walked %d..%d", startSlot, walkedTo)
	}
	got, want := canonicalLineage(t, db, lo, hi), canonicalLineage(t, imported, lo, hi)
	if len(got) != len(want) {
		t.Fatalf("canonical branches in %d..%d: %d, imported %d", lo, hi, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("canonical lineage differs at %d", i)
		}
	}
	gotConf, wantConf := confirmedPerBranch(t, db, lo, hi), confirmedPerBranch(t, imported, lo, hi)
	diff := 0
	for k, v := range wantConf {
		if gotConf[k] != v {
			diff++
			if diff <= 5 {
				t.Errorf("branch %x: confirmed %d, imported %d", k[:6], gotConf[k], v)
			}
		}
	}
	gotKept, wantKept := keptInSlots(t, db, lo, hi), keptInSlots(t, imported, lo, hi)
	t.Logf("slots %d..%d: %d canonical branches, %d differ in confirmations; kept %d, imported %d", lo, hi, len(got), diff, gotKept, wantKept)
	if gotKept != wantKept {
		t.Errorf("kept transactions in slots %d..%d: %d, imported %d", lo, hi, gotKept, wantKept)
	}
}

func canonicalLineage(t *testing.T, db *sql.DB, lo, hi uint32) []string {
	rows, err := db.Query(`SELECT t.txid FROM branch b JOIN tx t ON t.id = b.tx WHERE b.canonical_no IS NOT NULL AND b.slot BETWEEN ? AND ? ORDER BY b.slot`, lo, hi)
	must(t, err)
	defer rows.Close()
	var ret []string
	for rows.Next() {
		var id []byte
		must(t, rows.Scan(&id))
		ret = append(ret, string(id))
	}
	return ret
}

func confirmedPerBranch(t *testing.T, db *sql.DB, lo, hi uint32) map[string]int64 {
	rows, err := db.Query(`SELECT c.txid, count(*) FROM tx x JOIN tx c ON c.id = x.confirming_branch WHERE c.slot BETWEEN ? AND ? GROUP BY c.txid`, lo, hi)
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

func keptInSlots(t *testing.T, db *sql.DB, lo, hi uint32) int64 {
	var n int64
	must(t, db.QueryRow(`SELECT count(*) FROM tx WHERE txid >= ? AND txid < ? AND state = ?`,
		base.Slot2Bytes(lo), base.Slot2Bytes(hi+1), store.StateLinked).Scan(&n))
	return n
}

func envUint(t *testing.T, key string, def uint32) uint32 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseUint(v, 10, 32)
	must(t, err)
	return uint32(n)
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
