package live

import (
	"database/sql"
	"errors"
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
//	DAGSTORE_REPLAY_UNPARSED_EVERY  if set to N, every N-th transaction is stored unparsed, as one of a
//	                             library not held would be: the walks reaching it must wait without
//	                             pulling and keep their branch unsealed, until it is parsed at the end
func TestReplay(t *testing.T) {
	r := newReplay(t, nil)
	defer r.close()
	delayEvery := envUint(t, "DAGSTORE_REPLAY_DELAY_EVERY", 0)
	delaySlots := envUint(t, "DAGSTORE_REPLAY_DELAY_SLOTS", 30)
	unparsedEvery := envUint(t, "DAGSTORE_REPLAY_UNPARSED_EVERY", 0)
	held := make(map[uint32][]*transaction.Transaction) // slot of delivery -> held transactions
	n := r.n

	t0 := time.Now()
	var fed, delayed, unparsed int
	var lastLRB *base.TransactionID
	for s := r.from; s < r.from+r.slots+delaySlots; s++ {
		for _, tx := range held[s] {
			must(t, n.intake(received{tx: tx}))
		}
		delete(held, s)
		var heaviest *base.TransactionID
		var heaviestCov uint64
		r.src.Iterator(base.Slot2Bytes(s)).Iterate(func(_, v []byte) bool {
			tx, err := transaction.Parse(v)
			if err != nil || s >= r.from+r.slots {
				return err == nil && s < r.from+r.slots
			}
			fed++
			if delayEvery > 0 && fed%int(delayEvery) == 0 {
				held[s+delaySlots] = append(held[s+delaySlots], tx)
				delayed++
				if pred, ok := tx.StemPredecessor(); ok {
					must(t, n.claimBranch(tx.ID(), pred)) // as the feeders' branch lists would
				}
			} else if unparsedEvery > 0 && fed%int(unparsedEvery) == 0 {
				must(t, n.intake(received{txid: tx.ID(), raw: v}))
				unparsed++
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
		r.step(heaviest)
		if heaviest != nil {
			lastLRB = heaviest
		}
	}
	if unparsed > 0 {
		var held, sealed int64
		must(t, n.queryRow(`SELECT count(*), coalesce(sum(sealed), 0) FROM branch WHERE canonical_no IS NOT NULL`).Scan(&held, &sealed))
		var waitingOn, pulls int64
		must(t, n.queryRow(`SELECT count(*) FROM waiting w JOIN tx x ON x.id = w.tx WHERE x.state = ? AND x.parsed = 0`, store.StateIntake).Scan(&waitingOn))
		for id := range n.pulls {
			if _, state, err := n.lookup(id); err == nil && state == store.StateIntake {
				pulls++ // an unparsed transaction asked by pull: its bytes are stored already
			}
		}
		t.Logf("%d stored unparsed: %d of %d canonical branches sealed, %d waits on unparsed transactions, %d of them pulled",
			unparsed, sealed, held, waitingOn, pulls)
		if waitingOn == 0 || sealed == held {
			t.Fatalf("expected walks waiting on unparsed transactions and canonical branches held unsealed")
		}
		if pulls != 0 {
			t.Fatalf("unparsed transactions must not be pulled: %d pulls", pulls)
		}
		// the restart: what is stored unparsed is parsed, the waiting walks resume, and sealing,
		// 500 branches a step, catches up: every canonical branch must end sealed
		must(t, n.parsePending())
		for i := 0; i < 500 && sealed < held; i++ {
			r.step(lastLRB)
			must(t, n.queryRow(`SELECT coalesce(sum(sealed), 0) FROM branch WHERE canonical_no IS NOT NULL`).Scan(&sealed))
		}
		if sealed < held {
			t.Fatalf("after parsing, %d of %d canonical branches sealed", sealed, held)
		}
	}
	t.Logf("%d of them delayed by %d slots", delayed, delaySlots)
	t.Logf("fed %d transactions over %d slots in %v (%.0f tx/s)", fed, r.slots, time.Since(t0).Round(time.Second), float64(fed)/time.Since(t0).Seconds())
	r.check()
}

// TestReplayUnparsed checks that transactions of a library the dagstore does not hold are kept and
// end up exactly as if the library had been there: the same slot range is fed with the libraries
// of upgrades 6 and 7 withheld, so every transaction of that history (version 6) fails the version
// check and is stored unparsed, nothing is linked; then the full libraries are loaded, as a restart
// does, the stored transactions parsed and the history walked, and the result compared with the
// importer's as in TestReplay. Same environment as TestReplay.
func TestReplayUnparsed(t *testing.T) {
	r := newReplay(t, map[uint32]bool{196000: true, 283000: true})
	defer r.close()
	n := r.n

	var fed int
	for s := r.from; s < r.from+r.slots; s++ {
		r.src.Iterator(base.Slot2Bytes(s)).Iterate(func(k, v []byte) bool {
			_, err := transaction.Parse(v)
			if !errors.Is(err, transaction.ErrTxVersionMismatch) {
				t.Fatalf("expected a version mismatch without the library, got %v", err)
			}
			txid, err := base.TransactionIDFromBytes(k)
			must(t, err)
			must(t, n.intake(received{txid: txid, raw: v}))
			fed++
			return true
		})
	}
	var unparsed, linked int64
	must(t, n.queryRow(`SELECT coalesce(sum(state = ? AND parsed = 0), 0), coalesce(sum(state = ?), 0) FROM tx`,
		store.StateIntake, store.StateLinked).Scan(&unparsed, &linked))
	t.Logf("fed %d transactions without the library: %d stored unparsed, %d linked", fed, unparsed, linked)
	if unparsed != int64(fed) || linked != 0 {
		t.Fatalf("stored unparsed %d of %d, linked %d", unparsed, fed, linked)
	}

	// the restart: every library loaded, what is stored unparsed parsed
	ledger.MustInitLibraryCacheFromMap(r.libs)
	must(t, n.parsePending())
	must(t, n.queryRow(`SELECT coalesce(sum(state = ? AND parsed = 0), 0) FROM tx`, store.StateIntake).Scan(&unparsed))
	if unparsed != 0 {
		t.Fatalf("%d transactions still unparsed with every library loaded", unparsed)
	}
	for s := r.from; s < r.from+r.slots; s++ {
		var heaviest *base.TransactionID
		var txid []byte
		if err := n.queryRow(`SELECT t.txid FROM branch b JOIN tx t ON t.id = b.tx WHERE b.slot = ? ORDER BY b.total_coverage DESC LIMIT 1`, s).
			Scan(&txid); err == nil {
			id, err := base.TransactionIDFromBytes(txid)
			must(t, err)
			heaviest = &id
		}
		r.step(heaviest)
	}
	r.check()
}

// replay is the setup TestReplay and TestReplayUnparsed share: the imported database to compare
// with, the libraries, the txstore and a node over a new database.
type replay struct {
	t           *testing.T
	imported    *sql.DB
	libs        map[uint32][]byte
	txDB        *badger.DB
	src         *badger_adaptor.DB
	db          *sql.DB
	n           *Node
	from, slots uint32
}

// newReplay loads the libraries without the upgrades in withhold, which a test may load later.
func newReplay(t *testing.T, withhold map[uint32]bool) *replay {
	txstorePath, importedPath := os.Getenv("DAGSTORE_REPLAY_TXSTORE"), os.Getenv("DAGSTORE_REPLAY_IMPORTED")
	if txstorePath == "" || importedPath == "" {
		t.Skip("DAGSTORE_REPLAY_TXSTORE and DAGSTORE_REPLAY_IMPORTED not set")
	}
	r := &replay{t: t, libs: make(map[uint32][]byte)}
	r.from, r.slots = envUint(t, "DAGSTORE_REPLAY_FROM", 225000), envUint(t, "DAGSTORE_REPLAY_SLOTS", 500)
	var err error
	r.imported, err = store.OpenReadOnly(importedPath, 2)
	must(t, err)
	libs, err := store.LoadLibraries(r.imported)
	must(t, err)
	loaded := make(map[uint32][]byte)
	for _, l := range libs {
		r.libs[l.ActivationSlot] = l.JSON
		if !withhold[l.ActivationSlot] {
			loaded[l.ActivationSlot] = l.JSON
		}
	}
	ledger.MustInitLibraryCacheFromMap(loaded)

	r.txDB, err = badger.Open(badger.DefaultOptions(txstorePath).WithReadOnly(true).WithLogger(nil))
	must(t, err)
	r.src = badger_adaptor.New(r.txDB)
	r.db, err = store.Create(filepath.Join(t.TempDir(), "replay.db"))
	must(t, err)
	r.n = &Node{db: r.db, params: dagstore.DefaultParams, pulls: make(map[base.TransactionID]*pullState),
		gate: ingate.New[base.TransactionID](time.Hour, 100_000, 1_000_000)}
	r.n.codec, err = store.LoadCodec(r.db)
	must(t, err)
	r.n.branchFetch.started, r.n.branchFetch.done = true, true
	must(t, r.n.begin())
	return r
}

func (r *replay) close() {
	_ = r.db.Close()
	_ = r.txDB.Close()
	_ = r.imported.Close()
}

// step is one slot of the loop: the heaviest branch of the slot as the LRB, the walk, sealing.
func (r *replay) step(lrb *base.TransactionID) {
	if lrb != nil {
		r.n.lrb = lrb
		must(r.t, r.n.updateCanonical())
	}
	for i := 0; i < 20; i++ {
		must(r.t, r.n.walkStep())
	}
	must(r.t, r.n.sealStep())
	must(r.t, r.n.codec.TrainIfDue(r.n.tx))
	must(r.t, r.n.tx.Commit())
	must(r.t, r.n.begin())
}

// check purges orphans, checks that no edge dangles, and compares with the importer's database
// a margin away from the ends of the range: canonical lineage, confirmations per canonical branch,
// kept transactions.
func (r *replay) check() {
	t, n, db := r.t, r.n, r.db
	var intakeBefore, intakeAfter int64
	must(t, n.queryRow(`SELECT count(*) FROM tx WHERE state = ?`, store.StateIntake).Scan(&intakeBefore))
	must(t, n.purgeOrphans())
	must(t, n.queryRow(`SELECT count(*) FROM tx WHERE state = ?`, store.StateIntake).Scan(&intakeAfter))
	t.Logf("orphan purge: intake %d -> %d", intakeBefore, intakeAfter)
	must(t, n.tx.Commit())
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

	r.checkBytes()

	var startSlot, tipSlot uint32
	must(t, db.QueryRow(`SELECT min(slot), max(slot) FROM branch WHERE canonical_no IS NOT NULL`).Scan(&startSlot, &tipSlot))
	var walkedTo uint32
	must(t, db.QueryRow(`SELECT coalesce(max(slot), 0) FROM branch WHERE walked = 1 AND canonical_no IS NOT NULL`).Scan(&walkedTo))
	var sealedCanon, canon, pending, lost int64
	must(t, db.QueryRow(`SELECT count(*), coalesce(sum(sealed), 0) FROM branch WHERE canonical_no IS NOT NULL`).Scan(&canon, &sealedCanon))
	must(t, db.QueryRow(`SELECT coalesce(sum(state = ?), 0), coalesce(sum(state = ?), 0) FROM tx`, store.StateMissing, store.StateLost).Scan(&pending, &lost))
	t.Logf("canonical lineage slots %d..%d (%d branches, %d sealed), walked to %d, missing %d, lost %d",
		startSlot, tipSlot, canon, sealedCanon, walkedTo, pending, lost)

	lo, hi := startSlot+100, walkedTo-100
	if hi <= lo {
		t.Fatalf("range too short to compare: walked %d..%d", startSlot, walkedTo)
	}
	got, want := canonicalLineage(t, db, lo, hi), canonicalLineage(t, r.imported, lo, hi)
	if len(got) != len(want) {
		t.Fatalf("canonical branches in %d..%d: %d, imported %d", lo, hi, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("canonical lineage differs at %d", i)
		}
	}
	gotConf, wantConf := confirmedPerBranch(t, db, lo, hi), confirmedPerBranch(t, r.imported, lo, hi)
	diff := 0
	for k, v := range wantConf {
		if gotConf[k] != v {
			diff++
			if diff <= 5 {
				t.Errorf("branch %x: confirmed %d, imported %d", k[:6], gotConf[k], v)
			}
		}
	}
	gotKept, wantKept := keptInSlots(t, db, lo, hi), keptInSlots(t, r.imported, lo, hi)
	t.Logf("slots %d..%d: %d canonical branches, %d differ in confirmations; kept %d, imported %d", lo, hi, len(got), diff, gotKept, wantKept)
	if gotKept != wantKept {
		t.Errorf("kept transactions in slots %d..%d: %d, imported %d", lo, hi, gotKept, wantKept)
	}
}

// checkBytes decodes every stored transaction and compares it with the txstore's bytes: the rows
// stored raw before the dictionary was trained, those recompressed when it was, and those
// compressed from then on. The range holds far more than the training sample, so all three occur.
func (r *replay) checkBytes() {
	t := r.t
	rows, err := r.db.Query(`SELECT t.txid, b.bytes, b.dict FROM tx t JOIN tx_bytes b ON b.id = t.id`)
	must(t, err)
	defer rows.Close()
	var raw, compressed, storedBytes, rawBytes int64
	for rows.Next() {
		var txid, data []byte
		var dict sql.NullInt64
		must(t, rows.Scan(&txid, &data, &dict))
		got, err := r.n.codec.Decode(dict, data)
		must(t, err)
		id, err := base.TransactionIDFromBytes(txid)
		must(t, err)
		if want := r.src.Get(id[:]); string(got) != string(want) {
			t.Fatalf("bytes of %s differ from the txstore's", id.StringShort())
		}
		if dict.Valid {
			compressed++
		} else {
			raw++
		}
		storedBytes += int64(len(data))
		rawBytes += int64(len(got))
	}
	must(t, rows.Err())
	t.Logf("bytes: %d rows compressed, %d raw, stored %.3f of raw size", compressed, raw, float64(storedBytes)/float64(max(1, rawBytes)))
	if compressed == 0 {
		t.Errorf("no row compressed: the dictionary was not trained")
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
