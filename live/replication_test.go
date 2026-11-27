package live

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lunfardo314/dagstore"
	"github.com/lunfardo314/dagstore/api"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/global"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/ledger/transaction"
	"github.com/lunfardo314/proxima/peering"
	"github.com/lunfardo314/unitrie/adaptors/badger_adaptor"
)

// TestReplication runs two dagstores over real peering on localhost (SPEC.md section 5.4), each
// the other's feeder and replica. A is fed a slot range of real history through its intake while
// it runs, with the heaviest branch of each slot as its feeders' LRB; B starts empty and learns
// everything from A: the LRB and the branch list by lineage requests, the transactions by A's
// pushes and, for what a full send queue dropped, by pulls at the source pace. Once B has caught up,
// both must hold the same canonical lineage, the same confirmations per canonical branch, the same
// kept transactions and the same sealed tip, built independently; and B must not have pushed back
// anything it got from A. Environment as TestReplay; DAGSTORE_REPLICATION_SLOTS (default 160) slots
// from DAGSTORE_REPLAY_FROM.
func TestReplication(t *testing.T) {
	txstorePath, importedPath := os.Getenv("DAGSTORE_REPLAY_TXSTORE"), os.Getenv("DAGSTORE_REPLAY_IMPORTED")
	if txstorePath == "" || importedPath == "" {
		t.Skip("DAGSTORE_REPLAY_TXSTORE and DAGSTORE_REPLAY_IMPORTED not set")
	}
	from, slots := envUint(t, "DAGSTORE_REPLAY_FROM", 225000), envUint(t, "DAGSTORE_REPLICATION_SLOTS", 160)
	imported, err := store.OpenReadOnly(importedPath, 2)
	must(t, err)
	defer imported.Close()
	libs, err := store.LoadLibraries(imported)
	must(t, err)
	txDB, err := badger.Open(badger.DefaultOptions(txstorePath).WithReadOnly(true).WithLogger(nil))
	must(t, err)
	defer txDB.Close()
	src := badger_adaptor.New(txDB)

	params := dagstore.DefaultParams
	params.CanonicalDepthSlots = 64
	params.HeartbeatPeriod = 2 * time.Second
	params.MissingRetryInterval = 10 * time.Second
	type ds struct {
		n   *Node
		env *global.Global
		rdb *sql.DB
	}
	mk := func(i int) ds {
		dir := t.TempDir()
		if keep := os.Getenv("DAGSTORE_REPLICATION_KEEP"); keep != "" {
			dir = keep // kept after the test, for a look at both databases
		}
		path := filepath.Join(dir, fmt.Sprintf("ds%d.db", i))
		db, err := store.Create(path)
		must(t, err)
		must(t, store.SaveLibraries(db, libs))
		ll, err := api.LoadLedger(db)
		must(t, err)
		codec, err := store.LoadCodec(db)
		must(t, err)
		rdb, err := store.OpenReadOnly(path, 4)
		must(t, err)
		cfg := peering.MakeConfigFor(2, i)
		other := fmt.Sprintf("peer%d", 1-i)
		cfg.Feeders, cfg.DAGStores = []string{other}, []string{other}
		env := global.NewDefault()
		n, err := newNode(db, rdb, env, params, ll, codec, "", cfg)
		must(t, err)
		return ds{n, env, rdb}
	}
	a, b := mk(0), mk(1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 2)
	for _, d := range []ds{a, b} {
		go func() { done <- d.n.Run(ctx) }()
	}
	time.Sleep(3 * time.Second) // peering connects

	// feed A, about 2,000 transactions a second, so that pushes mostly fit B's send queue
	bID := a.n.feeders[0].id
	t0 := time.Now()
	fed := 0
	for s := from; s < from+slots; s++ {
		var heaviest *base.TransactionID
		var heaviestCov uint64
		src.Iterator(base.Slot2Bytes(s)).Iterate(func(_, v []byte) bool {
			tx, err := transaction.Parse(v)
			if err != nil {
				return false
			}
			a.n.in <- received{tx: tx, txid: tx.ID(), raw: v}
			fed++
			if stem := store.Stem(tx); stem != nil && stem.TotalCoverage > heaviestCov {
				id := tx.ID()
				heaviest, heaviestCov = &id, stem.TotalCoverage
			}
			return true
		})
		if heaviest != nil {
			a.n.lineage <- lrbMsg{from: bID, r: peering.LRBReply{LRB: *heaviest, OldestSlot: from}}
		}
		if ahead := time.Duration(fed)*time.Second/2000 - time.Since(t0); ahead > 0 {
			time.Sleep(ahead)
		}
	}
	t.Logf("fed A %d transactions over %d slots in %v", fed, slots, time.Since(t0).Round(time.Second))

	meta := func(d ds, key string) string {
		var v []byte
		_ = d.rdb.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
		return string(v)
	}
	deadline := time.Now().Add(5 * time.Minute)
	for {
		aTip, aSealed := meta(a, "canonical_tip"), meta(a, "sealed_tip")
		if aTip != "" && aTip == meta(b, "canonical_tip") && aSealed == aTip && meta(b, "sealed_tip") == aTip &&
			keptInSlots(t, a.rdb, from, from+slots) == keptInSlots(t, b.rdb, from, from+slots) {
			break
		}
		if time.Now().After(deadline) {
			rows, err := b.rdb.Query(`SELECT txid, state FROM tx WHERE state IN (?, ?) LIMIT 10`, store.StateLost, store.StateMissing)
			must(t, err)
			for rows.Next() {
				var id []byte
				var bState int
				must(t, rows.Scan(&id, &bState))
				aState, aBytes, aSource := -1, -1, -1
				_ = a.rdb.QueryRow(`SELECT t.state, coalesce(length(b.bytes), -1), coalesce(t.source, -1) FROM tx t LEFT JOIN tx_bytes b ON b.id = t.id
					WHERE t.txid = ?`, id).Scan(&aState, &aBytes, &aSource)
				txid, _ := base.TransactionIDFromBytes(id)
				t.Logf("B %s state %d; A state %d, bytes %d, source %d", txid.StringShort(), bState, aState, aBytes, aSource)
			}
			rows.Close()
			t.Fatalf("B did not catch up with A: canonical tips equal %v, A sealed to its tip %v, B sealed to A's tip %v",
				aTip == meta(b, "canonical_tip"), aSealed == aTip, meta(b, "sealed_tip") == aTip)
		}
		time.Sleep(2 * time.Second)
	}
	t.Logf("B caught up %v after the last slot was fed", time.Since(t0).Round(time.Second))
	cancel()
	for range 2 {
		must(t, <-done)
	}
	a.env.Stop()
	b.env.Stop()

	var lo, hi uint32
	must(t, a.rdb.QueryRow(`SELECT min(slot), max(slot) FROM branch WHERE canonical_no IS NOT NULL`).Scan(&lo, &hi))
	if ga, gb := canonicalLineage(t, a.rdb, lo, hi), canonicalLineage(t, b.rdb, lo, hi); !slices.Equal(ga, gb) {
		t.Fatalf("canonical lineage in %d..%d: A %d branches, B %d", lo, hi, len(ga), len(gb))
	}
	ca, cb := confirmedPerBranch(t, a.rdb, lo, hi), confirmedPerBranch(t, b.rdb, lo, hi)
	diff := 0
	for k, v := range ca {
		if cb[k] != v {
			diff++
		}
	}
	ka, kb := keptInSlots(t, a.rdb, lo, hi), keptInSlots(t, b.rdb, lo, hi)
	var pushed, pulled int64
	must(t, b.rdb.QueryRow(`SELECT coalesce(sum(source = ?), 0), coalesce(sum(source = ?), 0) FROM tx`, store.SourcePush, store.SourcePull).Scan(&pushed, &pulled))
	fa := b.n.feeders[0]
	t.Logf("canonical %d..%d: %d branches, %d differ in confirmations; kept A %d, B %d; B got %d by push, %d by pull (%d pulls sent, %d timeouts); pushed back to A: %d",
		lo, hi, len(ca), diff, ka, kb, pushed, pulled, fa.pullsSent.Load(), fa.timeouts.Load(), a.n.feeders[0].pushes.Load())
	if diff != 0 || ka != kb {
		t.Errorf("A and B differ")
	}
	if !fa.isDAGStore {
		t.Errorf("B did not learn from A's heartbeat that its feeder is a dagstore")
	}
	if back := a.n.feeders[0].pushes.Load(); back != 0 {
		t.Errorf("B pushed %d transactions back to A, which it got from A", back)
	}
}
