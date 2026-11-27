// Command dagstore-import loads the canonical history held in a node's txstore into a new dagstore
// database, offline. It is the first milestone of the dagstore: it exercises the schema, the cone
// walk and sealing on real history, and measures size per kept transaction and import speed.
//
// Offline there are no feeders, so the canonical tip is chosen from the txstore alone: the
// heaviest branch of the newest slot, then the branch of its lineage at least -depth slots older.
// The node's state database is read only for the ledger libraries, which decoding outputs needs.
package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lunfardo314/dagstore"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/ledger/multistate"
	"github.com/lunfardo314/proxima/ledger/transaction"
	"github.com/lunfardo314/unitrie/adaptors/badger_adaptor"
)

func main() {
	txstorePath := flag.String("txstore", "", "node txstore directory (proximadb.txstore); opened read-only")
	statePath := flag.String("state", "", "node state directory (proximadb), for the libraries; use a copy, opening it may write")
	outPath := flag.String("out", "", "new SQLite database file")
	librariesInto := flag.String("libraries_into", "", "existing dagstore database: only write the libraries of -state into it")
	depth := flag.Int("depth", 120, "canonical depth: slots between the newest branch and the canonical tip")
	commitEvery := flag.Int("commit_every", 20, "branches per SQLite transaction")
	flag.Parse()
	log.Print(dagstore.BannerString())
	if *statePath == "" || (*librariesInto == "" && (*txstorePath == "" || *outPath == "")) {
		flag.Usage()
		os.Exit(2)
	}

	libs := loadLibraries(*statePath)
	if *librariesInto != "" {
		db, err := store.Open(*librariesInto)
		if err != nil {
			log.Fatal(err)
		}
		if err = store.SaveLibraries(db, libs); err != nil {
			log.Fatalf("libraries: %v", err)
		}
		_ = db.Close()
		log.Printf("%d libraries written into %s", len(libs), *librariesInto)
		return
	}

	opts := badger.DefaultOptions(*txstorePath).WithReadOnly(true).WithLogger(nil)
	txDB, err := badger.Open(opts)
	if err != nil {
		log.Fatalf("txstore: %v", err)
	}
	defer txDB.Close()
	src := badger_adaptor.New(txDB)

	db, err := store.Create(*outPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err = store.SaveLibraries(db, libs); err != nil {
		log.Fatalf("libraries: %v", err)
	}

	codec, err := store.LoadCodec(db)
	if err != nil {
		log.Fatal(err)
	}
	im := &importer{src: src, db: db, ids: make(map[base.TransactionID]*node), commitEvery: *commitEvery, codec: codec}
	t0 := time.Now()
	im.scan()
	log.Printf("scan: %d transactions, %d bytes, slots %d..%d, %d branches, in %v",
		im.stats.stored, im.stats.storedBytes, im.minSlot, im.maxSlot, len(im.branches), time.Since(t0).Round(time.Second))

	t1 := time.Now()
	im.buildBranchTable(uint32(*depth))
	im.walk()
	walkTime := time.Since(t1)
	im.finish()
	log.Printf("walk: %v", walkTime.Round(time.Second))
	im.report(*outPath, walkTime)
}

// node is the walk's in-memory view of one transaction row.
type node struct {
	id    int64
	state uint8
}

const (
	nodeAllocated   = iota // id handed out to an edge, row not written yet
	nodeKept               // in the cone of a non-canonical branch only
	nodeConfirmed          // in the cone of a canonical branch
	nodePlaceholder        // missing or pruned
)

type branchInfo struct {
	txid        base.TransactionID
	predecessor base.TransactionID
	sequencer   base.ChainID
	stem        *ledger.StemLock
	canonicalNo int64 // 0 if not canonical
	gaps        int
	sealed      bool
	fork        int64
}

type importer struct {
	src         *badger_adaptor.DB
	db          *sql.DB
	tx          *sql.Tx
	stmts       map[string]*sql.Stmt
	commitEvery int
	codec       *store.Codec

	minSlot, maxSlot uint32
	branchesBySlot   map[uint32][]base.TransactionID
	branches         map[base.TransactionID]*branchInfo
	order            []*branchInfo // walk order: slot ascending, canonical first
	startSlot        uint32
	startBranch      base.TransactionID
	canonicalTip     base.TransactionID
	isPredecessor    map[base.TransactionID]bool

	ids        map[base.TransactionID]*node
	nextID     int64
	accounts   map[string]int64
	chains     map[base.ChainID]int64
	lockKinds  map[string]int64
	curGaps    *int
	stats      stats
	keptInTxst map[base.TransactionID]struct{}
}

type stats struct {
	stored, storedBytes                 int64
	kept, keptBytes, confirmed          int64
	missing, pruned                     int64
	inputs, endorsements, outputs       int64
	sigFailed, parseFailed, idMismatch  int64
	unknownLock, outputDecodeFailed     int64
	forks, longForks, branchesNoLineage int
}

// scan reads every key and value of the txstore once: counts, raw size and the branches per slot.
func (im *importer) scan() {
	im.branchesBySlot = make(map[uint32][]base.TransactionID)
	im.minSlot = ^uint32(0)
	im.src.Iterator(nil).Iterate(func(k, v []byte) bool {
		txid, err := base.TransactionIDFromBytes(k)
		if err != nil {
			return true
		}
		im.stats.stored++
		im.stats.storedBytes += int64(len(v))
		s := txid.Slot()
		im.minSlot = min(im.minSlot, s)
		im.maxSlot = max(im.maxSlot, s)
		if txid.IsBranchTransaction() {
			im.branchesBySlot[s] = append(im.branchesBySlot[s], txid)
		}
		return true
	})
	im.branches = make(map[base.TransactionID]*branchInfo)
	for _, list := range im.branchesBySlot {
		for _, id := range list {
			tx := im.load(id)
			if tx == nil {
				continue
			}
			stem, seqID, ok := branchData(tx)
			if !ok {
				im.stats.parseFailed++
				continue
			}
			im.branches[id] = &branchInfo{txid: id, predecessor: stem.PredecessorOutputID.TransactionID(), sequencer: seqID, stem: stem}
		}
	}
}

// buildBranchTable picks the canonical tip, numbers the canonical lineage from its oldest branch
// present (the starting branch) and records every other branch in a fork.
func (im *importer) buildBranchTable(depth uint32) {
	var newest *branchInfo
	for s := im.maxSlot; s >= im.minSlot && newest == nil; s-- {
		for _, id := range im.branchesBySlot[s] {
			if b := im.branches[id]; b != nil && (newest == nil || b.stem.TotalCoverage > newest.stem.TotalCoverage) {
				newest = b
			}
		}
	}
	if newest == nil {
		log.Fatal("no branch in the txstore")
	}
	tip := newest
	for tip.txid.Slot()+depth > newest.txid.Slot() {
		p := im.branches[tip.predecessor]
		if p == nil {
			log.Fatalf("lineage of the newest branch is shorter than %d slots", depth)
		}
		tip = p
	}
	im.canonicalTip = tip.txid
	var lineage []*branchInfo
	for b := tip; b != nil; b = im.branches[b.predecessor] {
		lineage = append(lineage, b)
	}
	start := lineage[len(lineage)-1]
	im.startBranch, im.startSlot = start.txid, start.txid.Slot()
	for i, b := range lineage {
		b.canonicalNo = int64(len(lineage) - i)
	}
	log.Printf("canonical lineage: %d branches, slots %d..%d (tip %s, newest branch at %d); predecessor of the starting branch %s not stored",
		len(lineage), im.startSlot, tip.txid.Slot(), tip.txid.StringShort(), newest.txid.Slot(), start.predecessor.StringShort())

	// Forks: every non-canonical branch tip at or after the starting slot, traced back to its fork point.
	im.isPredecessor = make(map[base.TransactionID]bool)
	for _, b := range im.branches {
		im.isPredecessor[b.predecessor] = true
	}
	var forkID int64
	for _, b := range im.branches {
		if b.canonicalNo != 0 || im.isPredecessor[b.txid] || b.txid.Slot() < im.startSlot || b.txid.Slot() > tip.txid.Slot() {
			continue
		}
		forkID++
		length, cur := 0, b
		for cur != nil && cur.canonicalNo == 0 {
			if cur.fork == 0 {
				cur.fork = forkID
			}
			length++
			cur = im.branches[cur.predecessor]
		}
		im.stats.forks++
		if length > 1 {
			im.stats.longForks++
		}
		if cur == nil {
			im.stats.branchesNoLineage++
		}
		b.fork = forkID // mark the tip for writeForks
	}

	for _, b := range im.branches {
		s := b.txid.Slot()
		if s >= im.startSlot && s <= tip.txid.Slot() && (b.canonicalNo != 0 || b.fork != 0) {
			im.order = append(im.order, b)
		}
	}
	sort.Slice(im.order, func(i, j int) bool {
		si, sj := im.order[i].txid.Slot(), im.order[j].txid.Slot()
		if si != sj {
			return si < sj
		}
		if (im.order[i].canonicalNo != 0) != (im.order[j].canonicalNo != 0) {
			return im.order[i].canonicalNo != 0
		}
		return bytes.Compare(im.order[i].txid[:], im.order[j].txid[:]) < 0
	})
}

// walk follows the branch table in slot order and walks each branch's cone delta: a canonical walk
// stops at transactions already confirmed, a non-canonical one at transactions already kept.
func (im *importer) walk() {
	im.accounts = make(map[string]int64)
	im.chains = make(map[base.ChainID]int64)
	im.lockKinds = make(map[string]int64)
	im.keptInTxst = make(map[base.TransactionID]struct{})
	im.begin()
	last := time.Now()
	for i, b := range im.order {
		im.curGaps = &b.gaps
		im.walkBranch(b)
		if b.txid == im.startBranch {
			b.sealed = b.gaps == 0
		} else if p := im.branches[b.predecessor]; p != nil {
			b.sealed = p.sealed && b.gaps == 0
		}
		if (i+1)%im.commitEvery == 0 {
			if err := im.codec.TrainIfDue(im.tx); err != nil {
				log.Fatal(err)
			}
			im.commit()
			im.begin()
		}
		if time.Since(last) > 30*time.Second {
			last = time.Now()
			log.Printf("walk: slot %d, %d/%d branches, %d kept, %d missing", b.txid.Slot(), i+1, len(im.order), im.stats.kept, im.stats.missing)
		}
	}
	im.commit()
}

func (im *importer) walkBranch(b *branchInfo) {
	canonical := b.canonicalNo != 0
	bid := im.get(b.txid).id
	stack := []base.TransactionID{b.txid}
	for len(stack) > 0 {
		txid := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		n := im.get(txid)
		switch n.state {
		case nodeConfirmed:
			continue
		case nodePlaceholder:
			if txid.Slot() >= im.startSlot {
				*im.curGaps++
				im.exec("waiting", n.id, bid)
			}
			continue
		case nodeKept:
			if !canonical {
				continue
			}
		}
		if txid.Slot() < im.startSlot {
			im.placeholder(n, txid, store.StatePruned)
			continue
		}
		tx := im.load(txid)
		if tx == nil {
			im.placeholder(n, txid, store.StateMissing)
			*im.curGaps++
			im.exec("waiting", n.id, bid) // as the live walk records it, so live sealing can take over
			continue
		}
		if n.state == nodeKept {
			im.exec("confirm", bid, n.id)
			n.state = nodeConfirmed
			im.stats.confirmed++
		} else {
			im.insert(n, tx, canonical, bid)
		}
		tx.ForEachInputID(func(_ byte, oid base.OutputID) bool {
			stack = append(stack, oid.TransactionID())
			return true
		})
		tx.ForEachEndorsement(func(_ byte, id base.TransactionID) bool {
			stack = append(stack, id)
			return true
		})
		if bl, ok := tx.ExplicitBaseline(); ok {
			stack = append(stack, bl)
		}
	}
}

// get returns the node of txid, handing out a new id if it has none yet.
func (im *importer) get(txid base.TransactionID) *node {
	n := im.ids[txid]
	if n == nil {
		im.nextID++
		n = &node{id: im.nextID}
		im.ids[txid] = n
	}
	return n
}

func (im *importer) load(txid base.TransactionID) *transaction.Transaction {
	raw := im.src.Get(txid[:])
	if len(raw) == 0 {
		return nil
	}
	tx, err := transaction.Parse(raw)
	if err != nil {
		im.stats.parseFailed++
		return nil
	}
	if tx.ID() != txid {
		im.stats.idMismatch++
		return nil
	}
	return tx
}

func (im *importer) placeholder(n *node, txid base.TransactionID, state int) {
	im.exec("placeholder", n.id, txid[:], txid.Slot(), txid.Timestamp().Tick, state)
	n.state = nodePlaceholder
	if state == store.StateMissing {
		im.stats.missing++
		im.exec("missing", n.id)
	} else {
		im.stats.pruned++
	}
}

func (im *importer) insert(n *node, tx *transaction.Transaction, canonical bool, branchID int64) {
	txid := tx.ID()
	raw := tx.Bytes()
	kind := store.TxKind(tx)
	var seq any
	if id, ok := store.SequencerID(tx); ok {
		seq = im.chainRow(id, store.ChainSequencer)
	}
	var baseline any
	if bl, ok := tx.ExplicitBaseline(); ok {
		baseline = im.get(bl).id
	}
	var confirming any
	if canonical {
		confirming = branchID
		n.state = nodeConfirmed
		im.stats.confirmed++
	} else {
		n.state = nodeKept
	}
	// the dagstore's only check of content beyond parsing: the txid does not cover the signature
	_, sigErr := tx.VerifySignature()
	sigOK := sigErr == nil
	if !sigOK {
		im.stats.sigFailed++
	}
	im.exec("tx", n.id, txid[:], txid.Slot(), txid.Timestamp().Tick, kind, seq, baseline, len(raw), store.StateLinked,
		confirming, boolInt(sigOK), 1, store.SourceImport)
	dict, data := im.codec.Encode(raw)
	im.exec("tx_bytes", n.id, data, dict)
	im.stats.kept++
	im.stats.keptBytes += int64(len(raw))
	im.keptInTxst[txid] = struct{}{}

	tx.ForEachInputID(func(i byte, oid base.OutputID) bool {
		im.exec("input", n.id, i, im.get(oid.TransactionID()).id, oid.Index())
		im.stats.inputs++
		return true
	})
	tx.ForEachEndorsement(func(i byte, id base.TransactionID) bool {
		im.exec("endorsement", n.id, i, im.get(id).id)
		im.stats.endorsements++
		return true
	})
	tx.ForEachProducedOutput(func(i byte, o *ledger.Output, oid base.OutputID) bool {
		im.output(n.id, i, o, oid)
		return true
	})
}

func (im *importer) output(txRow int64, i byte, o *ledger.Output, oid base.OutputID) {
	im.stats.outputs++
	f := store.OutputFactsOf(o, oid)
	var lockKind, account, chain any
	if f.Lock != "" {
		lockKind = im.lockKindRow(f.Lock)
	} else {
		im.stats.unknownLock++
	}
	if f.Account != nil {
		account = im.accountRow(f.Account)
	}
	if f.ChainID != nil {
		chain = im.chainRow(*f.ChainID, f.ChainKind)
	}
	im.exec("output", txRow, i, int64(o.TokenBalance()), lockKind, account, chain)
}

func (im *importer) accountRow(holder []byte) int64 {
	if id, ok := im.accounts[string(holder)]; ok {
		return id
	}
	id := int64(len(im.accounts) + 1)
	im.accounts[string(holder)] = id
	im.exec("account", id, holder)
	return id
}

func (im *importer) chainRow(chainID base.ChainID, kind int) int64 {
	if id, ok := im.chains[chainID]; ok {
		return id
	}
	id := int64(len(im.chains) + 1)
	im.chains[chainID] = id
	im.exec("chain", id, chainID[:], kind)
	return id
}

func (im *importer) lockKindRow(name string) int64 {
	if id, ok := im.lockKinds[name]; ok {
		return id
	}
	id := int64(len(im.lockKinds) + 1)
	im.lockKinds[name] = id
	im.exec("lock_kind", id, name)
	return id
}

// finish writes the branch and fork tables and the meta rows.
func (im *importer) finish() {
	im.begin()
	for _, b := range im.order {
		var pred, canon, fork any
		if p, ok := im.ids[b.predecessor]; ok {
			pred = p.id
		}
		if b.canonicalNo != 0 {
			canon = b.canonicalNo
		} else {
			fork = b.fork
		}
		im.exec("branch", im.ids[b.txid].id, b.txid.Slot(), im.chains[b.sequencer], pred,
			int64(b.stem.TotalCoverage), int64(b.stem.TotalSupply), int64(b.stem.SlotInflation), canon, fork, boolInt(b.sealed), b.gaps)
	}
	for _, b := range im.order {
		if b.canonicalNo != 0 {
			continue
		}
		// fork rows, one per tip: a tip is a branch that is no other stored branch's predecessor
		length, cur, sealedTip := 0, b, any(nil)
		for cur != nil && cur.canonicalNo == 0 {
			if sealedTip == nil && cur.sealed {
				sealedTip = im.ids[cur.txid].id
			}
			length++
			cur = im.branches[cur.predecessor]
		}
		var point any
		if cur != nil {
			point = im.ids[cur.txid].id
		}
		if b.fork != 0 && !im.isPredecessor[b.txid] {
			im.exec("fork", b.fork, im.ids[b.txid].id, point, length, sealedTip)
		}
	}
	libHash := ledger.L(0).LibraryHash()
	for k, v := range map[string]any{
		"starting_branch": im.startBranch[:],
		"canonical_tip":   im.canonicalTip[:],
		"library_hash":    libHash[:],
	} {
		im.exec("meta", k, v)
	}
	im.commit()
}

var statements = map[string]string{
	"tx": `INSERT INTO tx(id, txid, slot, tick, kind, sequencer, baseline, size, state, confirming_branch, sig_checked, parsed, source)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
	"placeholder": `INSERT INTO tx(id, txid, slot, tick, state) VALUES (?,?,?,?,?)`,
	"confirm":     `UPDATE tx SET confirming_branch = ? WHERE id = ?`,
	"tx_bytes":    `INSERT INTO tx_bytes(id, bytes, dict) VALUES (?,?,?)`,
	"input":       `INSERT INTO input(tx, idx, producer, out_idx) VALUES (?,?,?,?)`,
	"endorsement": `INSERT INTO endorsement(tx, idx, endorsed) VALUES (?,?,?)`,
	"output":      `INSERT INTO output(tx, idx, amount, lock_kind, account, chain) VALUES (?,?,?,?,?,?)`,
	"account":     `INSERT INTO account(id, holder) VALUES (?,?)`,
	"chain":       `INSERT INTO chain(id, chain_id, kind) VALUES (?,?,?)`,
	"lock_kind":   `INSERT INTO lock_kind(id, name) VALUES (?,?)`,
	"missing":     `INSERT INTO missing(tx) VALUES (?)`,
	// every branch of the table is walked here: walked = 1, as the live walk leaves it
	"branch": `INSERT INTO branch(tx, slot, sequencer, predecessor, total_coverage, total_supply, slot_inflation, canonical_no, fork, sealed, gaps, walked)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,1)`,
	"waiting": `INSERT OR IGNORE INTO waiting(tx, branch) VALUES (?,?)`,
	"fork":    `INSERT INTO fork(id, tip, fork_point, length, sealed_tip) VALUES (?,?,?,?,?)`,
	"meta":    `INSERT INTO meta(key, value) VALUES (?,?)`,
}

func (im *importer) begin() {
	var err error
	if im.tx, err = im.db.Begin(); err != nil {
		log.Fatal(err)
	}
	im.stmts = make(map[string]*sql.Stmt, len(statements))
	for k, q := range statements {
		if im.stmts[k], err = im.tx.Prepare(q); err != nil {
			log.Fatalf("%s: %v", k, err)
		}
	}
}

func (im *importer) commit() {
	if err := im.tx.Commit(); err != nil {
		log.Fatal(err)
	}
}

func (im *importer) exec(stmt string, args ...any) {
	if _, err := im.stmts[stmt].Exec(args...); err != nil {
		log.Fatalf("%s %v: %v", stmt, args, err)
	}
}

// branchData reads the stem lock and the sequencer ID of a branch transaction.
func branchData(tx *transaction.Transaction) (stem *ledger.StemLock, seqID base.ChainID, ok bool) {
	stem = store.Stem(tx)
	seqID, okSeq := store.SequencerID(tx)
	return stem, seqID, stem != nil && okSeq
}

// loadLibraries initialises the ledger libraries from the node's state database and returns every
// one of them for the library table, so the dagstore database parses without the state.
func loadLibraries(statePath string) []store.Library {
	stateDB := badger_adaptor.MustCreateOrOpenBadgerDB(statePath)
	defer func() { _ = stateDB.Close() }()
	stateStore := badger_adaptor.New(stateDB)
	if err := multistate.InitLedgerFromStore(stateStore, log.Printf); err != nil {
		log.Fatalf("libraries: %v", err)
	}
	var ret []store.Library
	multistate.IterateUpgradeLibraries(stateStore, func(slot uint32, libJSON []byte) bool {
		lib := ledger.L(slot)
		h := lib.LibraryHash()
		ret = append(ret, store.Library{UpgradeIndex: lib.UpgradeIndex(), ActivationSlot: slot, Hash: h[:], JSON: libJSON})
		return true
	})
	return ret
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (im *importer) report(outPath string, walkTime time.Duration) {
	if _, err := im.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		log.Printf("checkpoint: %v", err)
	}
	st := im.stats
	fileSize := int64(0)
	if fi, err := os.Stat(outPath); err == nil {
		fileSize = fi.Size()
	}
	sealed, sealedCanon := 0, 0
	for _, b := range im.order {
		if b.sealed {
			sealed++
			if b.canonicalNo != 0 {
				sealedCanon++
			}
		}
	}
	keptInStore := int64(len(im.keptInTxst))
	fmt.Printf("\n=== dagstore import: %s ===\n", filepath.Base(outPath))
	fmt.Printf("txstore: %d transactions, %d raw bytes (%.0f B avg), slots %d..%d\n",
		st.stored, st.storedBytes, float64(st.storedBytes)/float64(max(st.stored, 1)), im.minSlot, im.maxSlot)
	fmt.Printf("walked range: slots %d..%d (%d slots), %d branches (%d canonical sealed, %d sealed in all)\n",
		im.startSlot, im.canonicalTip.Slot(), im.canonicalTip.Slot()-im.startSlot+1, len(im.order), sealedCanon, sealed)
	fmt.Printf("forks: %d (longer than 1: %d, not reaching the canonical lineage: %d)\n", st.forks, st.longForks, st.branchesNoLineage)
	fmt.Printf("kept: %d (%d confirmed), %d raw bytes (%.0f B avg); orphans in the txstore: %d of %d (%.1f%%; includes slots outside the walked range)\n",
		st.kept, st.confirmed, st.keptBytes, float64(st.keptBytes)/float64(max(st.kept, 1)),
		st.stored-keptInStore, st.stored, 100*float64(st.stored-keptInStore)/float64(max(st.stored, 1)))
	fmt.Printf("placeholders: %d missing, %d pruned (below the starting branch)\n", st.missing, st.pruned)
	fmt.Printf("edges: %d inputs, %d endorsements (%.2f per kept tx); %d outputs (%.2f per kept tx)\n",
		st.inputs, st.endorsements, float64(st.inputs+st.endorsements)/float64(max(st.kept, 1)), st.outputs, float64(st.outputs)/float64(max(st.kept, 1)))
	fmt.Printf("checks: %d signatures failed, %d parse failures, %d txid mismatches, %d unknown locks\n",
		st.sigFailed, st.parseFailed, st.idMismatch, st.unknownLock)
	fmt.Printf("speed: %.0f kept tx/s over the walk (%v)\n", float64(st.kept)/walkTime.Seconds(), walkTime.Round(time.Second))
	fmt.Printf("database file: %d bytes, %.0f B per kept tx\n", fileSize, float64(fileSize)/float64(max(st.kept, 1)))

	rows, err := im.db.Query(`SELECT name, SUM(pgsize), SUM(payload), SUM(unused) FROM dbstat GROUP BY name ORDER BY SUM(pgsize) DESC`)
	if err != nil {
		log.Printf("dbstat: %v", err)
		return
	}
	defer rows.Close()
	fmt.Printf("%-24s %14s %10s %14s %12s\n", "table/index", "bytes", "B/kept tx", "payload", "unused")
	for rows.Next() {
		var name string
		var size, payload, unused int64
		if err = rows.Scan(&name, &size, &payload, &unused); err != nil {
			log.Print(err)
			return
		}
		fmt.Printf("%-24s %14d %10.1f %14d %12d\n", name, size, float64(size)/float64(max(st.kept, 1)), payload, unused)
	}
}
