package live

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/lunfardo314/dagstore"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/util/ingate"
)

// A real fork (SPEC.md section 6.6) on a synthetic branch table, no transaction bytes: everything
// is stored linked, so the walk only follows edges. Two lineages share slots 1..100 (A) and split
// there: A goes on to slot 300, B, from A's slot 100, too. Each branch spends its predecessor's
// output and endorses a plain transaction of its slot; plain transactions X are endorsed by both
// lineages, Y by B only. A losing branch L forks off A at slot 149.
//
// One dagstore follows A until it is canonical up to slot 236, then the feeders' LRB moves to B's
// tip. Another follows B from the start. After the renumbering both must hold exactly the same
// canonical lineage, confirmations, fork rows and sealed tip, and a restart must resume where the
// walk stands.
func TestRealFork(t *testing.T) {
	switched := newForkNode(t, "switched")
	switched.follow(t, "A300")
	switched.walkAll(t)
	if got := switched.confirmedBy(t, "X200"); got != "A200" {
		t.Fatalf("before the fork X200 is confirmed by %q, want A200", got)
	}
	switched.follow(t, "B300")
	switched.walkAll(t)

	fresh := newForkNode(t, "fresh")
	fresh.follow(t, "B300")
	fresh.walkAll(t)

	for _, tx := range []string{"X50", "X101", "X200", "Y150", "A50", "A101", "A200", "B120", "L150"} {
		if a, b := switched.confirmedBy(t, tx), fresh.confirmedBy(t, tx); a != b {
			t.Errorf("%s confirmed by %q after the fork, %q when following B from the start", tx, a, b)
		}
	}
	if got := switched.confirmedBy(t, "X200"); got != "B200" {
		t.Errorf("X200 confirmed by %q, want B200", got)
	}
	if got := switched.confirmedBy(t, "A200"); got != "" {
		t.Errorf("A200 confirmed by %q, want none", got)
	}
	for _, q := range []struct{ name, query string }{
		{"canonical lineage", `SELECT t.txid FROM branch b JOIN tx t ON t.id = b.tx WHERE b.canonical_no IS NOT NULL ORDER BY b.canonical_no`},
		{"confirmations", `SELECT t.txid || c.txid FROM tx t JOIN tx c ON c.id = t.confirming_branch ORDER BY t.txid`},
		{"forks", `SELECT tp.txid || coalesce(fp.txid, '') || f.length || coalesce(st.txid, '') FROM fork f JOIN tx tp ON tp.id = f.tip
			LEFT JOIN tx fp ON fp.id = f.fork_point LEFT JOIN tx st ON st.id = f.sealed_tip ORDER BY tp.txid`},
		{"branch forks", `SELECT t.txid || tp.txid FROM branch b JOIN tx t ON t.id = b.tx JOIN fork f ON f.id = b.fork JOIN tx tp ON tp.id = f.tip ORDER BY t.txid`},
		{"sealed tip", `SELECT value FROM meta WHERE key IN ('sealed_tip', 'canonical_tip') ORDER BY key`},
	} {
		a, b := switched.strings(t, q.query), fresh.strings(t, q.query)
		if !slices.Equal(a, b) {
			t.Errorf("%s differ: %d rows after the fork, %d following B from the start\n%q\n%q", q.name, len(a), len(b), a, b)
		}
	}
	// the old lineage above the fork point is one fork from A100, the losing branch L another
	var length int
	must(t, switched.n.queryRow(`SELECT f.length FROM fork f JOIN tx t ON t.id = f.tip WHERE t.txid = ?`, forkTxID("A236")).Scan(&length))
	if length != 136 {
		t.Errorf("fork of the old lineage has length %d, want 136", length)
	}
	must(t, switched.n.queryRow(`SELECT f.length FROM fork f JOIN tx t ON t.id = f.tip WHERE t.txid = ?`, forkTxID("L150")).Scan(&length))
	if length != 50 {
		t.Errorf("fork of L150 has length %d, want 50 (A101..A149 and L150)", length)
	}

	walkedNo, walkedSlot := switched.n.walkedNo, switched.n.walkedSlot
	must(t, switched.n.restore())
	if switched.n.walkedNo != walkedNo || switched.n.walkedSlot != walkedSlot {
		t.Errorf("after a restart the walk stands at %d (slot %d), before at %d (slot %d)",
			switched.n.walkedNo, switched.n.walkedSlot, walkedNo, walkedSlot)
	}
}

type forkNode struct {
	n   *Node
	ids map[string]int64
}

// forkTxID makes a txid for a name such as "A120": the slot from the digits, the rest from the
// whole name, so that each name is one transaction.
func forkTxID(name string) []byte {
	var slot uint32
	_, _ = fmt.Sscanf(name[1:], "%d", &slot)
	id := make([]byte, base.TransactionIDLength)
	binary.BigEndian.PutUint32(id, slot)
	copy(id[6:], name)
	return id
}

func newForkNode(t *testing.T, name string) *forkNode {
	db, err := store.Create(filepath.Join(t.TempDir(), name+".db"))
	must(t, err)
	t.Cleanup(func() { _ = db.Close() })
	codec, err := store.LoadCodec(db)
	must(t, err)
	params := dagstore.DefaultParams
	params.CanonicalDepthSlots = 64
	n := &Node{db: db, params: params, codec: codec, pulls: make(map[base.TransactionID]*pullState),
		gate: ingate.New[base.TransactionID](time.Hour, 1000, 10000)}
	n.branchFetch.started, n.branchFetch.done = true, true
	must(t, n.begin())
	f := &forkNode{n: n, ids: make(map[string]int64)}

	tx := func(name string, kind int) int64 {
		res, err := n.exec(`INSERT INTO tx(txid, slot, tick, kind, state, parsed, sig_checked) VALUES (?, ?, 0, ?, ?, 1, 1)`,
			forkTxID(name), binary.BigEndian.Uint32(forkTxID(name)), kind, store.StateLinked)
		must(t, err)
		id, _ := res.LastInsertId()
		f.ids[name] = id
		return id
	}
	branch := func(name, pred string, endorsed ...string) {
		id := tx(name, store.KindBranch)
		var p any
		if pred != "" {
			p = f.ids[pred]
			_, err := n.exec(`INSERT INTO input(tx, idx, producer, out_idx) VALUES (?, 0, ?, 0)`, id, p)
			must(t, err)
		}
		for i, e := range endorsed {
			_, err := n.exec(`INSERT INTO endorsement(tx, idx, endorsed) VALUES (?, ?, ?)`, id, i, f.ids[e])
			must(t, err)
		}
		_, err := n.exec(`INSERT INTO branch(tx, slot, predecessor) VALUES (?, ?, ?)`, id, binary.BigEndian.Uint32(forkTxID(name)), p)
		must(t, err)
	}
	for s := 1; s <= 300; s++ {
		tx(fmt.Sprintf("X%d", s), store.KindPlain)
		pred := ""
		if s > 1 {
			pred = fmt.Sprintf("A%d", s-1)
		}
		branch(fmt.Sprintf("A%d", s), pred, fmt.Sprintf("X%d", s))
		if s > 100 {
			tx(fmt.Sprintf("Y%d", s), store.KindPlain)
			bp := fmt.Sprintf("B%d", s-1)
			if s == 101 {
				bp = "A100"
			}
			branch(fmt.Sprintf("B%d", s), bp, fmt.Sprintf("X%d", s), fmt.Sprintf("Y%d", s))
		}
		if s == 150 {
			branch("L150", "A149")
		}
	}
	must(t, n.tx.Commit())
	must(t, n.begin())
	return f
}

// follow takes the named branch as the feeders' LRB.
func (f *forkNode) follow(t *testing.T, lrb string) {
	id, err := base.TransactionIDFromBytes(forkTxID(lrb))
	must(t, err)
	f.n.lrb = &id
	must(t, f.n.updateCanonical())
}

// walkAll walks and seals until nothing moves.
func (f *forkNode) walkAll(t *testing.T) {
	for i := 0; i < 100; i++ {
		before := f.n.walkedNo
		must(t, f.n.walkStep())
		must(t, f.n.sealStep())
		must(t, f.n.tx.Commit())
		must(t, f.n.begin())
		if f.n.walkedNo == before && i > 2 {
			return
		}
	}
}

func (f *forkNode) confirmedBy(t *testing.T, name string) string {
	var c sql.NullString
	must(t, f.n.queryRow(`SELECT c.txid FROM tx t LEFT JOIN tx c ON c.id = t.confirming_branch WHERE t.txid = ?`, forkTxID(name)).Scan(&c))
	if !c.Valid {
		return ""
	}
	b := []byte(c.String)
	return string(b[6:][:clen(b[6:])])
}

func clen(b []byte) int {
	for i, c := range b {
		if c == 0 {
			return i
		}
	}
	return len(b)
}

func (f *forkNode) strings(t *testing.T, query string) []string {
	rows, err := f.n.query(query)
	must(t, err)
	defer rows.Close()
	var ret []string
	for rows.Next() {
		var s []byte
		must(t, rows.Scan(&s))
		ret = append(ret, string(s))
	}
	must(t, rows.Err())
	return ret
}
