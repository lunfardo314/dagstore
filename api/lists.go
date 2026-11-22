package api

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/unitrie/common"
)

type branchRow struct {
	tx            int64
	TxID          string `json:"txid"`
	Short         string `json:"short"`
	Slot          uint32 `json:"slot"`
	Sequencer     string `json:"sequencer,omitempty"`
	Predecessor   string `json:"predecessor,omitempty"`
	TotalCoverage uint64 `json:"total_coverage"`
	TotalSupply   uint64 `json:"total_supply"`
	SlotInflation uint64 `json:"slot_inflation"`
	CanonicalNo   int64  `json:"canonical_no,omitempty"`
	Fork          int64  `json:"fork,omitempty"`
	Sealed        bool   `json:"sealed"`
	Gaps          int64  `json:"gaps,omitempty"`
}

const branchSelect = `SELECT b.tx, t.txid, b.slot, ch.chain_id, p.txid, b.total_coverage, b.total_supply, b.slot_inflation,
		b.canonical_no, b.fork, b.sealed, b.gaps
	FROM branch b
	JOIN tx t ON t.id = b.tx
	LEFT JOIN chain ch ON ch.id = b.sequencer
	LEFT JOIN tx p ON p.id = b.predecessor `

func (s *Server) branchRows(ctx context.Context, where string, args ...any) ([]*branchRow, error) {
	rows, err := s.db.QueryContext(ctx, branchSelect+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := make([]*branchRow, 0)
	for rows.Next() {
		var (
			b                 branchRow
			txid, seq, pred   []byte
			cov, supply, infl int64
			canon, fork       sql.NullInt64
		)
		if err = rows.Scan(&b.tx, &txid, &b.Slot, &seq, &pred, &cov, &supply, &infl, &canon, &fork, &b.Sealed, &b.Gaps); err != nil {
			return nil, err
		}
		b.TxID, b.Short = txidStrings(txid)
		if len(seq) > 0 {
			b.Sequencer = hex.EncodeToString(seq)
		}
		if len(pred) > 0 {
			b.Predecessor = hex.EncodeToString(pred)
		}
		// stored as SQLite's signed integers, read back as the uint64 they were
		b.TotalCoverage, b.TotalSupply, b.SlotInflation = uint64(cov), uint64(supply), uint64(infl)
		b.CanonicalNo, b.Fork = canon.Int64, fork.Int64
		ret = append(ret, &b)
	}
	return ret, rows.Err()
}

// meta returns a value of the meta table, nil if absent.
func (s *Server) meta(ctx context.Context, key string) ([]byte, error) {
	var v []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

// canonicalTipSlot is the default slot of the branch and fork lists.
func (s *Server) canonicalTipSlot(ctx context.Context) (uint32, error) {
	v, err := s.meta(ctx, "canonical_tip")
	if err != nil || v == nil {
		return 0, err
	}
	id, err := base.TransactionIDFromBytes(v)
	if err != nil {
		return 0, err
	}
	return id.Slot(), nil
}

// branches lists branches by slot, canonical (canonical=true), non-canonical (false) or all.
func (s *Server) branches(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := s.ctx(r)
	defer cancel()
	tipSlot, err := s.canonicalTipSlot(ctx)
	if err != nil {
		return err
	}
	from, to, err := s.slotRange(r, tipSlot)
	if err != nil {
		return err
	}
	limit, err := s.limit(r, s.cfg.MaxLimit)
	if err != nil {
		return err
	}
	afterSlot, afterTx, err := parsePairCursor(r.URL.Query().Get("after"))
	if err != nil {
		return err
	}
	where := `WHERE b.slot BETWEEN ? AND ? AND (b.slot, b.tx) > (?, ?)`
	switch r.URL.Query().Get("canonical") {
	case "", "all":
	case "true":
		where += ` AND b.canonical_no IS NOT NULL`
	case "false":
		where += ` AND b.canonical_no IS NULL`
	default:
		return badRequest("canonical is true, false or all")
	}
	items, err := s.branchRows(ctx, where+` ORDER BY b.slot, b.tx LIMIT ?`, from, to, afterSlot, afterTx, limit)
	if err != nil {
		return err
	}
	ret := page[*branchRow]{Items: items}
	if len(items) == limit {
		last := items[len(items)-1]
		ret.Next = pairCursor(int64(last.Slot), last.tx)
	}
	return writeJSON(w, ret)
}

type forkRow struct {
	ID          int64  `json:"id"`
	Tip         string `json:"tip"`
	TipSlot     uint32 `json:"tip_slot"`
	ForkPoint   string `json:"fork_point,omitempty"`
	ForkPointNo int64  `json:"fork_point_canonical_no,omitempty"`
	Length      int64  `json:"length"`
	SealedTip   string `json:"sealed_tip,omitempty"`
	tipTx       int64
}

// forks lists fork tips with their fork point and length, in a slot range of the tip, or over the
// whole store when no range is given and min_length is at least 2 (real forks are few; length-1
// forks, the branches that lost their slot, are about one per sequencer per slot).
func (s *Server) forks(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	minLength := 1
	if v := q.Get("min_length"); v != "" {
		var err error
		if minLength, err = strconv.Atoi(v); err != nil || minLength < 1 {
			return badRequest("min_length must be a positive integer")
		}
	}
	limit, err := s.limit(r, s.cfg.MaxLimit)
	if err != nil {
		return err
	}
	afterSlot, afterTx, err := parsePairCursor(q.Get("after"))
	if err != nil {
		return err
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	// A fork's tip branch carries the fork's id, so in a slot range the slot index of the branches
	// leads to the fork rows; over the whole store the fork table is read instead.
	fromClause := `FROM branch tb JOIN fork f ON f.id = tb.fork AND f.tip = tb.tx`
	where := `WHERE f.length >= ? AND (tb.slot, tb.tx) > (?, ?)`
	args := []any{minLength, afterSlot, afterTx}
	if q.Get("from_slot") != "" || q.Get("to_slot") != "" {
		tipSlot, err := s.canonicalTipSlot(ctx)
		if err != nil {
			return err
		}
		from, to, err := s.slotRange(r, tipSlot)
		if err != nil {
			return err
		}
		where += ` AND tb.slot BETWEEN ? AND ?`
		args = append(args, from, to)
	} else if minLength < 2 {
		return badRequest("give from_slot (and to_slot), or min_length of at least 2")
	} else {
		fromClause = `FROM fork f JOIN branch tb ON tb.tx = f.tip`
	}
	query := `SELECT f.id, t.txid, tb.slot, tb.tx, fp.txid, fpb.canonical_no, f.length, st.txid ` + fromClause + `
		JOIN tx t ON t.id = tb.tx
		LEFT JOIN tx fp ON fp.id = f.fork_point
		LEFT JOIN branch fpb ON fpb.tx = f.fork_point
		LEFT JOIN tx st ON st.id = f.sealed_tip ` + where
	rows, err := s.db.QueryContext(ctx, query+` ORDER BY tb.slot, tb.tx LIMIT ?`, append(args, limit)...)
	if err != nil {
		return err
	}
	defer rows.Close()
	ret := page[*forkRow]{Items: make([]*forkRow, 0)}
	for rows.Next() {
		var (
			f                  forkRow
			tip, fp, sealedTip []byte
			fpNo               sql.NullInt64
		)
		if err = rows.Scan(&f.ID, &tip, &f.TipSlot, &f.tipTx, &fp, &fpNo, &f.Length, &sealedTip); err != nil {
			return err
		}
		f.Tip = hex.EncodeToString(tip)
		if len(fp) > 0 {
			f.ForkPoint = hex.EncodeToString(fp)
		}
		f.ForkPointNo = fpNo.Int64
		if len(sealedTip) > 0 {
			f.SealedTip = hex.EncodeToString(sealedTip)
		}
		ret.Items = append(ret.Items, &f)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(ret.Items) == limit {
		last := ret.Items[len(ret.Items)-1]
		ret.Next = pairCursor(int64(last.TipSlot), last.tipTx)
	}
	return writeJSON(w, ret)
}

type outputList struct {
	Account   string       `json:"account,omitempty"`
	Chain     string       `json:"chain,omitempty"`
	ChainKind string       `json:"chain_kind,omitempty"`
	Items     []*outputRow `json:"items"`
	Next      string       `json:"next,omitempty"`
}

// account lists the outputs indexed under an account, with who consumed each, in store order
// (about the order of time). The holder is given in hex, or in the form proxi prints.
func (s *Server) account(w http.ResponseWriter, r *http.Request) error {
	v := r.PathValue("holder")
	if i, j := strings.IndexByte(v, '('), strings.LastIndexByte(v, ')'); i >= 0 && j > i {
		v = v[i+1 : j] // addressED25519(0x...)
	}
	holder, err := decodeHex(v)
	if err != nil || len(holder) == 0 {
		return badRequest("bad account %q: hex expected", r.PathValue("holder"))
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	var id int64
	if err = s.db.QueryRowContext(ctx, `SELECT id FROM account WHERE holder = ?`, holder).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return notFound("no output of account %x is stored", holder)
		}
		return err
	}
	ret, err := s.outputPage(r, ctx, `o.account = ?`, id)
	if err != nil {
		return err
	}
	ret.Account = hex.EncodeToString(holder)
	return writeJSON(w, ret)
}

// chain lists every output of a chain, each of which is one transition: sequencers, delegations,
// the mine chain and any other chain.
func (s *Server) chain(w http.ResponseWriter, r *http.Request) error {
	b, err := decodeHex(r.PathValue("chainID"))
	if err != nil {
		return badRequest("bad chain ID: hex expected")
	}
	chainID, err := base.ChainIDFromBytes(b)
	if err != nil {
		return badRequest("bad chain ID: %v", err)
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	var id, kind int64
	if err = s.db.QueryRowContext(ctx, `SELECT id, kind FROM chain WHERE chain_id = ?`, chainID[:]).Scan(&id, &kind); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return notFound("chain %s is not in the store", chainID.StringShort())
		}
		return err
	}
	ret, err := s.outputPage(r, ctx, `o.chain = ?`, id)
	if err != nil {
		return err
	}
	ret.Chain = chainID.StringHex()
	ret.ChainKind = store.Name(store.ChainKindNames, sql.NullInt64{Int64: kind, Valid: true})
	return writeJSON(w, ret)
}

func (s *Server) outputPage(r *http.Request, ctx context.Context, cond string, arg any) (*outputList, error) {
	limit, err := s.limit(r, s.cfg.MaxLimit)
	if err != nil {
		return nil, err
	}
	afterTx, afterIdx, err := parsePairCursor(r.URL.Query().Get("after"))
	if err != nil {
		return nil, err
	}
	items, err := s.outputs(ctx, `WHERE `+cond+` AND (o.tx, o.idx) > (?, ?) ORDER BY o.tx, o.idx LIMIT ?`, arg, afterTx, afterIdx, limit)
	if err != nil {
		return nil, err
	}
	ret := &outputList{Items: items}
	if len(items) == limit {
		last := items[len(items)-1]
		ret.Next = pairCursor(last.producer, int64(last.Idx))
	}
	return ret, nil
}

type missingRow struct {
	*txRow
	FirstNeeded  *int64  `json:"first_needed,omitempty"`
	Tried        string  `json:"tried,omitempty"`
	ReferencedBy []txRef `json:"referenced_by"`
}

const maxReferencedBy = 10

// missing lists the missing records, pending (state=missing) or lost (state=lost) or both.
func (s *Server) missing(w http.ResponseWriter, r *http.Request) error {
	limit, err := s.limit(r, s.cfg.MaxLimit)
	if err != nil {
		return err
	}
	after := int64(0)
	if v := r.URL.Query().Get("after"); v != "" {
		if after, err = strconv.ParseInt(v, 10, 64); err != nil {
			return badRequest("bad cursor %q", v)
		}
	}
	where := `JOIN missing m ON m.tx = t.id WHERE m.tx > ?`
	args := []any{after}
	switch v := r.URL.Query().Get("state"); v {
	case "":
	case "missing", "lost":
		where += ` AND t.state = ?`
		args = append(args, store.Code(store.StateNames, v))
	default:
		return badRequest("state is missing or lost")
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	rows, err := s.txRows(ctx, where+` ORDER BY m.tx LIMIT ?`, append(args, limit)...)
	if err != nil {
		return err
	}
	ret := page[*missingRow]{Items: make([]*missingRow, 0, len(rows))}
	for _, row := range rows {
		m := &missingRow{txRow: row}
		var first sql.NullInt64
		var tried sql.NullString
		if err = s.db.QueryRowContext(ctx, `SELECT first_needed, tried FROM missing WHERE tx = ?`, row.id).Scan(&first, &tried); err != nil {
			return err
		}
		if first.Valid {
			m.FirstNeeded = &first.Int64
		}
		m.Tried = tried.String
		if m.ReferencedBy, err = s.txRefs(ctx, `SELECT `+refColumns+` FROM input i JOIN tx t ON t.id = i.tx WHERE i.producer = ?1
			UNION SELECT `+refColumns+` FROM endorsement e JOIN tx t ON t.id = e.tx WHERE e.endorsed = ?1 LIMIT ?2`,
			row.id, maxReferencedBy); err != nil {
			return err
		}
		ret.Items = append(ret.Items, m)
	}
	if len(rows) == limit {
		ret.Next = strconv.FormatInt(rows[len(rows)-1].id, 10)
	}
	return writeJSON(w, ret)
}

// export streams the raw bytes of every stored transaction of a slot range in txid order, each as
// a 4-byte big-endian length followed by the bytes. It is meant for bulk transfer, so the range
// is not capped and the query timeout does not apply.
func (s *Server) export(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	if q.Get("from_slot") == "" || q.Get("to_slot") == "" {
		return badRequest("from_slot and to_slot are required")
	}
	from, err := parseSlot(q.Get("from_slot"))
	if err != nil {
		return err
	}
	to, err := parseSlot(q.Get("to_slot"))
	if err != nil {
		return err
	}
	if to < from {
		return badRequest("to_slot is below from_slot")
	}
	rows, err := s.db.QueryContext(r.Context(), `SELECT b.bytes FROM tx t JOIN tx_bytes b ON b.id = t.id
		WHERE t.txid >= ? AND t.txid < ? ORDER BY t.txid`, slotKey(from), slotEndKey(to))
	if err != nil {
		return err
	}
	defer rows.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	bw := bufio.NewWriterSize(w, 1<<16)
	var lenBuf [4]byte
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil // the response has started; the client sees a truncated stream
		}
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(raw)))
		if _, err = bw.Write(lenBuf[:]); err == nil {
			_, err = bw.Write(raw)
		}
		if err != nil {
			return nil
		}
	}
	_ = bw.Flush()
	return nil
}

// explorerStore gives the proxima DAG explorer the read interface it takes over the txstore:
// lookup by txid and iteration by key prefix, here over the stored transactions.
type explorerStore struct {
	db *sql.DB
}

func (e *explorerStore) GetTxBytes(id *base.TransactionID) []byte {
	var raw []byte
	_ = e.db.QueryRow(`SELECT b.bytes FROM tx t JOIN tx_bytes b ON b.id = t.id WHERE t.txid = ?`, id[:]).Scan(&raw)
	return raw
}

func (e *explorerStore) HasTxBytes(id *base.TransactionID) bool {
	return len(e.GetTxBytes(id)) > 0
}

func (e *explorerStore) Iterator(prefix []byte) common.KVIterator {
	return &explorerIterator{db: e.db, prefix: prefix}
}

type explorerIterator struct {
	db     *sql.DB
	prefix []byte
}

func (it *explorerIterator) Iterate(fun func(k, v []byte) bool) {
	it.iterate(true, fun)
}

func (it *explorerIterator) IterateKeys(fun func(k []byte) bool) {
	it.iterate(false, func(k, _ []byte) bool { return fun(k) })
}

// iterate streams the stored transactions whose txid begins with the prefix, in key order, until
// fun returns false.
func (it *explorerIterator) iterate(withValues bool, fun func(k, v []byte) bool) {
	query := `SELECT t.txid, NULL FROM tx t WHERE t.state = ?`
	if withValues {
		query = `SELECT t.txid, b.bytes FROM tx t JOIN tx_bytes b ON b.id = t.id WHERE t.state = ?`
	}
	args := []any{store.StateLinked}
	if len(it.prefix) > 0 {
		query += ` AND t.txid >= ?`
		args = append(args, it.prefix)
		if end := prefixEnd(it.prefix); end != nil {
			query += ` AND t.txid < ?`
			args = append(args, end)
		}
	}
	rows, err := it.db.Query(query+` ORDER BY t.txid`, args...)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var k, v []byte
		if rows.Scan(&k, &v) != nil || !fun(k, v) {
			return
		}
	}
}

// prefixEnd is the smallest key greater than every key with the prefix, nil if there is none.
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
