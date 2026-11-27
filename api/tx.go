package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/ledger/transaction"
)

// txRef points at another transaction row.
type txRef struct {
	TxID      string `json:"txid"`
	Short     string `json:"short"`
	State     string `json:"state"`
	Confirmed bool   `json:"confirmed,omitempty"`
}

func scanTxRef(sc scanner, extra ...any) (txRef, error) {
	var (
		txid      []byte
		state     sql.NullInt64
		confirmed bool
	)
	if err := sc.Scan(append([]any{&txid, &state, &confirmed}, extra...)...); err != nil {
		return txRef{}, err
	}
	full, short := txidStrings(txid)
	return txRef{TxID: full, Short: short, State: store.Name(store.StateNames, state), Confirmed: confirmed}, nil
}

// refColumns are the columns scanTxRef reads, from a tx table aliased t.
const refColumns = `t.txid, t.state, t.confirming_branch IS NOT NULL`

func (s *Server) txRefs(ctx context.Context, query string, args ...any) ([]txRef, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := make([]txRef, 0)
	for rows.Next() {
		ref, err := scanTxRef(rows)
		if err != nil {
			return nil, err
		}
		ret = append(ret, ref)
	}
	return ret, rows.Err()
}

type inputRef struct {
	Idx      int    `json:"idx"`
	OutputID string `json:"output_id"`
	Producer txRef  `json:"producer"`
}

type outputRow struct {
	producer   int64
	Idx        int     `json:"idx"`
	OutputID   string  `json:"output_id"`
	Slot       uint32  `json:"slot"`
	Amount     uint64  `json:"amount"`
	Lock       string  `json:"lock,omitempty"`
	Account    string  `json:"account,omitempty"`
	Chain      string  `json:"chain,omitempty"`
	ChainKind  string  `json:"chain_kind,omitempty"`
	ConsumedBy []txRef `json:"consumed_by"`
}

// outputSelect reads outputRows; callers append the WHERE clause.
const outputSelect = `SELECT o.tx, o.idx, p.txid, p.slot, o.amount, lk.name, a.holder, ch.chain_id, ch.kind
	FROM output o
	JOIN tx p ON p.id = o.tx
	LEFT JOIN lock_kind lk ON lk.id = o.lock_kind
	LEFT JOIN account a ON a.id = o.account
	LEFT JOIN chain ch ON ch.id = o.chain `

// outputs reads output rows and, for each, the transactions consuming it: more than one when
// conflicting transactions are both stored, for example in the cones of competing branches.
func (s *Server) outputs(ctx context.Context, where string, args ...any) ([]*outputRow, error) {
	rows, err := s.db.QueryContext(ctx, outputSelect+where, args...)
	if err != nil {
		return nil, err
	}
	ret := make([]*outputRow, 0)
	for rows.Next() {
		var (
			o                     outputRow
			txid, holder, chainID []byte
			lock                  sql.NullString
			amount                int64
			chainKind             sql.NullInt64
		)
		if err = rows.Scan(&o.producer, &o.Idx, &txid, &o.Slot, &amount, &lock, &holder, &chainID, &chainKind); err != nil {
			rows.Close()
			return nil, err
		}
		o.Amount = uint64(amount)
		o.OutputID = hex.EncodeToString(txid) + fmt.Sprintf("%02x", o.Idx)
		o.Lock = lock.String
		if len(holder) > 0 {
			o.Account = hex.EncodeToString(holder)
		}
		if len(chainID) > 0 {
			o.Chain = hex.EncodeToString(chainID)
			o.ChainKind = store.Name(store.ChainKindNames, chainKind)
		}
		ret = append(ret, &o)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for _, o := range ret {
		if o.ConsumedBy, err = s.txRefs(ctx, `SELECT `+refColumns+` FROM input i JOIN tx t ON t.id = i.tx
			WHERE i.producer = ? AND i.out_idx = ?`, o.producer, o.Idx); err != nil {
			return nil, err
		}
	}
	return ret, nil
}

type txDetail struct {
	*txRow
	Source         string       `json:"source,omitempty"`
	ArchiveSegment string       `json:"archive_segment,omitempty"` // a pruned transaction: the segment holding it
	Inputs         []inputRef   `json:"inputs"`
	Endorsements   []txRef      `json:"endorsements"`
	Baseline       *txRef       `json:"baseline,omitempty"`
	Outputs        []*outputRow `json:"outputs"`
	EndorsedBy     []txRef      `json:"endorsed_by"`
	Branch         *branchRow   `json:"branch,omitempty"`
	Hex            string       `json:"hex,omitempty"`
	Parsed         string       `json:"parsed,omitempty"`
	Decompiled     string       `json:"decompiled,omitempty"`
	ParseError     string       `json:"parse_error,omitempty"`
}

// maxEndorsedBy caps the endorsers listed with a transaction; a branch can have thousands.
const maxEndorsedBy = 100

func (s *Server) tx(w http.ResponseWriter, r *http.Request) error {
	txid, err := parseTxID(r.PathValue("txid"))
	if err != nil {
		return err
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	row, err := s.txRowByTxID(ctx, txid)
	if err != nil {
		return err
	}
	d := &txDetail{txRow: row}
	var source sql.NullInt64
	if err = s.db.QueryRowContext(ctx, `SELECT source FROM tx WHERE id = ?`, row.id).Scan(&source); err != nil {
		return err
	}
	d.Source = store.Name(store.SourceNames, source)
	if row.State == store.StateNames[store.StatePruned] {
		d.ArchiveSegment = s.archiveSegment(row.Slot)
	}

	rows, err := s.db.QueryContext(ctx, `SELECT `+refColumns+`, i.idx, i.out_idx FROM input i JOIN tx t ON t.id = i.producer
		WHERE i.tx = ? ORDER BY i.idx`, row.id)
	if err != nil {
		return err
	}
	d.Inputs = make([]inputRef, 0)
	for rows.Next() {
		var in inputRef
		var outIdx int
		if in.Producer, err = scanTxRef(rows, &in.Idx, &outIdx); err != nil {
			rows.Close()
			return err
		}
		in.OutputID = in.Producer.TxID + fmt.Sprintf("%02x", outIdx)
		d.Inputs = append(d.Inputs, in)
	}
	rows.Close()
	if d.Endorsements, err = s.txRefs(ctx, `SELECT `+refColumns+` FROM endorsement e JOIN tx t ON t.id = e.endorsed
		WHERE e.tx = ? ORDER BY e.idx`, row.id); err != nil {
		return err
	}
	if bl, err := s.txRefs(ctx, `SELECT `+refColumns+` FROM tx x JOIN tx t ON t.id = x.baseline WHERE x.id = ?`, row.id); err != nil {
		return err
	} else if len(bl) > 0 {
		d.Baseline = &bl[0]
	}
	if d.Outputs, err = s.outputs(ctx, `WHERE o.tx = ? ORDER BY o.idx`, row.id); err != nil {
		return err
	}
	if d.EndorsedBy, err = s.txRefs(ctx, `SELECT `+refColumns+` FROM endorsement e JOIN tx t ON t.id = e.tx
		WHERE e.endorsed = ? LIMIT ?`, row.id, maxEndorsedBy); err != nil {
		return err
	}
	if row.Kind == "branch" {
		if b, err := s.branchRows(ctx, `WHERE b.tx = ?`, row.id); err != nil {
			return err
		} else if len(b) > 0 {
			d.Branch = b[0]
		}
	}
	raw, err := s.txBytes(ctx, row.id)
	if err != nil {
		return err
	}
	if raw != nil {
		d.Hex = hex.EncodeToString(raw)
		d.Parsed, d.Decompiled, err = s.render(ctx, raw, row.Slot)
		if err != nil {
			d.ParseError = err.Error()
		}
	}
	return writeJSON(w, d)
}

// txBytes returns the stored bytes of the row, nil for a placeholder.
func (s *Server) txBytes(ctx context.Context, id int64) ([]byte, error) {
	var data []byte
	var dict sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT bytes, dict FROM tx_bytes WHERE id = ?`, id).Scan(&data, &dict)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.Codec.Decode(dict, data)
}

func (s *Server) txBytesByTxID(ctx context.Context, txid base.TransactionID) ([]byte, error) {
	var data []byte
	var dict sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT b.bytes, b.dict FROM tx t JOIN tx_bytes b ON b.id = t.id WHERE t.txid = ?`, txid[:]).Scan(&data, &dict)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return s.Codec.Decode(dict, data)
}

// render prints the transaction with the library valid at its slot, in human-readable form and as
// decompiled source, with the consumed outputs when every producer is stored. The printers read
// what partial-context validation sets up; it is run for display only, never to decide what is
// stored, and a failure is reported beside the text.
func (s *Server) render(ctx context.Context, raw []byte, slot uint32) (parsed, decompiled string, err error) {
	parse := func() (*transaction.Transaction, error) {
		tx, err := transaction.ParseWithPartialValidation(raw)
		if tx == nil {
			return nil, err
		}
		if err != nil {
			err = fmt.Errorf("partial-context validation: %w", err)
		}
		return tx, err
	}
	tx, err := parse()
	if tx == nil {
		return "", "", err
	}
	ctxErr := tx.SetFullContext(func(i byte) ([]byte, error) {
		oid := tx.MustInputAt(i)
		praw, err := s.txBytesByTxID(ctx, oid.TransactionID())
		if err != nil || praw == nil {
			return nil, fmt.Errorf("producer of input %d is not stored", i)
		}
		ptx, err := transaction.Parse(praw)
		if err != nil {
			return nil, err
		}
		o, err := ptx.ProducedOutputAt(oid.Index())
		if err != nil {
			return nil, err
		}
		return o.Bytes(), nil
	})
	if ctxErr != nil {
		// printed without the consumed outputs
		tx, _ = parse()
	}
	lib := ledger.L(slot)
	return tx.LinesHRWithLib(lib).String(), tx.LinesSourceWithLib(lib).String(), err
}

type outputDetail struct {
	*outputRow
	Producer *txRow `json:"producer"`
	Parsed   string `json:"parsed,omitempty"`
}

func (s *Server) output(w http.ResponseWriter, r *http.Request) error {
	b, err := decodeHex(r.PathValue("oid"))
	if err != nil {
		return badRequest("bad output ID: hex expected")
	}
	oid, err := base.OutputIDFromBytes(b)
	if err != nil {
		return badRequest("bad output ID: %v", err)
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	producer, err := s.txRowByTxID(ctx, oid.TransactionID())
	if err != nil {
		return err
	}
	outs, err := s.outputs(ctx, `WHERE o.tx = ? AND o.idx = ?`, producer.id, oid.Index())
	if err != nil {
		return err
	}
	if len(outs) == 0 {
		return notFound("output %s is not in the store", oid.StringShort())
	}
	d := &outputDetail{outputRow: outs[0], Producer: producer}
	if raw, err := s.txBytes(ctx, producer.id); err != nil {
		return err
	} else if raw != nil {
		if tx, err := transaction.Parse(raw); err == nil {
			if o, err := tx.ProducedOutputAt(oid.Index()); err == nil {
				d.Parsed = o.LinesHR().String()
			}
		}
	}
	return writeJSON(w, d)
}

func (s *Server) slot(w http.ResponseWriter, r *http.Request) error {
	n, err := parseSlot(r.PathValue("n"))
	if err != nil {
		return err
	}
	return s.listRange(w, r, n, n)
}

func (s *Server) txRange(w http.ResponseWriter, r *http.Request) error {
	if r.URL.Query().Get("from_slot") == "" {
		return badRequest("from_slot is required")
	}
	from, to, err := s.slotRange(r, 0)
	if err != nil {
		return err
	}
	return s.listRange(w, r, from, to)
}

// listRange lists the transactions of a slot range in txid order, which is time order, filtered by
// kind and by status: a state name, "confirmed" or "unconfirmed" (linked, on no canonical cone).
func (s *Server) listRange(w http.ResponseWriter, r *http.Request, from, to uint32) error {
	q := r.URL.Query()
	limit, err := s.limit(r, s.cfg.MaxLimit)
	if err != nil {
		return err
	}
	where := `WHERE t.txid >= ? AND t.txid < ?`
	args := []any{slotKey(from), slotEndKey(to)}
	if v := q.Get("after"); v != "" {
		after, err := parseTxID(v)
		if err != nil {
			return err
		}
		where += ` AND t.txid > ?`
		args = append(args, after[:])
	}
	if v := q.Get("kind"); v != "" {
		k := store.Code(store.KindNames, v)
		if k < 0 {
			return badRequest("kind is one of %v", store.KindNames)
		}
		where += ` AND t.kind = ?`
		args = append(args, k)
	}
	switch v := q.Get("status"); v {
	case "":
	case "confirmed":
		where += ` AND t.confirming_branch IS NOT NULL`
	case "unconfirmed":
		where += ` AND t.state = ? AND t.confirming_branch IS NULL`
		args = append(args, store.StateLinked)
	default:
		st := store.Code(store.StateNames, v)
		if st < 0 {
			return badRequest("status is confirmed, unconfirmed or one of %v", store.StateNames)
		}
		where += ` AND t.state = ?`
		args = append(args, st)
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	items, err := s.txRows(ctx, where+` ORDER BY t.txid LIMIT ?`, append(args, limit)...)
	if err != nil {
		return err
	}
	ret := page[*txRow]{Items: items}
	if len(items) == limit {
		ret.Next = items[len(items)-1].TxID
	}
	return writeJSON(w, ret)
}

type coneVertex struct {
	*txRow
	Depth int `json:"depth"`
}

type coneEdge struct {
	From string `json:"from"` // the dependent transaction
	To   string `json:"to"`   // its dependency
	Type string `json:"type"` // input, endorsement or baseline
}

type cone struct {
	Root      string       `json:"root"`
	Vertices  []coneVertex `json:"vertices"`
	Edges     []coneEdge   `json:"edges"`
	Truncated bool         `json:"truncated,omitempty"`
}

const (
	pastNeighbours = `SELECT producer AS n, 'input' AS typ FROM input WHERE tx = ?1
		UNION SELECT endorsed, 'endorsement' FROM endorsement WHERE tx = ?1
		UNION SELECT baseline, 'baseline' FROM tx WHERE id = ?1 AND baseline IS NOT NULL`
	// Baselines have no reverse index, so the future cone follows inputs and endorsements only: a
	// transaction linked to the walk by its baseline alone is missed.
	futureNeighbours = `SELECT tx AS n, 'input' AS typ FROM input WHERE producer = ?1
		UNION SELECT tx, 'endorsement' FROM endorsement WHERE endorsed = ?1`
)

func (s *Server) pastCone(w http.ResponseWriter, r *http.Request) error {
	return s.cone(w, r, pastNeighbours, false)
}

func (s *Server) futureCone(w http.ResponseWriter, r *http.Request) error {
	return s.cone(w, r, futureNeighbours, true)
}

// cone walks breadth first from the root to the given depth, until limit vertices are reached.
// Edges always point from the dependent transaction to its dependency, in both directions of walk.
func (s *Server) cone(w http.ResponseWriter, r *http.Request, neighbours string, future bool) error {
	txid, err := parseTxID(r.PathValue("txid"))
	if err != nil {
		return err
	}
	depth := 3
	if v := r.URL.Query().Get("depth"); v != "" {
		if depth, err = strconv.Atoi(v); err != nil || depth < 0 {
			return badRequest("depth must be a non-negative integer")
		}
		depth = min(depth, 100)
	}
	limit, err := s.limit(r, s.cfg.MaxConeLimit)
	if err != nil {
		return err
	}
	if r.URL.Query().Get("limit") == "" {
		limit = 500
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	root, err := s.txRowByTxID(ctx, txid)
	if err != nil {
		return err
	}
	ret := &cone{Root: root.TxID, Vertices: []coneVertex{{root, 0}}, Edges: make([]coneEdge, 0)}
	hexOf := map[int64]string{root.id: root.TxID}
	for i := 0; i < len(ret.Vertices); i++ {
		v := ret.Vertices[i]
		if v.Depth >= depth {
			continue
		}
		rows, err := s.db.QueryContext(ctx, neighbours, v.id)
		if err != nil {
			return err
		}
		type nb struct {
			id  int64
			typ string
		}
		var nbs []nb
		for rows.Next() {
			var n nb
			if err = rows.Scan(&n.id, &n.typ); err != nil {
				rows.Close()
				return err
			}
			nbs = append(nbs, n)
		}
		rows.Close()
		for _, n := range nbs {
			if _, seen := hexOf[n.id]; !seen {
				if len(ret.Vertices) >= limit {
					ret.Truncated = true
					continue
				}
				row, err := scanTxRow(s.db.QueryRowContext(ctx, txSelect+`WHERE t.id = ?`, n.id))
				if err != nil {
					return err
				}
				hexOf[n.id] = row.TxID
				ret.Vertices = append(ret.Vertices, coneVertex{row, v.Depth + 1})
			}
			e := coneEdge{From: v.TxID, To: hexOf[n.id], Type: n.typ}
			if future {
				e.From, e.To = e.To, e.From
			}
			ret.Edges = append(ret.Edges, e)
		}
	}
	return writeJSON(w, ret)
}

type confirmedIn struct {
	Branch    *txRow   `json:"branch"`
	Items     []*txRow `json:"items"`
	Truncated bool     `json:"truncated,omitempty"`
}

// confirmedIn lists the transactions a canonical branch confirmed. They are found by walking the
// branch's past cone through transactions with this confirming branch only: everything a branch
// newly confirms is reached from it through transactions it newly confirms, so no index is needed.
func (s *Server) confirmedIn(w http.ResponseWriter, r *http.Request) error {
	txid, err := parseTxID(r.PathValue("branch"))
	if err != nil {
		return err
	}
	limit, err := s.limit(r, s.cfg.MaxConeLimit)
	if err != nil {
		return err
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	b, err := s.txRowByTxID(ctx, txid)
	if err != nil {
		return err
	}
	if b.Kind != "branch" || b.ConfirmingBranch != b.TxID {
		return badRequest("%s is not a canonical branch", b.Short)
	}
	ret := &confirmedIn{Branch: b, Items: []*txRow{b}}
	seen := map[int64]bool{b.id: true}
	for i := 0; i < len(ret.Items); i++ {
		rows, err := s.db.QueryContext(ctx, `SELECT d.n FROM (`+pastNeighbours+`) d
			JOIN tx t ON t.id = d.n WHERE t.confirming_branch = ?2`, ret.Items[i].id, b.id)
		if err != nil {
			return err
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		for _, id := range ids {
			if seen[id] {
				continue
			}
			if len(ret.Items) >= limit {
				ret.Truncated = true
				break
			}
			seen[id] = true
			row, err := scanTxRow(s.db.QueryRowContext(ctx, txSelect+`WHERE t.id = ?`, id))
			if err != nil {
				return err
			}
			ret.Items = append(ret.Items, row)
		}
	}
	sort.Slice(ret.Items, func(i, j int) bool { return bytes.Compare([]byte(ret.Items[i].TxID), []byte(ret.Items[j].TxID)) < 0 })
	return writeJSON(w, ret)
}
