// Package api serves the dagstore's read-only query API (SPEC.md section 11), its dashboard
// (section 11.1) and the proxima DAG explorer pages, all from the SQLite database.
package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lunfardo314/dagstore"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/api/dag_explorer"
	"github.com/lunfardo314/proxima/ledger/base"
)

// Config bounds what one request may cost: the API is unauthenticated, so every list is paged and
// every scan has a ceiling.
type Config struct {
	DBPath        string          `json:"db_path"`
	QueryTimeout  time.Duration   `json:"query_timeout"`
	MaxRangeSlots uint32          `json:"max_range_slots"` // slot span of one /range, /branches or /forks request
	DefaultLimit  int             `json:"default_limit"`
	MaxLimit      int             `json:"max_limit"`
	MaxConeLimit  int             `json:"max_cone_limit"` // vertices of one cone traversal
	StatsInterval time.Duration   `json:"stats_interval"`
	SlowRequest   time.Duration   `json:"slow_request"` // a request taking longer is logged
	Params        dagstore.Params `json:"params"`
}

var DefaultConfig = Config{
	QueryTimeout:  15 * time.Second,
	MaxRangeSlots: 1000,
	DefaultLimit:  100,
	MaxLimit:      1000,
	MaxConeLimit:  10000,
	StatsInterval: 10 * time.Minute,
	SlowRequest:   2 * time.Second,
	Params:        dagstore.DefaultParams,
}

type Server struct {
	db      *sql.DB
	cfg     Config
	started time.Time
	stats   atomic.Pointer[Stats]
	// Live, when set, gives the live process's state for /status and the dashboard
	Live func() *LiveStatus
	// Libs are the ledger libraries in use, with their alarms
	Libs *Libraries
}

func New(db *sql.DB, cfg Config, libs *Libraries) *Server {
	return &Server{db: db, cfg: cfg, started: time.Now(), Libs: libs}
}

// Handler returns the routes of the query API, the dashboard and the DAG explorer.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	handle := func(pattern string, h func(w http.ResponseWriter, r *http.Request) error) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			if err := h(w, r); err != nil {
				if writeError(w, err) >= 500 {
					slog.Error("request failed", "path", r.URL.RequestURI(), "err", err)
				}
			}
		})
	}
	mux.HandleFunc("GET /{$}", serveDashboard)
	handle("GET /status", s.status)
	handle("GET /stats", s.serveStats)
	handle("GET /tx/{txid}", s.tx)
	handle("GET /output/{oid}", s.output)
	handle("GET /slot/{n}", s.slot)
	handle("GET /range", s.txRange)
	handle("GET /branches", s.branches)
	handle("GET /forks", s.forks)
	handle("GET /past_cone/{txid}", s.pastCone)
	handle("GET /future_cone/{txid}", s.futureCone)
	handle("GET /account/{holder}", s.account)
	handle("GET /chain/{chainID}", s.chain)
	handle("GET /confirmed_in/{branch}", s.confirmedIn)
	handle("GET /missing", s.missing)
	handle("GET /export", s.export)
	dag_explorer.Register(func(path string, h func(http.ResponseWriter, *http.Request)) {
		mux.HandleFunc("GET "+path, h)
	}, &explorerStore{db: s.db})
	return s.logRequests(mux)
}

// httpError carries the status code of a client error; anything else is a 500.
type httpError struct {
	code int
	msg  string
}

func (e *httpError) Error() string { return e.msg }

func badRequest(format string, args ...any) error {
	return &httpError{http.StatusBadRequest, fmt.Sprintf(format, args...)}
}

func notFound(format string, args ...any) error {
	return &httpError{http.StatusNotFound, fmt.Sprintf(format, args...)}
}

// writeError writes the error as JSON and returns the status code it chose.
func writeError(w http.ResponseWriter, err error) int {
	code := http.StatusInternalServerError
	var he *httpError
	switch {
	case errors.As(err, &he):
		code = he.code
	case errors.Is(err, sql.ErrNoRows):
		code = http.StatusNotFound
	case errors.Is(err, context.DeadlineExceeded):
		code = http.StatusServiceUnavailable
		err = fmt.Errorf("query took longer than the limit; narrow the range or lower the limit")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	return code
}

func writeJSON(w http.ResponseWriter, v any) error {
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(v)
}

// ctx bounds the request's queries by the configured timeout.
func (s *Server) ctx(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
}

func (s *Server) limit(r *http.Request, max int) (int, error) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return min(s.cfg.DefaultLimit, max), nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, badRequest("limit must be a positive integer")
	}
	return min(n, max), nil
}

// slotRange reads from_slot and to_slot. A missing from_slot is def; a missing to_slot is from_slot.
func (s *Server) slotRange(r *http.Request, def uint32) (from, to uint32, err error) {
	q := r.URL.Query()
	from, to = def, def
	if v := q.Get("from_slot"); v != "" {
		if from, err = parseSlot(v); err != nil {
			return
		}
		to = from
	}
	if v := q.Get("to_slot"); v != "" {
		if to, err = parseSlot(v); err != nil {
			return
		}
	}
	if to < from {
		return 0, 0, badRequest("to_slot is below from_slot")
	}
	if to-from >= s.cfg.MaxRangeSlots {
		return 0, 0, badRequest("at most %d slots per request", s.cfg.MaxRangeSlots)
	}
	return
}

func parseSlot(v string) (uint32, error) {
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return 0, badRequest("bad slot %q", v)
	}
	return uint32(n), nil
}

func decodeHex(v string) ([]byte, error) {
	return hex.DecodeString(strings.TrimPrefix(strings.TrimPrefix(v, "0x"), "0X"))
}

// parseTxID accepts the hex form of a transaction ID and the dashed form proxima prints.
func parseTxID(v string) (base.TransactionID, error) {
	if b, err := decodeHex(v); err == nil {
		if ret, err := base.TransactionIDFromBytes(b); err == nil {
			return ret, nil
		}
	}
	if ret, err := base.TransactionIDFromStringDashed(v); err == nil {
		return ret, nil
	}
	return base.TransactionID{}, badRequest("bad transaction ID %q: hex or <slot>-<tick>-<hash>", v)
}

// slotKey is the smallest txid key of a slot: txids begin with the big-endian slot.
func slotKey(slot uint32) []byte {
	return base.Slot2Bytes(slot)
}

// slotEndKey is the key past every txid of the slot. The last slot has no next one: its end key is
// longer than any txid and sorts after all of them.
func slotEndKey(slot uint32) []byte {
	if slot == ^uint32(0) {
		return bytes.Repeat([]byte{0xff}, base.TransactionIDLength+1)
	}
	return base.Slot2Bytes(slot + 1)
}

// txRow is a transaction as lists show it. Placeholder rows (missing, lost, pruned) have only the
// ID fields and the state.
type txRow struct {
	id               int64
	TxID             string `json:"txid"`
	Short            string `json:"short"`
	Slot             uint32 `json:"slot"`
	Tick             uint8  `json:"tick"`
	Kind             string `json:"kind,omitempty"`
	State            string `json:"state"`
	Sequencer        string `json:"sequencer,omitempty"`
	Size             int64  `json:"size,omitempty"`
	ConfirmingBranch string `json:"confirming_branch,omitempty"`
	ConfirmingNo     int64  `json:"confirming_canonical_no,omitempty"`
	SigChecked       bool   `json:"sig_checked,omitempty"`
}

// txSelect is the query every txRow is read with; callers append the WHERE clause.
const txSelect = `SELECT t.id, t.txid, t.slot, t.tick, t.kind, t.state, ch.chain_id, t.size, c.txid, cb.canonical_no, t.sig_checked
	FROM tx t
	LEFT JOIN chain ch ON ch.id = t.sequencer
	LEFT JOIN tx c ON c.id = t.confirming_branch
	LEFT JOIN branch cb ON cb.tx = t.confirming_branch `

type scanner interface{ Scan(dest ...any) error }

func scanTxRow(sc scanner) (*txRow, error) {
	var (
		r                  txRow
		txid, seq, conf    []byte
		kind, state, size  sql.NullInt64
		confNo, sigChecked sql.NullInt64
	)
	if err := sc.Scan(&r.id, &txid, &r.Slot, &r.Tick, &kind, &state, &seq, &size, &conf, &confNo, &sigChecked); err != nil {
		return nil, err
	}
	r.TxID, r.Short = txidStrings(txid)
	r.Kind = store.Name(store.KindNames, kind)
	r.State = store.Name(store.StateNames, state)
	if len(seq) > 0 {
		r.Sequencer = hex.EncodeToString(seq)
	}
	r.Size = size.Int64
	if len(conf) > 0 {
		r.ConfirmingBranch = hex.EncodeToString(conf)
	}
	r.ConfirmingNo = confNo.Int64
	r.SigChecked = sigChecked.Int64 != 0
	return &r, nil
}

func txidStrings(b []byte) (full, short string) {
	id, err := base.TransactionIDFromBytes(b)
	if err != nil {
		return hex.EncodeToString(b), ""
	}
	return hex.EncodeToString(b), id.StringShort()
}

func (s *Server) txRows(ctx context.Context, where string, args ...any) ([]*txRow, error) {
	rows, err := s.db.QueryContext(ctx, txSelect+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ret := make([]*txRow, 0)
	for rows.Next() {
		r, err := scanTxRow(rows)
		if err != nil {
			return nil, err
		}
		ret = append(ret, r)
	}
	return ret, rows.Err()
}

// txRowByID reads one transaction by its txid.
func (s *Server) txRowByTxID(ctx context.Context, txid base.TransactionID) (*txRow, error) {
	r, err := scanTxRow(s.db.QueryRowContext(ctx, txSelect+"WHERE t.txid = ?", txid[:]))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, notFound("transaction %s is not in the store", txid.StringShort())
	}
	return r, err
}

// page is the envelope of every paged list: next is the cursor of the following page, passed back
// as `after`; it is empty on the last page.
type page[T any] struct {
	Items []T    `json:"items"`
	Next  string `json:"next,omitempty"`
}

// pairCursor encodes a two-integer position, as "a-b".
func pairCursor(a, b int64) string { return fmt.Sprintf("%d-%d", a, b) }

func parsePairCursor(v string) (a, b int64, err error) {
	if v == "" {
		return 0, -1, nil
	}
	x, y, ok := strings.Cut(v, "-")
	if ok {
		if a, err = strconv.ParseInt(x, 10, 64); err == nil {
			if b, err = strconv.ParseInt(y, 10, 64); err == nil {
				return
			}
		}
	}
	return 0, 0, badRequest("bad cursor %q", v)
}
