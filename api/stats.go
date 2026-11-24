package api

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/lunfardo314/dagstore"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger"
)

//go:embed dashboard.html
var dashboardHTML []byte

func serveDashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(dashboardHTML)
}

// branchPoint is one of the positions of the store: starting branch, sealed tip, canonical tip.
type branchPoint struct {
	TxID        string `json:"txid"`
	Short       string `json:"short"`
	Slot        uint32 `json:"slot"`
	CanonicalNo int64  `json:"canonical_no,omitempty"`
	LagSlots    int64  `json:"lag_slots"`
	Lag         string `json:"lag"`
}

// LiveStatus is the live process's state: its feeders, their LRB, the branch list fetch, the
// history walker and the intake counters (cumulative since start).
type LiveStatus struct {
	Feeders         int         `json:"feeders"`
	FeedersAlive    int         `json:"feeders_alive"`
	LRB             string      `json:"lrb,omitempty"`
	LRBSlot         uint32      `json:"lrb_slot,omitempty"`
	BranchFetch     BranchFetch `json:"branch_fetch"`
	WalkerSlot      *uint32     `json:"walker_slot,omitempty"`
	PendingPulls    int         `json:"pending_pulls"`
	Intake          int         `json:"intake_queue"`
	Pushed          int64       `json:"pushed"`
	PulledIn        int64       `json:"pulled_in"`
	Duplicates      int64       `json:"duplicates"`
	NotFromFeeders  int64       `json:"not_from_feeders"`
	BadBytes        int64       `json:"bad_bytes"`
	Stored          int64       `json:"stored"`
	Linked          int64       `json:"linked"`
	SigFailed       int64       `json:"sig_failed"`
	PullsSent       int64       `json:"pulls_sent"`
	Lost            int64       `json:"lost"`
	Purged          int64       `json:"purged_orphans"`
	VersionMismatch int64       `json:"version_mismatch"`
}

type BranchFetch struct {
	Started          bool   `json:"started"`
	Done             bool   `json:"done"`
	NextSlot         uint32 `json:"next_slot"`
	TargetSlot       uint32 `json:"target_slot"`
	FeederOldestSlot uint32 `json:"feeder_oldest_slot"`
	FeederPurgeFloor uint32 `json:"feeder_purge_floor"`
	Entries          int    `json:"entries"`
}

// Status is what /status returns: where the store stands, cheap to compute, plus the counters of
// the latest stats snapshot.
type Status struct {
	Version               string       `json:"version"`
	CommitHash            string       `json:"commit_hash"`
	CommitTime            string       `json:"commit_time"`
	Mode                  string       `json:"mode"`
	CurrentSlot           uint32       `json:"current_slot"`
	StartingBranch        *branchPoint `json:"starting_branch"`
	NewestSealedCanonical *branchPoint `json:"newest_sealed_canonical"`
	CanonicalTip          *branchPoint `json:"canonical_tip"`
	// Filled by the live modules, which do not exist yet: the feeders' LRB, the feeders alive
	// and the history walker's position.
	FeedersLRB    *branchPoint `json:"feeders_lrb"`
	Feeders       []string     `json:"feeders"`
	HistoryWalker *uint32      `json:"history_walker_slot"`
	Live          *LiveStatus  `json:"live,omitempty"`

	Intake         *int64     `json:"intake,omitempty"`
	MissingPending *int64     `json:"missing_pending,omitempty"`
	MissingLost    *int64     `json:"missing_lost,omitempty"`
	LongForks      *int64     `json:"forks_longer_than_1,omitempty"`
	StatsAt        *time.Time `json:"stats_at,omitempty"`
	Uptime         string     `json:"uptime"`
	Alarms         []string   `json:"alarms"`
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) error {
	ctx, cancel := s.ctx(r)
	defer cancel()
	st := &Status{
		Version:     dagstore.Version,
		CommitHash:  dagstore.CommitHash,
		CommitTime:  dagstore.CommitTime,
		Mode:        "read-only: no feeders, no live sync",
		CurrentSlot: ledger.TimeNow().Slot,
		Feeders:     []string{},
		Uptime:      time.Since(s.started).Round(time.Second).String(),
		Alarms:      s.Libs.Alarms(),
	}
	var err error
	for _, p := range []struct {
		key string
		dst **branchPoint
	}{{"starting_branch", &st.StartingBranch}, {"canonical_tip", &st.CanonicalTip}} {
		v, err := s.meta(ctx, p.key)
		if err != nil {
			return err
		}
		if v != nil {
			if *p.dst, err = s.branchPoint(ctx, `WHERE t.txid = ?`, st.CurrentSlot, v); err != nil {
				return err
			}
		}
	}
	if st.NewestSealedCanonical, err = s.branchPoint(ctx, `WHERE b.tx = (SELECT tx FROM branch
		WHERE canonical_no IS NOT NULL AND sealed = 1 ORDER BY canonical_no DESC LIMIT 1)`, st.CurrentSlot); err != nil {
		return err
	}
	if s.Live != nil {
		st.Mode = "live: following feeders"
		st.Live = s.Live()
		st.HistoryWalker = st.Live.WalkerSlot
		if st.Live.LRB != "" {
			if st.FeedersLRB, err = s.branchPoint(ctx, `WHERE t.txid = ?`, st.CurrentSlot, mustHex(st.Live.LRB)); err != nil {
				return err
			}
		}
	}
	if snap := s.stats.Load(); snap != nil {
		st.Intake = ptr(snap.Tx.ByState["intake"].Count)
		st.MissingPending, st.MissingLost = ptr(snap.Missing.Pending), ptr(snap.Missing.Lost)
		st.LongForks = ptr(snap.Forks.Longer)
		st.StatsAt = &snap.ComputedAt
	}
	return writeJSON(w, st)
}

func ptr[T any](v T) *T { return &v }

func mustHex(s string) []byte {
	b, _ := hex.DecodeString(s)
	return b
}

func (s *Server) branchPoint(ctx context.Context, where string, current uint32, args ...any) (*branchPoint, error) {
	var (
		txid  []byte
		slot  uint32
		canon sql.NullInt64
	)
	err := s.db.QueryRowContext(ctx, `SELECT t.txid, t.slot, b.canonical_no FROM branch b JOIN tx t ON t.id = b.tx `+where, args...).
		Scan(&txid, &slot, &canon)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p := &branchPoint{Slot: slot, CanonicalNo: canon.Int64, LagSlots: int64(current) - int64(slot)}
	p.TxID, p.Short = txidStrings(txid)
	p.Lag = (time.Duration(p.LagSlots) * ledger.SlotDuration()).Round(time.Second).String()
	return p, nil
}

type countSize struct {
	Count int64 `json:"count"`
	Bytes int64 `json:"bytes,omitempty"`
}

type tableSize struct {
	Name  string `json:"name"`
	Bytes int64  `json:"bytes"`
}

// Stats is the snapshot /stats returns, recomputed every StatsInterval: the aggregates scan whole
// tables, so they are not computed per request.
type Stats struct {
	ComputedAt time.Time `json:"computed_at"`
	Took       string    `json:"took"`
	Tx         struct {
		ByState   map[string]countSize `json:"by_state"`
		ByKind    map[string]countSize `json:"by_kind"` // stored transactions only
		Confirmed int64                `json:"confirmed"`
	} `json:"tx"`
	Branches struct {
		Total           int64 `json:"total"`
		Canonical       int64 `json:"canonical"`
		Sealed          int64 `json:"sealed"`
		SealedCanonical int64 `json:"sealed_canonical"`
		WithGaps        int64 `json:"with_gaps"`
	} `json:"branches"`
	Forks struct {
		Length1   int64 `json:"length_1"`
		Longer    int64 `json:"longer"`
		MaxLength int64 `json:"max_length"`
	} `json:"forks"`
	Missing struct {
		Pending           int64   `json:"pending"`
		Lost              int64   `json:"lost"`
		OldestPendingSlot *uint32 `json:"oldest_pending_slot,omitempty"`
	} `json:"missing"`
	Dictionaries struct {
		Accounts int64            `json:"accounts"`
		Chains   map[string]int64 `json:"chains"`
	} `json:"dictionaries"`
	Database struct {
		FileBytes          int64       `json:"file_bytes"`
		WALBytes           int64       `json:"wal_bytes"`
		PageSize           int64       `json:"page_size"`
		Pages              int64       `json:"pages"`
		FreePages          int64       `json:"free_pages"`
		DiskFreeBytes      int64       `json:"disk_free_bytes"`
		BytesPerStoredTx   float64     `json:"bytes_per_stored_tx"`
		HistoryDays        float64     `json:"history_days"`
		BytesPerHistoryDay float64     `json:"bytes_per_history_day"`
		Tables             []tableSize `json:"tables"`
	} `json:"database"`
	Libraries []LibraryInfo `json:"libraries"`
	Config    Config        `json:"config"`
}

func (s *Server) serveStats(w http.ResponseWriter, _ *http.Request) error {
	snap := s.stats.Load()
	if snap == nil {
		return &httpError{http.StatusServiceUnavailable, "the first stats snapshot is being computed"}
	}
	return writeJSON(w, snap)
}

// RunStats computes the stats snapshot now and then every StatsInterval, until ctx is done.
func (s *Server) RunStats(ctx context.Context) {
	for {
		t := time.Now()
		snap, err := s.computeStats(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("stats snapshot failed", "err", err)
		} else {
			took := time.Since(t).Round(time.Millisecond)
			snap.Took = took.String()
			snap.Libraries = s.Libs.Info()
			s.stats.Store(snap)
			slog.Info("stats", "stored", snap.Tx.ByState["linked"].Count, "confirmed", snap.Tx.Confirmed,
				"branches", snap.Branches.Total, "missing", snap.Missing.Pending, "lost", snap.Missing.Lost,
				"file", fmt.Sprintf("%.1fGB", float64(snap.Database.FileBytes)/1e9), "took", took)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.cfg.StatsInterval):
		}
	}
}

func (s *Server) computeStats(ctx context.Context) (*Stats, error) {
	st := &Stats{ComputedAt: time.Now().UTC(), Config: s.cfg}
	st.Tx.ByState = make(map[string]countSize)
	st.Tx.ByKind = make(map[string]countSize)
	rows, err := s.db.QueryContext(ctx, `SELECT state, kind, count(*), coalesce(sum(size), 0), sum(confirming_branch IS NOT NULL)
		FROM tx GROUP BY state, kind`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var state, kind sql.NullInt64
		var n, size, confirmed int64
		if err = rows.Scan(&state, &kind, &n, &size, &confirmed); err != nil {
			rows.Close()
			return nil, err
		}
		sn := store.Name(store.StateNames, state)
		c := st.Tx.ByState[sn]
		st.Tx.ByState[sn] = countSize{c.Count + n, c.Bytes + size}
		if state.Int64 == store.StateLinked || state.Int64 == store.StateIntake {
			kn := store.Name(store.KindNames, kind)
			c = st.Tx.ByKind[kn]
			st.Tx.ByKind[kn] = countSize{c.Count + n, c.Bytes + size}
		}
		st.Tx.Confirmed += confirmed
	}
	rows.Close()

	b := &st.Branches
	if err = s.db.QueryRowContext(ctx, `SELECT count(*), coalesce(sum(canonical_no IS NOT NULL), 0), coalesce(sum(sealed), 0),
		coalesce(sum(sealed AND canonical_no IS NOT NULL), 0), coalesce(sum(gaps > 0), 0) FROM branch`).
		Scan(&b.Total, &b.Canonical, &b.Sealed, &b.SealedCanonical, &b.WithGaps); err != nil {
		return nil, err
	}
	f := &st.Forks
	if err = s.db.QueryRowContext(ctx, `SELECT coalesce(sum(length = 1), 0), coalesce(sum(length > 1), 0), coalesce(max(length), 0) FROM fork`).
		Scan(&f.Length1, &f.Longer, &f.MaxLength); err != nil {
		return nil, err
	}
	var oldest sql.NullInt64
	if err = s.db.QueryRowContext(ctx, `SELECT coalesce(sum(t.state = ?1), 0), coalesce(sum(t.state = ?2), 0), min(CASE WHEN t.state = ?1 THEN t.slot END)
		FROM missing m JOIN tx t ON t.id = m.tx`, store.StateMissing, store.StateLost).
		Scan(&st.Missing.Pending, &st.Missing.Lost, &oldest); err != nil {
		return nil, err
	}
	if oldest.Valid {
		st.Missing.OldestPendingSlot = ptr(uint32(oldest.Int64))
	}
	if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM account`).Scan(&st.Dictionaries.Accounts); err != nil {
		return nil, err
	}
	st.Dictionaries.Chains = make(map[string]int64)
	if rows, err = s.db.QueryContext(ctx, `SELECT kind, count(*) FROM chain GROUP BY kind`); err != nil {
		return nil, err
	}
	for rows.Next() {
		var kind sql.NullInt64
		var n int64
		if err = rows.Scan(&kind, &n); err != nil {
			rows.Close()
			return nil, err
		}
		st.Dictionaries.Chains[store.Name(store.ChainKindNames, kind)] = n
	}
	rows.Close()
	if err = s.databaseStats(ctx, st); err != nil {
		return nil, err
	}
	return st, nil
}

func (s *Server) databaseStats(ctx context.Context, st *Stats) error {
	d := &st.Database
	if fi, err := os.Stat(s.cfg.DBPath); err == nil {
		d.FileBytes = fi.Size()
	}
	if fi, err := os.Stat(s.cfg.DBPath + "-wal"); err == nil {
		d.WALBytes = fi.Size()
	}
	var fs syscall.Statfs_t
	if syscall.Statfs(filepath.Dir(s.cfg.DBPath), &fs) == nil {
		d.DiskFreeBytes = int64(fs.Bavail) * fs.Bsize
	}
	for _, p := range []struct {
		pragma string
		dst    *int64
	}{{"page_size", &d.PageSize}, {"page_count", &d.Pages}, {"freelist_count", &d.FreePages}} {
		if err := s.db.QueryRowContext(ctx, "PRAGMA "+p.pragma).Scan(p.dst); err != nil {
			return err
		}
	}
	if stored := st.Tx.ByState["linked"].Count + st.Tx.ByState["intake"].Count; stored > 0 {
		d.BytesPerStoredTx = float64(d.FileBytes) / float64(stored)
	}
	// history held: the canonical lineage from the starting branch to the canonical tip
	var first, last sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT min(slot), max(slot) FROM branch WHERE canonical_no IS NOT NULL`).Scan(&first, &last); err != nil {
		return err
	}
	if first.Valid {
		d.HistoryDays = float64(last.Int64-first.Int64+1) * ledger.SlotDuration().Hours() / 24
		d.BytesPerHistoryDay = float64(d.FileBytes) / d.HistoryDays
	}
	rows, err := s.db.QueryContext(ctx, `SELECT name, pgsize FROM dbstat WHERE aggregate = TRUE ORDER BY pgsize DESC`)
	if err != nil {
		return err
	}
	defer rows.Close()
	d.Tables = make([]tableSize, 0)
	for rows.Next() {
		var t tableSize
		if err = rows.Scan(&t.Name, &t.Bytes); err != nil {
			return err
		}
		d.Tables = append(d.Tables, t)
	}
	return rows.Err()
}
