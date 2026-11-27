package api

import (
	"context"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/ledger"
)

// /metrics on a new, empty database, read-only and then with a live status: the whole-table
// metrics appear once a stats snapshot exists, the live ones only with a live process, and every
// scrape is valid Prometheus text (promhttp fails the scrape on a malformed metric).
func TestMetrics(t *testing.T) {
	ledger.InitWithTestingLedgerData()
	path := filepath.Join(t.TempDir(), "metrics.db")
	db, err := store.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := DefaultConfig
	cfg.DBPath = path
	codec, err := store.LoadCodec(db)
	if err != nil {
		t.Fatal(err)
	}
	s := New(db, cfg, &Libraries{alarmIndex: map[string]bool{}}, codec)

	scrape := func() string {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
		if w.Code != 200 {
			t.Fatalf("/metrics: %d", w.Code)
		}
		b, _ := io.ReadAll(w.Body)
		return string(b)
	}
	has := func(text string, lines ...string) {
		t.Helper()
		for _, l := range lines {
			if !strings.Contains(text, l) {
				t.Errorf("missing %q", l)
			}
		}
	}

	// before the first snapshot: the cheap status only, and the runtime's metrics
	text := scrape()
	has(text, "dagstore_current_slot ", "dagstore_alarms 0", "go_goroutines ", "process_open_fds ")
	if strings.Contains(text, "dagstore_branches") || strings.Contains(text, "dagstore_feeders ") {
		t.Errorf("snapshot or live metrics before either exists")
	}

	snap, err := s.computeStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	snap.Took = "1s"
	s.stats.Store(snap)
	walker := uint32(7)
	s.Live = func() *LiveStatus {
		return &LiveStatus{Feeders: 1, FeedersAlive: 1, Pushed: 42, WalkerSlot: &walker,
			FeederList: []FeederStatus{{Peer: "peerA", Alive: true, LRBSlot: 9, Pushes: 40, Timeouts: 3}}}
	}
	text = scrape()
	has(text,
		`dagstore_branches{set="all"} 0`, `dagstore_missing{state="lost"} 0`, `dagstore_db_bytes{part="file"}`, "dagstore_stats_seconds 1",
		"dagstore_feeders 1", "dagstore_pushed_total 42", `dagstore_slot{point="walker"} 7`,
		`dagstore_feeder_alive{feeder="peerA"} 1`, `dagstore_feeder_lrb_slot{feeder="peerA"} 9`, `dagstore_feeder_timeouts_total{feeder="peerA"} 3`)
}
