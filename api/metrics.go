package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// The metrics of SPEC.md section 12 are read at scrape time off the same views as /status and
// /stats, so they never disagree with the dashboard and the live loop carries no metric code. The
// whole-table figures are those of the latest stats snapshot, as old as dagstore_stats_age_seconds.

const metricsPrefix = "dagstore_"

func desc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(metricsPrefix+name, help, labels, nil)
}

var (
	mCurrentSlot = desc("current_slot", "Current slot by the ledger clock")
	mPointSlot   = desc("slot", "Slot of a point of the store: starting branch, newest sealed canonical branch, canonical tip, the feeders' LRB, the history walker", "point")
	mPointLag    = desc("lag_slots", "Slots the point is behind the current slot", "point")
	mAlarms      = desc("alarms", "Library alarms raised since start")
	mUptime      = desc("uptime_seconds", "Seconds since the process started")

	mFeeders      = desc("feeders", "Feeders declared")
	mFeedersAlive = desc("feeders_alive", "Feeders alive")
	mIntakeQueue  = desc("intake_queue", "Received transactions waiting for the writer")
	mPendingPulls = desc("pending_pulls", "Transactions being pulled")
	mBranchFetch  = desc("branch_fetch_slot", "Branch list fetch: the next slot requested and the slot it runs to", "end")

	mFeederAlive  = desc("feeder_alive", "1 if the feeder is alive", "feeder")
	mFeederLRB    = desc("feeder_lrb_slot", "Slot of the feeder's latest LRB answer", "feeder")
	mFeederOldest = desc("feeder_oldest_slot", "Oldest slot of the feeder's txstore", "feeder")
	mFeederFloor  = desc("feeder_purge_floor", "Purge floor of the feeder's txstore, 0 when it does not purge", "feeder")
	mFeederPush   = desc("feeder_pushes_total", "Transactions pushed by the feeder, copies included", "feeder")
	mFeederPulled = desc("feeder_pulled_in_total", "Pulled transactions the feeder answered", "feeder")
	mFeederPulls  = desc("feeder_pulls_sent_total", "Pulls sent to the feeder", "feeder")
	mFeederTimeo  = desc("feeder_timeouts_total", "Pulls the feeder left unanswered", "feeder")
	mOthers       = desc("other_dagstores", "Dagstores among the peers that are not feeders")
	mReplicaAlive = desc("replica_alive", "1 if the replica is alive", "replica")
	mOtherPulled  = desc("other_dagstore_pulled_in_total", "Pulled transactions the other dagstore answered", "peer")
	mOtherPulls   = desc("other_dagstore_pulls_sent_total", "Pulls sent to the other dagstore", "peer")
	mOtherTimeo   = desc("other_dagstore_timeouts_total", "Pulls the other dagstore left unanswered", "peer")

	mStatsAge     = desc("stats_age_seconds", "Age of the stats snapshot the whole-table metrics come from")
	mTx           = desc("tx", "Transactions by state", "state")
	mTxBytes      = desc("tx_bytes", "Bytes of transactions by state", "state")
	mTxConfirmed  = desc("tx_confirmed", "Transactions confirmed by a canonical branch")
	mBranches     = desc("branches", "Branches stored: all, canonical, sealed, sealed canonical, with gaps", "set")
	mForks        = desc("forks", "Forks stored by length", "length")
	mMissing      = desc("missing", "Missing transactions: pending a pull, or lost", "state")
	mDBBytes      = desc("db_bytes", "Database size: the file, its WAL, and free pages awaiting incremental vacuum", "part")
	mDiskFree     = desc("disk_free_bytes", "Free disk space beside the database")
	mBytesPerTx   = desc("bytes_per_stored_tx", "Database file bytes per stored transaction")
	mHistoryDays  = desc("history_days", "Days of canonical history held")
	mSegments     = desc("archive_segments", "Archive segments the database was pruned to")
	mStatsSeconds = desc("stats_seconds", "Time the latest stats snapshot took")
)

// liveCounters are the live process's counters since start, one metric each.
var liveCounters = []struct {
	d   *prometheus.Desc
	get func(*LiveStatus) int64
}{
	{desc("pushed_total", "Transactions pushed by feeders, copies included"), func(l *LiveStatus) int64 { return l.Pushed }},
	{desc("pulled_in_total", "Pulled transactions received"), func(l *LiveStatus) int64 { return l.PulledIn }},
	{desc("duplicates_total", "Copies dropped as already received"), func(l *LiveStatus) int64 { return l.Duplicates }},
	{desc("not_from_feeders_total", "Transactions dropped as not sent by a feeder"), func(l *LiveStatus) int64 { return l.NotFromFeeders }},
	{desc("bad_bytes_total", "Transactions dropped as not parsing or not matching their txid"), func(l *LiveStatus) int64 { return l.BadBytes }},
	{desc("stored_total", "Transactions stored"), func(l *LiveStatus) int64 { return l.Stored }},
	{desc("linked_total", "Transactions linked by the walk"), func(l *LiveStatus) int64 { return l.Linked }},
	{desc("sig_failed_total", "Transactions whose signature did not verify"), func(l *LiveStatus) int64 { return l.SigFailed }},
	{desc("pulls_sent_total", "Pulls sent to one peer, feeders and other dagstores"), func(l *LiveStatus) int64 { return l.PullsSent }},
	{desc("broad_pulls_total", "Broad pulls, each sent to every alive peer"), func(l *LiveStatus) int64 { return l.BroadPulls }},
	{desc("to_replicas_total", "Transactions passed on to the replicas"), func(l *LiveStatus) int64 { return l.ToReplicas }},
	{desc("lost_total", "Transactions declared lost after the pull rounds"), func(l *LiveStatus) int64 { return l.Lost }},
	{desc("purged_orphans_total", "Orphans purged"), func(l *LiveStatus) int64 { return l.Purged }},
	{desc("version_mismatch_total", "Transactions of a version the dagstore's libraries do not know"), func(l *LiveStatus) int64 { return l.VersionMismatch }},
	{desc("stored_unparsed_total", "Transactions stored unparsed, waiting for a library"), func(l *LiveStatus) int64 { return l.Unparsed }},
	{desc("tip_held_total", "LRB answers on which the feeders did not agree, the tip held"), func(l *LiveStatus) int64 { return l.TipHeld }},
	{desc("renumbered_total", "Real forks: the canonical lineage renumbered from a fork point"), func(l *LiveStatus) int64 { return l.Renumbered }},
}

type collector struct{ s *Server }

// Describe sends nothing: the collector is unchecked, since what it reports depends on the mode
// and on what the store holds (no feeders when read-only, nothing from /stats before its first
// snapshot).
func (c collector) Describe(chan<- *prometheus.Desc) {}

func (c collector) Collect(ch chan<- prometheus.Metric) {
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	counter := func(d *prometheus.Desc, v int64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, float64(v), labels...)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := c.s.currentStatus(ctx)
	if err != nil {
		slog.Warn("metrics: status", "err", err)
		return
	}
	gauge(mCurrentSlot, float64(st.CurrentSlot))
	gauge(mAlarms, float64(len(st.Alarms)))
	gauge(mUptime, time.Since(c.s.started).Seconds())
	for _, p := range []struct {
		name string
		bp   *branchPoint
	}{{"starting", st.StartingBranch}, {"sealed_canonical", st.NewestSealedCanonical}, {"canonical_tip", st.CanonicalTip}, {"feeders_lrb", st.FeedersLRB}} {
		if p.bp != nil {
			gauge(mPointSlot, float64(p.bp.Slot), p.name)
			gauge(mPointLag, float64(p.bp.LagSlots), p.name)
		}
	}
	if l := st.Live; l != nil {
		if l.WalkerSlot != nil {
			gauge(mPointSlot, float64(*l.WalkerSlot), "walker")
			gauge(mPointLag, float64(int64(st.CurrentSlot)-int64(*l.WalkerSlot)), "walker")
		}
		gauge(mFeeders, float64(l.Feeders))
		gauge(mFeedersAlive, float64(l.FeedersAlive))
		gauge(mIntakeQueue, float64(l.Intake))
		gauge(mPendingPulls, float64(l.PendingPulls))
		if l.BranchFetch.Started {
			gauge(mBranchFetch, float64(l.BranchFetch.NextSlot), "next")
			gauge(mBranchFetch, float64(l.BranchFetch.TargetSlot), "target")
		}
		for _, lc := range liveCounters {
			counter(lc.d, lc.get(l))
		}
		gauge(mOthers, float64(len(l.OtherDAGStores)))
		for _, f := range l.Replicas {
			alive := 0.0
			if f.Alive {
				alive = 1
			}
			gauge(mReplicaAlive, alive, f.Peer)
		}
		for _, f := range l.OtherDAGStores {
			counter(mOtherPulled, f.PulledIn, f.Peer)
			counter(mOtherPulls, f.PullsSent, f.Peer)
			counter(mOtherTimeo, f.Timeouts, f.Peer)
		}
		for _, f := range l.FeederList {
			alive := 0.0
			if f.Alive {
				alive = 1
			}
			gauge(mFeederAlive, alive, f.Peer)
			if f.LRBSlot > 0 {
				gauge(mFeederLRB, float64(f.LRBSlot), f.Peer)
			}
			gauge(mFeederOldest, float64(f.OldestSlot), f.Peer)
			gauge(mFeederFloor, float64(f.PurgeFloor), f.Peer)
			counter(mFeederPush, f.Pushes, f.Peer)
			counter(mFeederPulled, f.PulledIn, f.Peer)
			counter(mFeederPulls, f.PullsSent, f.Peer)
			counter(mFeederTimeo, f.Timeouts, f.Peer)
		}
	}
	snap := c.s.stats.Load()
	if snap == nil {
		return
	}
	gauge(mStatsAge, time.Since(snap.ComputedAt).Seconds())
	if took, err := time.ParseDuration(snap.Took); err == nil {
		gauge(mStatsSeconds, took.Seconds())
	}
	for state, cs := range snap.Tx.ByState {
		gauge(mTx, float64(cs.Count), state)
		gauge(mTxBytes, float64(cs.Bytes), state)
	}
	gauge(mTxConfirmed, float64(snap.Tx.Confirmed))
	b := snap.Branches
	gauge(mBranches, float64(b.Total), "all")
	gauge(mBranches, float64(b.Canonical), "canonical")
	gauge(mBranches, float64(b.Sealed), "sealed")
	gauge(mBranches, float64(b.SealedCanonical), "sealed_canonical")
	gauge(mBranches, float64(b.WithGaps), "with_gaps")
	gauge(mForks, float64(snap.Forks.Length1), "1")
	gauge(mForks, float64(snap.Forks.Longer), "longer")
	gauge(mMissing, float64(snap.Missing.Pending), "pending")
	gauge(mMissing, float64(snap.Missing.Lost), "lost")
	d := snap.Database
	gauge(mDBBytes, float64(d.FileBytes), "file")
	gauge(mDBBytes, float64(d.WALBytes), "wal")
	gauge(mDBBytes, float64(d.FreePages*d.PageSize), "free_pages")
	gauge(mDiskFree, float64(d.DiskFreeBytes))
	gauge(mBytesPerTx, d.BytesPerStoredTx)
	gauge(mHistoryDays, d.HistoryDays)
	gauge(mSegments, float64(len(d.ArchiveSegments)))
}

// metricsHandler serves the dagstore's metrics, the Go runtime's and the process's, and those of
// extra, when given.
func (s *Server) metricsHandler(extra prometheus.Gatherer) http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collector{s}, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	gatherers := prometheus.Gatherers{reg}
	if extra != nil {
		gatherers = append(gatherers, extra)
	}
	return promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})
}
