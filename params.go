// Package dagstore holds what the dagstore's modules share: its parameters. The modules are in
// the subpackages (store, api) and the binary in cmd/dagstore.
package dagstore

import "time"

// Params are the dagstore's tunables, as proposed in SPEC.md section 9.
type Params struct {
	CanonicalDepthSlots     int           `json:"canonical_depth_slots"` // never below 64
	OrphanPurgeSlots        int           `json:"orphan_purge_slots"`
	HeartbeatPeriod         time.Duration `json:"heartbeat_period"`
	BranchListMaxSlots      int           `json:"branch_list_max_slots"`
	PullTimeout             time.Duration `json:"pull_timeout"`
	HistoryPullsPerSecond   int           `json:"history_pulls_per_second"`
	PullsPerSecondPerFeeder int           `json:"pulls_per_second_per_feeder"`
	BroadPullPerSecond      int           `json:"broad_pull_per_second"`
	SourcePullsPerSecond    int           `json:"source_pulls_per_second"`
	MissingRetryRounds      int           `json:"missing_retry_rounds"`
	MissingRetryInterval    time.Duration `json:"missing_retry_interval"`
}

var DefaultParams = Params{
	CanonicalDepthSlots:     120,
	OrphanPurgeSlots:        360,
	HeartbeatPeriod:         30 * time.Second,
	BranchListMaxSlots:      150,
	PullTimeout:             5 * time.Second,
	HistoryPullsPerSecond:   20,
	PullsPerSecondPerFeeder: 5,
	BroadPullPerSecond:      1,
	SourcePullsPerSecond:    200,
	MissingRetryRounds:      3,
	MissingRetryInterval:    10 * time.Minute,
}
