// Package live is the dagstore following the network: intake of feeders' pushes (SPEC.md section
// 5), the tip and the branch table (6.2), the history walk with its pulls and sealing (6.3, 6.4)
// and the heartbeat (7.2). One goroutine owns the database's write connection and does all of it;
// the callbacks of peering only hand messages over to it.
package live

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/lunfardo314/dagstore"
	"github.com/lunfardo314/dagstore/api"
	"github.com/lunfardo314/dagstore/store"
	"github.com/lunfardo314/proxima/api/client"
	"github.com/lunfardo314/proxima/global"
	"github.com/lunfardo314/proxima/ledger"
	"github.com/lunfardo314/proxima/ledger/base"
	"github.com/lunfardo314/proxima/ledger/transaction"
	"github.com/lunfardo314/proxima/ledger/txbuildercore"
	"github.com/lunfardo314/proxima/peering"
	"github.com/lunfardo314/proxima/util/ingate"
)

// commitPeriod is how often the writer commits: readers see the store's changes this much later.
const commitPeriod = time.Second

type Node struct {
	db         *sql.DB // the write connection, used by the loop goroutine only
	rdb        *sql.DB // read connections, for answering peers' pulls
	tx         *sql.Tx
	stmts      map[string]*sql.Stmt
	params     dagstore.Params
	env        *global.Global
	peers      *peering.Peers
	feeders    []*feeder
	feederByID map[peer.ID]*feeder
	gate       *ingate.Gate[base.TransactionID]

	in         chan received
	lineage    chan any // lineage replies of the feeders and fetched libraries, for the loop
	libs       *api.Libraries
	libraryAPI string

	// state of the loop goroutine
	startSlot        uint32 // slot of the starting branch, 0 until it is known
	walkedNo         int64  // the newest canonical branch walked, and its slot
	walkedSlot       uint32
	lrb              *base.TransactionID
	lrbAskedAt       time.Time
	noMajorityLogged time.Time
	branchFetch      branchFetch
	pulls            map[base.TransactionID]*pullState

	mu   sync.Mutex // guards info, read by the API
	info api.LiveStatus
}

// received is a transaction from a feeder, parsed, or only its bytes when they are of a library
// the dagstore does not hold yet (tx nil): those are stored unparsed and parsed after the restart
// that loads the library.
type received struct {
	from   peer.ID
	tx     *transaction.Transaction
	txid   base.TransactionID
	raw    []byte
	pulled bool
}

// Open opens (or creates) the database at path and loads the ledger libraries: from the database,
// or, for a new one, from the node API at libraryAPI with their proofs, then stored.
func Open(path, libraryAPI string) (*sql.DB, *api.Libraries, error) {
	var db *sql.DB
	var err error
	_, statErr := os.Stat(path)
	created := os.IsNotExist(statErr)
	if created {
		db, err = store.Create(path)
	} else {
		db, err = store.Open(path)
	}
	if err != nil {
		return nil, nil, err
	}
	var version []byte
	if err = db.QueryRow(`SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&version); err != nil {
		return nil, nil, err
	}
	if string(version) != store.SchemaVersion {
		return nil, nil, fmt.Errorf("%s has schema version %s, this build writes %s", path, version, store.SchemaVersion)
	}
	if created || !hasLibraries(db) {
		if libraryAPI == "" {
			return nil, nil, fmt.Errorf("a new database needs the ledger libraries: dagstore.library_api is not set")
		}
		libs, err := fetchLibraries(libraryAPI)
		if err != nil {
			return nil, nil, err
		}
		if err = store.SaveLibraries(db, libs); err != nil {
			return nil, nil, err
		}
		slog.Info("libraries fetched with proof", "count", len(libs), "from", libraryAPI)
	}
	libs, err := api.LoadLedger(db)
	return db, libs, err
}

func hasLibraries(db *sql.DB) bool {
	var n int
	return db.QueryRow(`SELECT count(*) FROM library`).Scan(&n) == nil && n > 0
}

// fetchLibraries reads the latest library from a node API and walks back through each one's
// predecessor to genesis. Each comes with the node's proof that a branch commits to it, checked
// here, so nothing rests on trusting the node.
func fetchLibraries(apiURL string) ([]store.Library, error) {
	c := client.NewWithGoogleDNS(apiURL)
	var ret []store.Library
	var slot *uint32
	for {
		def, err := c.GetLedgerDefinition(slot)
		if err != nil {
			return nil, err
		}
		lib, _, err := txbuildercore.LibraryFromLedgerDefinition(&def.LedgerDefinitionJSON)
		if err != nil {
			return nil, fmt.Errorf("library of slot %d: %w", def.UpgradeSlot, err)
		}
		h := lib.LibraryHash()
		ret = append([]store.Library{{ActivationSlot: def.UpgradeSlot, Hash: h[:], JSON: []byte(def.LibraryJSON)}}, ret...)
		if def.UpgradeSlot == 0 {
			break
		}
		prev := def.PrevUpgradeSlot
		slot = &prev
	}
	genesis, err := ledger.ParseLibraryFromJSON(ret[0].JSON, ledger.GetEmbeddedFunctionResolver)
	if err != nil {
		return nil, err
	}
	// the upgrade index counts the upgrades the genesis folds
	folded := ledger.FoldedUpgrades(genesis)
	for i := range ret {
		ret[i].UpgradeIndex = uint16(i)
		if i > 0 {
			ret[i].UpgradeIndex += uint16(folded)
		}
	}
	return ret, nil
}

// libraryRefresh is how often the network's libraries are fetched and compared (SPEC.md section
// 7.1): an upgrade activates at a slot known long before, so minutes are soon enough.
const libraryRefresh = 10 * time.Minute

type librariesFetched struct {
	libs []store.Library
	err  error
}

// refreshLibraries fetches the network's libraries with their proofs, off the loop: the result
// comes back on the lineage channel.
func (n *Node) refreshLibraries() {
	go func() {
		libs, err := fetchLibraries(n.libraryAPI)
		n.lineage <- librariesFetched{libs, err}
	}()
}

// onLibraries stores the fetched libraries this build makes the same or does not list, and raises
// the alarms Reconcile finds.
func (n *Node) onLibraries(r librariesFetched) error {
	if r.err != nil {
		slog.Warn("libraries not fetched", "from", n.libraryAPI, "err", r.err)
		return nil
	}
	toStore, alarms := api.Reconcile(n.libs.Stored(), n.libs.Built(), n.libs.BuiltHash(), r.libs)
	for _, l := range toStore {
		if _, err := n.exec(`INSERT OR REPLACE INTO library(upgrade_index, activation_slot, hash, json) VALUES (?,?,?,?)`,
			l.UpgradeIndex, l.ActivationSlot, l.Hash, l.JSON); err != nil {
			return err
		}
		slog.Info("library stored, proven", "upgrade", l.UpgradeIndex, "slot", l.ActivationSlot)
	}
	n.libs.Added(toStore)
	for _, a := range alarms {
		slog.Error("ALARM", "msg", a)
		n.libs.Alarm(a)
	}
	return nil
}

// New sets up peering as a dagstore and the intake. The ledger libraries must be loaded.
func New(db, readDB *sql.DB, env *global.Global, params dagstore.Params, libs *api.Libraries, libraryAPI string) (*Node, error) {
	cfg, err := peering.ReadConfig()
	if err != nil {
		return nil, err
	}
	cfg.IsDAGStore = true
	if len(cfg.Feeders) == 0 {
		return nil, fmt.Errorf("peering.feeders: no feeder declared")
	}
	peers, err := peering.New(env, cfg)
	if err != nil {
		return nil, err
	}
	n := &Node{
		db:         db,
		rdb:        readDB,
		params:     params,
		env:        env,
		peers:      peers,
		gate:       ingate.New[base.TransactionID](time.Hour, 100_000, 1_000_000),
		in:         make(chan received, 10_000),
		lineage:    make(chan any, 100),
		libs:       libs,
		libraryAPI: libraryAPI,
		pulls:      make(map[base.TransactionID]*pullState),
		feederByID: make(map[peer.ID]*feeder),
	}
	for _, name := range cfg.Feeders {
		info, err := peer.AddrInfoFromP2pAddr(cfg.PreConfiguredPeers[name].Multiaddr)
		if err != nil {
			return nil, fmt.Errorf("peering.feeders: '%s': %w", name, err)
		}
		fd := &feeder{id: info.ID}
		n.feeders = append(n.feeders, fd)
		n.feederByID[info.ID] = fd
	}
	peers.OnReceiveTxBytes(n.onTxBytes)
	peers.OnReceivePullTxRequest(n.onPullRequest)
	peers.OnLRBReply(func(from peer.ID, r peering.LRBReply) { n.lineage <- lrbMsg{from, r} })
	peers.OnBranchListReply(func(from peer.ID, r peering.BranchListReply) { n.lineage <- branchListMsg{from, r} })
	return n, nil
}

// Run starts peering and runs the loop until ctx is done.
func (n *Node) Run(ctx context.Context) error {
	n.peers.Run()
	defer n.peers.Stop()
	if err := n.begin(); err != nil {
		return err
	}
	if err := n.restore(); err != nil {
		return err
	}
	if err := n.parsePending(); err != nil {
		return err
	}
	n.commitOrDie()
	slog.Info("live", "feeders", len(n.feeders), "starting_slot", n.startSlot)

	commit := time.NewTicker(commitPeriod)
	defer commit.Stop()
	lrbPoll := time.NewTicker(time.Minute)
	defer lrbPoll.Stop()
	heartbeat := time.NewTicker(n.params.HeartbeatPeriod)
	defer heartbeat.Stop()
	// pulls within both budgets: overall and per feeder
	pullRate := max(1, min(n.params.HistoryPullsPerSecond, n.params.PullsPerSecondPerFeeder*len(n.feeders)))
	pullTick := time.NewTicker(time.Second / time.Duration(pullRate))
	defer pullTick.Stop()
	report := time.NewTicker(time.Minute)
	defer report.Stop()
	purge := time.NewTicker(time.Minute)
	defer purge.Stop()
	libTimer := time.NewTimer(30 * time.Second)
	defer libTimer.Stop()

	n.pollLRB()
	for {
		var err error
		select {
		case <-ctx.Done():
			return n.tx.Commit()
		case r := <-n.in:
			err = n.intake(r)
		case m := <-n.lineage:
			err = n.onLineage(m)
		case <-commit.C:
			err = n.step()
		case <-pullTick.C:
			err = n.pullOne()
		case <-lrbPoll.C:
			n.pollLRB()
		case <-heartbeat.C:
			n.sendHeartbeat()
		case <-report.C:
			n.logReport()
		case <-purge.C:
			err = n.purgeOrphans()
		case <-libTimer.C:
			if n.libraryAPI != "" {
				n.refreshLibraries()
			}
			libTimer.Reset(libraryRefresh)
		}
		if err != nil {
			return err
		}
	}
}

// step is the periodic work: advance the branch list fetch and the history walk, seal, commit.
func (n *Node) step() error {
	if n.lrb == nil && time.Since(n.lrbAskedAt) > 5*time.Second {
		n.pollLRB() // the first request may have gone out before the feeders were connected
	}
	if err := n.fetchBranchesStep(); err != nil {
		return err
	}
	if err := n.walkStep(); err != nil {
		return err
	}
	if err := n.sealStep(); err != nil {
		return err
	}
	if err := n.tx.Commit(); err != nil {
		return err
	}
	if err := n.begin(); err != nil {
		return err
	}
	n.updateInfo()
	return nil
}

func (n *Node) commitOrDie() {
	if err := n.tx.Commit(); err != nil {
		panic(err)
	}
	if err := n.begin(); err != nil {
		panic(err)
	}
}

func (n *Node) begin() error {
	var err error
	if n.tx, err = n.db.Begin(); err != nil {
		return err
	}
	n.stmts = make(map[string]*sql.Stmt)
	return nil
}

// exec runs a statement in the current write transaction, preparing it once per transaction.
func (n *Node) exec(query string, args ...any) (sql.Result, error) {
	st, err := n.stmt(query)
	if err != nil {
		return nil, err
	}
	return st.Exec(args...)
}

func (n *Node) queryRow(query string, args ...any) *sql.Row {
	st, err := n.stmt(query)
	if err != nil {
		return n.tx.QueryRow(query, args...) // returns the same error on Scan
	}
	return st.QueryRow(args...)
}

func (n *Node) query(query string, args ...any) (*sql.Rows, error) {
	st, err := n.stmt(query)
	if err != nil {
		return nil, err
	}
	return st.Query(args...)
}

func (n *Node) stmt(query string) (*sql.Stmt, error) {
	if st, ok := n.stmts[query]; ok {
		return st, nil
	}
	st, err := n.tx.Prepare(query)
	if err != nil {
		return nil, err
	}
	n.stmts[query] = st
	return st, nil
}

func (n *Node) sendHeartbeat() {
	hb := peering.DAGStoreHeartbeat{}
	if v, err := n.metaTxID("sealed_tip"); err == nil && v != nil {
		hb.SealedBranch = *v
	}
	if v, err := n.metaTxID("starting_branch"); err == nil && v != nil {
		hb.StartingBranch = *v
	}
	if v, err := n.metaTxID("canonical_tip"); err == nil && v != nil {
		hb.CanonicalTip = *v
	}
	_ = n.queryRow(`SELECT count(*) FROM tx WHERE state = ?`, store.StateLost).Scan(&hb.Lost)
	n.peers.SendDAGStoreHeartbeat(hb)
}

func (n *Node) metaTxID(key string) (*base.TransactionID, error) {
	var v []byte
	err := n.queryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	id, err := base.TransactionIDFromBytes(v)
	return &id, err
}

func (n *Node) setMeta(key string, value any) error {
	_, err := n.exec(`INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

func (n *Node) readDB() *sql.DB { return n.rdb }
