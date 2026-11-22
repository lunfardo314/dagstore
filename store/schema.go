// Package store holds the dagstore's SQLite database: schema, creation and the codes stored in it.
package store

import (
	"database/sql"
	"fmt"
	"os"

	_ "modernc.org/sqlite"
)

// SchemaVersion is stored in meta and bumped on any change of the schema below.
const SchemaVersion = "1"

// Transaction kinds (tx.kind).
const (
	KindPlain     = 0
	KindSequencer = 1
	KindBranch    = 2
	KindMining    = 3
)

// Transaction states (tx.state). A placeholder row (missing, lost, pruned) has no bytes: it exists so
// that every edge points to a row and every gap can be queried.
const (
	StateLinked  = 0
	StateIntake  = 1
	StateMissing = 2
	StateLost    = 3
	StatePruned  = 4 // below the starting branch
)

// Sources of a transaction's bytes (tx.source).
const (
	SourcePush   = 0
	SourcePull   = 1
	SourceBroad  = 2
	SourceImport = 3
)

// Chain kinds (chain.kind).
const (
	ChainOther      = 0
	ChainSequencer  = 1
	ChainDelegation = 2
	ChainMine       = 3
)

// Every id is an integer rowid, so edges and indexes carry 8 bytes at most instead of 32-byte txids.
// The edge and output tables are WITHOUT ROWID: their primary key is the row, so they hold no
// second copy of it.
const schema = `
CREATE TABLE tx (
	id                INTEGER PRIMARY KEY,
	txid              BLOB    NOT NULL UNIQUE,
	slot              INTEGER NOT NULL,
	tick              INTEGER NOT NULL,
	kind              INTEGER,
	sequencer         INTEGER,
	baseline          INTEGER,
	size              INTEGER,
	state             INTEGER NOT NULL,
	confirming_branch INTEGER,
	sig_checked       INTEGER NOT NULL DEFAULT 0,
	parsed            INTEGER NOT NULL DEFAULT 0,
	source            INTEGER,
	received_at       INTEGER
);
CREATE INDEX tx_sequencer ON tx(sequencer, slot) WHERE sequencer IS NOT NULL;

CREATE TABLE tx_bytes (
	id    INTEGER PRIMARY KEY,
	bytes BLOB NOT NULL
);

CREATE TABLE input (
	tx       INTEGER NOT NULL,
	idx      INTEGER NOT NULL,
	producer INTEGER NOT NULL,
	out_idx  INTEGER NOT NULL,
	PRIMARY KEY (tx, idx)
) WITHOUT ROWID;
CREATE INDEX input_producer ON input(producer, out_idx);

CREATE TABLE endorsement (
	tx       INTEGER NOT NULL,
	idx      INTEGER NOT NULL,
	endorsed INTEGER NOT NULL,
	PRIMARY KEY (tx, idx)
) WITHOUT ROWID;
CREATE INDEX endorsement_endorsed ON endorsement(endorsed);

CREATE TABLE output (
	tx        INTEGER NOT NULL,
	idx       INTEGER NOT NULL,
	amount    INTEGER NOT NULL,
	lock_kind INTEGER,
	account   INTEGER,
	chain     INTEGER,
	PRIMARY KEY (tx, idx)
) WITHOUT ROWID;
CREATE INDEX output_account ON output(account) WHERE account IS NOT NULL;
CREATE INDEX output_chain ON output(chain) WHERE chain IS NOT NULL;

CREATE TABLE account (
	id     INTEGER PRIMARY KEY,
	holder BLOB NOT NULL UNIQUE
);

CREATE TABLE chain (
	id       INTEGER PRIMARY KEY,
	chain_id BLOB NOT NULL UNIQUE,
	kind     INTEGER NOT NULL
);

CREATE TABLE lock_kind (
	id   INTEGER PRIMARY KEY,
	name TEXT NOT NULL UNIQUE
);

CREATE TABLE branch (
	tx             INTEGER PRIMARY KEY,
	slot           INTEGER NOT NULL,
	sequencer      INTEGER,
	predecessor    INTEGER,
	total_coverage INTEGER,
	total_supply   INTEGER,
	slot_inflation INTEGER,
	canonical_no   INTEGER,
	fork           INTEGER,
	sealed         INTEGER NOT NULL DEFAULT 0,
	gaps           INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX branch_slot ON branch(slot);
CREATE UNIQUE INDEX branch_canonical ON branch(canonical_no) WHERE canonical_no IS NOT NULL;

CREATE TABLE fork (
	id         INTEGER PRIMARY KEY,
	tip        INTEGER NOT NULL,
	fork_point INTEGER,
	length     INTEGER NOT NULL,
	sealed_tip INTEGER
);

CREATE TABLE missing (
	tx            INTEGER PRIMARY KEY,
	referenced_by INTEGER,
	first_needed  INTEGER,
	tried         TEXT
);

CREATE TABLE library (
	upgrade_index   INTEGER PRIMARY KEY,
	activation_slot INTEGER NOT NULL,
	hash            BLOB    NOT NULL,
	json            BLOB    NOT NULL
);

CREATE TABLE meta (
	key   TEXT PRIMARY KEY,
	value BLOB
);
`

// Create makes a new database at path; it refuses to touch an existing file. Incremental
// auto-vacuum must be set before the first table exists and cannot be switched on later without
// rewriting the file, and it is what lets a pruned or split database give space back while running.
func Create(path string) (*sql.DB, error) {
	if _, err := os.Stat(path); err == nil {
		return nil, fmt.Errorf("%s already exists", path)
	}
	db, err := Open(path)
	if err != nil {
		return nil, err
	}
	for _, stmt := range []string{"PRAGMA auto_vacuum = INCREMENTAL", "PRAGMA journal_mode = WAL", schema} {
		if _, err = db.Exec(stmt); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("create %s: %w", path, err)
		}
	}
	if _, err = db.Exec("INSERT INTO meta(key, value) VALUES ('schema_version', ?)", SchemaVersion); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// Open opens an existing database with one connection: the dagstore has one writer, and readers
// open their own.
func Open(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=synchronous(NORMAL)&_pragma=busy_timeout(10000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}
