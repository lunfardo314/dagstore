# DAGStore — long-term storage of the tangle history

> **DRAFT** 2026-11-22, revision 8 (deployment-neutral, for publication with the `dagstore` repository). Built so far: the offline importer (section 8.2.1), the query API with the dashboard (section 11.2), and the live process: intake, history walk, sealing and heartbeat (section 6.7). Separate repository `dagstore`, depending on `proxima` for code, never the other way round (hard constraint, section 2.3). Proxima-side changes are listed in section 10 and need their own `feat/` branch.

## 1. Problem

- Every node stores every transaction it ever received, raw, in `proximadb.txstore` (Badger, key = txid, value = bytes; [`txstore/txstore.go`](https://github.com/lunfardo314/proxima/blob/develop/txstore/txstore.go)). Nothing deletes from it. Two nodes on one box hold two copies, and neither is guaranteed complete.
- A node that has run since genesis keeps everything back to genesis, and that database only grows. **Bounding it is the main reason for the dagstore.**
- The ledger's history is the past cone of the branches of the canonical lineage. Part of what is stored are orphans, which are not history: the share depends on what counts as history (section 8.2.1 measured 6% when every branch's cone is kept, 43% when only canonical cones are).
- The tools (`proxi db txstore get|list|audit|crosscheck|dagviz`, [`api/dag_explorer`](https://github.com/lunfardo314/proxima/tree/develop/api/dag_explorer)) are basic, because a key-value store keyed by txid supports only lookup by txid and iteration by slot prefix.

## 2. Goal

A special node, the **dagstore**, that keeps the canonical DAG from genesis (or from a chosen starting branch) to the recent past. Its links are consistent and complete, missing data is recorded explicitly, and a rich query API sits on top. Once a dagstore holds a slot's history, the nodes may delete it from their own txstore, so the founders' txstores shrink gradually to a hot window of one to two days.

### 2.1 Guarantees

- **Link consistency.** For every stored transaction, every input (output ID → producer txid) and every endorsement points to a stored transaction, to genesis, to the pruned zone below the starting branch, or to an explicit `missing` record. Nothing dangles silently.
- **Integrity of the canonical record, not validity.** No ledger state, no constraint validation. Intake parses only (stage 1). The signature of a kept transaction is checked once, when its branch is sealed (section 6.4), so the record holds only transactions whose signature verifies (section 5.3).
- **Validity by consensus.** A transaction in the past cone of a canonical branch was validated by the nodes that built that lineage. The dagstore relies on this.
- **Parsing.** Every transaction can be parsed and decompiled with the library valid at its slot (section 7.1).

### 2.2 Not

Not a consensus participant, no ledger state, no sequencer, no attacher, no memDAG. **It does not relay gossip.** It does not use the node's forward sync module or the ledger state of any node (section 6.1).

### 2.3 Dependency direction (hard constraint)

**The dagstore depends on proxima; proxima never depends on the dagstore.** No proxima package, test or tool imports a dagstore package, and proxima's `go.mod` never names the `dagstore` module. What both sides need (the peering messages and their handlers, the cone walker, the signature check, the in-gate) lives in proxima and is used by the dagstore. A proxima change made for the dagstore is general and behind config, so a node that declares no dagstore behaves as before (section 10).

**The two repositories move in lockstep.** The dagstore's `go.mod` pins the exact proxima commit it is built and tested against, with no `replace` directive:

- a proxima change the dagstore needs is pushed first, on proxima's `feat/dagstore` branch;
- the dagstore commit that uses it moves the pin to that commit, in the same commit;
- once `feat/dagstore` is merged into proxima's `develop`, the pin moves to a `develop` commit.

Working on both at once, an untracked `go.work` in the dagstore checkout points the build at a local proxima checkout; before a dagstore commit, it builds and tests with `GOWORK=off`, against the pinned commit.

## 3. Vocabulary

| Term | Meaning |
|------|---------|
| **intake** | Raw transactions received and parsed but not yet linked into the DAG. |
| **branch table** | Every branch the dagstore knows of, canonical or not, with its predecessor (section 6.2). The sync follows it. |
| **lineage** | A chain of branches linked by stem input, back to the starting branch. The stored branches form a tree; normally it is one canonical chain with short side shoots, the competing branches of each slot. |
| **canonical lineage** | The lineage of the canonical tip. Each of its branches carries its **canonical number**, its position on the lineage counted from the starting branch. |
| **canonical tip** | The newest branch the dagstore treats as final: on the lineage of the feeders' latest reliable branch (LRB) and at least `CanonicalDepthSlots` (never below 64) behind it. Newer branches have no canonical number yet. |
| **fork** | A lineage off the canonical one, from its **fork point** (the canonical branch it leaves from) to its tip. Every competing branch that lost its slot is a fork of length 1; a longer fork appears only if the network itself forked. |
| **sealed branch** | A branch whose past cone is stored back to genesis (or the starting branch), checked, with every missing record resolved or declared lost. A property of a **branch**, canonical or not, not of a slot; the branch ID carries its slot. A **sealed tip** is the newest sealed branch of a lineage. The newest sealed canonical branch is announced to the network. |
| **starting branch** | The oldest canonical branch kept: the branch at slot 1 on a full store, the oldest unpruned one after pruning. |
| **confirming branch** of a transaction | The first canonical branch whose past cone contains it; none if it is on no canonical cone. |
| **kept** transaction | One in the past cone of some stored branch, canonical or not. Everything else is an orphan and is purged (section 6.5). |
| **feeder** | A node, or another dagstore, in mutual trusted peering with the dagstore: it pushes every transaction it takes in, and it is where the dagstore pulls history from. A dagstore feeding another is its **source**, the other its **replica** (section 5.4). |
| **trusted dagstore** | A dagstore declared in the node's config: the node pushes to it and believes its sealed-branch announcements. |

## 4. Architecture

```
                 +----------------------- dagstore process ---------------------------+
 feeders  --push-->  peering (proxima/peering, reused)                                 |
 (gossip msg)    |     | gossip handler: feeders only -> in-gate -> parse -> intake     |
 feeders  <-pull->     | targeted pulls (rotating feeders), answers -> same intake      |
 any peer <-pull-      | pull server <- SQLite lookup                                   |
                 |     | heartbeat protocol -> sealed branch every 30 s                 |
                 |                                                                     |
                 |  tip        : feeders' LRB (peering, light)                          |
                 |  branches   : branch table from the feeders' txstore (peering)    |
                 |  history    : follows the branch table, cone walk, targeted pulls   |
                 |  sealer     : signature check of kept txs, seals branches           |
                 |  purger     : orphans and stale intake out, old ranges to archive    |
                 |  query API  : REST over SQLite                                       |
                 +---------------------------------------------------------------------+
```

One process, one SQLite database (section 8), one writer goroutine. Each loop is a small module in the style of [`core/core_modules`](https://github.com/lunfardo314/proxima/tree/develop/core/core_modules).

## 5. Intake: push from feeders after stage 2

### 5.1 Mutual trusted peering

Declared on both sides, in config, between static peers:

- node: `peering.dagstores: [<peer name>, ...]`, names from `peering.peers`;
- dagstore: `peering.feeders: [<peer name>, ...]`, names from its `peering.peers`.

Both ends are static peers of each other, so the relation never churns and the dynamic slots stay free. The node pushes only to a peer it chose; the dagstore takes pushes only from peers it chose.

**Role advertisement (decided).** The heartbeat (section 7.2) tells every peer that the sender is a dagstore. A node receiving one leaves that peer out of its gossip targets, declared or not. The dagstore accepts dynamic peers within its bounds (`max_dynamic_peers`): they use it for pulls and never send it unsolicited gossip. Only trusted dagstores get pushes, the routing of old pulls and a say in purging.

**Typical deployment:** two dagstores, each fed by several nodes, sequencer and access nodes alike, and feeding each other, which makes each a replica of the other (section 5.4).

### 5.2 The hook on the node

In `TxInputQueue.processValidated` ([`core/core_modules/txinput_queue/txinput_queue.go`](https://github.com/lunfardo314/proxima/blob/develop/core/core_modules/txinput_queue/txinput_queue.go)), right after `q.MustPersistTxBytes(tx)`:

```go
q.PushTxBytesToDAGStores(tx.Bytes(), txid, fromPeer)
```

By then the transaction has parsed and passed the signature check, the canonical-encoding check and the sender pace gate, and been persisted. Everything a node keeps reaches its dagstores: what it gossips, but also pulled transactions (never relayed), its own sequencer transactions and API submissions. `fromPeer` is excluded, so a transaction the dagstore just served to the node is not pushed back.

`PushTxBytesToDAGStores` lives in `peering`: for each alive trusted dagstore it calls the existing `SendTxBytesToPeer`, i.e. the existing gossip message, on the existing gossip protocol, through the existing bounded per-peer queue. `GossipTxBytesToPeers` leaves trusted dagstores out of its targets, so each dagstore gets one copy per feeder, not two.

**Cost.** With N feeders, each dagstore receives every transaction N times. The in-gate drops the copies on the txid prefix of the gossip message before parsing; the prefix can be trusted here because only feeders are heard. Upload per feeder grows by one message per transaction per dagstore. Acceptable at today's load; if it ever matters, a node-side option can restrict the push to transactions the node received from a non-feeder or created itself.

### 5.3 On the dagstore: stage 1 only

The gossip stream handler of `proxima/peering` is reused unchanged. The dagstore's input module:

1. hears gossip messages from feeders only. Unsolicited gossip from anyone else, which a node aware of the role does not send, is dropped unread: the dagstore relays nothing and needs nothing beyond what feeders send;
2. dedups by txid (`inGate`, exported from the node's txinput queue);
3. parses (`transaction.Parse`, stage 1) and checks that the computed txid matches;
4. appends to intake.

No signature check at intake. A transaction that parses but is never included is in no stored branch's cone, so it is purged as an orphan (section 6.5).

**Signature check at sealing.** The txid does not cover the signature bytes ([`ledger/txbuildercore/sign.go`](https://github.com/lunfardo314/proxima/blob/develop/ledger/txbuildercore/sign.go)): the signature signs the txid, so it cannot be part of it. Parsing alone therefore does not show that the stored bytes carry a valid signature. The sealer checks it once for every kept transaction, when its branch is sealed (section 6.4), so the record holds only transactions whose signature verifies.

The check needs no EasyFL and no library. The builder signs `txid[:]` (`TxBuilder.SignED25519`) and the ledger requires `validSignature(txID, txSignatureData)` ([`ledger/def/tx_integrity_validator.easyfl`](https://github.com/lunfardo314/proxima/blob/develop/ledger/def/tx_integrity_validator.easyfl)), so the check is one `ed25519.Verify(pubKey, txid, sig)` in Go on the signature data (type byte, 64-byte signature, 32-byte public key, `base.SignatureDataED25519`), with an exact-length check on it. Tens of microseconds per kept transaction, none for orphans.

The dagstore checks the signature, not whether the signer was entitled to spend the inputs: that is validation, left to consensus (section 2.1). Should a question about a historic transaction ever arise, its consumed outputs are all stored, since the DAG is consistent, so it can be answered offline from them.

### 5.4 A dagstore as feeder: replication

A dagstore's feeder can be another dagstore. That is the replication mechanism: no database copying, no second protocol, only the feeder relation of this section with a dagstore on the feeding side.

**Declaration.** The same as for a node, on both sides: the replica lists the source in `peering.feeders`, the source lists the replica in `peering.dagstores`. Two dagstores may feed each other.

**What a source does,** the same three things a node feeder does, from its own database:

- **Push.** Every transaction its intake accepts for the first time goes to its trusted dagstores, except the peer it came from. Loops end by themselves: a dagstore pushes a txid only when its in-gate sees it first, so in a ring of dagstores each transaction travels each link at most once.
- **Pulls**, answered from its database at any age. Nodes keep only `keep_slots` of history; a source keeps all of it, so for anything older a replica asks its dagstore feeders first (section 6.3).
- **Lineage requests** (section 6.2.1). `PullBranchList` is answered from its branch table rather than a txstore scan, and covers every branch the source knows. `PullLRB` is answered with the LRB its own feeders agree on; a replica that also has node feeders counts it as one vote among theirs, one fed by dagstores only follows it.

**What is replicated: transactions, nothing else.** The replica builds its own branch table, cone walks, signature checks, seals, fork and lost records, independently. None of the source's conclusions are copied. Two dagstores reaching the same sealed branches from the same transactions are a check on each other, and a transaction lost on one may still be found by the other through its own feeders.

**Uses:**

- a second copy of the history, kept current by pushes, for example two dagstores feeding each other;
- starting a new dagstore without loading the nodes: its first sync runs entirely against a source, which holds the whole history, at a dagstore-to-dagstore pace that can be set higher than the pace towards nodes (`SourcePullsPerSecond`);
- a read-only replica with dagstores as its only feeders, for example behind a public explorer, so query load never reaches the dagstores the nodes rely on.

A replica announces its own sealed branch like any dagstore; nodes act only on the dagstores they declared, so a replica nobody declared never causes a node to delete anything.

## 6. Building the canonical DAG

### 6.1 No ledger state anywhere

The dagstore depends on no node's ledger state, because state retention must not limit how far back the dagstore can go:

- The node's forward sync ([`core/core_modules/forward_sync`](https://github.com/lunfardo314/proxima/tree/develop/core/core_modules/forward_sync)) commits ledger states: it checks committed root records (`multistate.FetchRootRecord`), calls `ForceCommitBranch` and hands off to the attacher. The dagstore has neither state nor attacher.
- `get_branch_list` ([`api/server/server.go`](https://github.com/lunfardo314/proxima/blob/develop/api/server/server.go)) walks the source's **state** root records (`multistate.IterateBranchIDsBack`), refuses ranges over 8740 slots and returns at most 200 branches. It cannot be stretched to genesis: a node deletes the root record of every branch older than `BranchTxIDStateTTLSlots` (17480 slots, about two days) together with its txID record (`multistate.DeleteRootRecord`, [`kb/archive/shipped/txid_ttl_tiered.md`](https://github.com/lunfardo314/proxima/blob/develop/kb/archive/shipped/txid_ttl_tiered.md)). It stays as it is, serving forward sync. The lineage from genesis is read from the txstore instead, by a new endpoint (section 6.2).

**What is carved out for reuse instead** is the txstore past-cone walker of `proxi db txstore audit` ([`proxi/db_cmd/txstore/audit.go`](https://github.com/lunfardo314/proxima/blob/develop/proxi/db_cmd/txstore/audit.go)): the frontier, dependency discovery from inputs and endorsements, missing-dependency records and per-slot branch completion. It already walks cones from txstore alone. Moved into a package of its own (e.g. `txstore/conewalk`), over an interface to get transaction bytes and to report a missing txid, it serves both the audit command and the dagstore. The parts of forward sync that touch no state (source rotation in `requestChain`) are a few lines and not worth a shared package. `forward_sync` is bound by [`kb/sync_semantics.md`](https://github.com/lunfardo314/proxima/blob/develop/kb/sync_semantics.md) and stays untouched.

### 6.2 Tip and branch table

Each branch consumes the stem output of its predecessor ([`kb/dag_semantics.md`](https://github.com/lunfardo314/proxima/blob/develop/kb/dag_semantics.md); [`ledger/lock_stem.go`](https://github.com/lunfardo314/proxima/blob/develop/ledger/lock_stem.go), `PredecessorOutputID`). The tree of branches is therefore readable from branch transactions alone, and every link checks itself: a branch's stem input must be the stem output of its predecessor.

Sync from genesis starts by fetching the whole branch tree from the feeders and storing it in the branch table. The history walker then follows that table, with pulls.

1. **Tip.** Ask each feeder for its LRB over peering (section 6.2.1), once a minute, and take the one most feeders agree on. The canonical tip is the newest branch on its lineage at least `CanonicalDepthSlots` behind it; `CanonicalDepthSlots` is never below 64. A lineage counts as canonical only from that depth down: branches newer than the tip get no canonical number until the tip passes them.
2. **Branch table.** A new peering request (section 6.2.1) asks a feeder for every branch in its **txstore** in a slot range, with its predecessor: a key-prefix scan per slot finds the branches (txids carry the timestamp and the branch flag; `findBranches` of the audit tool does this today), and each branch's stem input names its predecessor. At most `BranchListMaxSlots` (150) slots per request. It lists what the txstore holds: the canonical branches, the branches that lost their slot, and branches no node ever committed, because a node persists a transaction after stage 2, before attaching it. The dagstore fetches the whole range once, from genesis to the present (about 1,950 requests today, one every 3 s, under two hours), from two feeders whose txstore reaches genesis, and takes the union. From the tip it walks back by predecessors and gives each branch of the canonical lineage its canonical number; every other branch goes into a fork (section 6.6).
3. **Check.** A listed entry is a claim until the branch's bytes are pulled; then its stem input is checked against the listed predecessor. A wrong entry is dropped and the feeder that listed it is logged.
4. **Live.** Once the walker reaches the present, new branches arrive by push and enter the table directly; the endpoint is used again only after a gap, such as a dagstore restart.
5. **Forks.** If the feeders' LRB moves to a lineage that forks below the canonical tip, the canonical numbers move with it (section 6.6). `CanonicalDepthSlots` is chosen so this does not happen in normal operation, but nothing breaks if it does.

The request reads only the txstore, never ledger state, so it reaches as far back as the feeder's txstore does: to genesis on a node that has run since genesis. A node whose txstore was started later holds less, so the sync asks the feeders whose txstore reaches furthest back for the oldest ranges.

#### 6.2.1 Lineage over peering, no HTTP

The tip and the branch table come over the pull protocol between mutually trusted peers, not over any node API: sequencer node APIs stay closed, nothing is opened to the dagstores' addresses, and every feeder serves its own txstore, sequencer nodes included. The pull protocol already starts each message with a type byte (`PullTransactions` is type 0, [`peering/pull.go`](https://github.com/lunfardo314/proxima/blob/develop/peering/pull.go)); two request types join it:

- `PullLRB` → the feeder's LRB branch txid, its oldest txstore slot and its purge floor;
- `PullBranchList(from_slot, to_slot)` → the list of (branch txid, predecessor txid) pairs, in one or more reply messages.

Wire layouts ([`peering/dagstore.go`](https://github.com/lunfardo314/proxima/blob/develop/peering/dagstore.go)), integers big-endian, the first byte the message type:

| Type | Message | Layout |
|------|---------|--------|
| 1 | `PullLRB` | `[1]` |
| 2 | `PullBranchList` | `[2] from u32 \| to u32`, at most `BranchListMaxSlots` (150) slots |
| 3 | LRB reply | `[3] LRB txid (32) \| oldest txstore slot u32 \| purge floor u32` |
| 4 | branch list reply | `[4] from u32 \| to u32 \| last u8 \| N × (branch txid, predecessor txid)`, 64 bytes an entry, at most `MaxBranchListEntries` (1023) per message; a range takes several messages, `last` = 1 on the final one |

Replies go back on the feeder's own pull stream. A feeder answers requests only from its trusted dagstores, and a dagstore takes replies only from its feeders.

A feeder answers them only to peers it declared as trusted dagstores, at the same per-stream pace as transaction pulls. The only HTTP calls the dagstore makes are for the ledger identity and the libraries (section 7.1), and those go to the public access nodes: both come with their proof, so nothing in them rests on trusting the node that serves them.

Stem data carries each branch's total coverage and supply. The dagstore stores them and can compare competing branches by absolute coverage ([`kb/branch_comparison.md`](https://github.com/lunfardo314/proxima/blob/develop/kb/branch_comparison.md)) as a check on the feeders, but the decision stays with the feeders' LRB.

### 6.3 History: slot by slot from the feeders, without rushing

The history walker goes forward through the branch table, slot by slot from the starting branch: the canonical branch of the slot first, then the slot's other branches.

- For branch `b`, walk its cone delta over its predecessor (the carved-out cone walker): inputs (producer = `oid.TransactionID()`) and endorsements, stopping at transactions already reached from `b`'s lineage. A canonical `b` becomes the confirming branch of what it newly reaches; every reached transaction is kept.
- A referenced txid not in the store is pulled. Only transactions in some branch's cone are pulled during the history pass, so no bandwidth goes to orphans. Keeping losing branches is not free: their cones overlap the canonical cone only partly. Measured on history from before library upgrade 7 (section 8.2.1), each losing branch brought about 3.6 transactions of its own that no canonical cone holds.
- Once the walk reaches the present, live pushes have usually delivered everything already and the walker only links.

**Pull policy, in order:**

1. **One feeder at a time.** The branch of slot `s` is served by one feeder, the next slot by the next feeder, rotating. For slots older than the nodes' `keep_slots`, dagstore feeders are asked first: nodes no longer hold them. Pulls are targeted with a new `PullTransactionFromPeer(id, txid)`; today `PullTransactionsFromPeers` broadcasts to every alive peer ([`peering/pull.go`](https://github.com/lunfardo314/proxima/blob/develop/peering/pull.go)).
2. **Next feeder on a miss.** The pull protocol has no "not found" answer (the server traces and stays silent, `pull_tx_server.go`), so a miss is a timeout (`PullTimeout`, 5 s); the txid then goes to the next feeder, and so on through all feeders.
3. **Any other dagstore peer** that is not a feeder.
4. **Broad pull, last resort, canonical cones only.** A pull sent to every alive peer, for transactions in a canonical cone that no feeder and no dagstore has. A gap in a non-canonical branch's cone does not justify loading every peer: it ends at step 3. Rate-limited hard (`BroadPullPerSecond`, 1/s), because it costs every peer a lookup. Answers here are the only intake not from a feeder; the sealer's signature check (section 6.4) covers them.

**Pace.** Bounded by `HistoryPullsPerSecond` (default 20) overall and `PullsPerSecondPerFeeder` (default 5); at most one branch delta in flight per feeder. The server side allows 500/s per stream, so this stays far below what a feeder serves anyway. Both are config values: the dagstore is meant to take days over the first sync, not to load the feeders.

**Explicit loss.** A missing record carries the txid, who references it, first-needed time, the sources tried and a state (`pending`, `lost`). It becomes `lost` once every step of the ladder has failed `MissingRetryRounds` times, rounds `MissingRetryInterval` apart. A lost record is kept forever and queries that cross it say so. It no longer blocks sealing; the branch is marked sealed with gaps (count kept per branch). As long as it is pending, sealing waits, and nodes delete only well below the sealed tip (section 7.2), so a transaction that some node may still hold is never deleted before the dagstore has asked for it.

### 6.4 Sealer

Branch `b` is sealed when its predecessor is sealed, its cone walk is complete, none of its missing records is pending, and every transaction its cone walk reached has passed the signature check of section 5.3 (ed25519 over the txid, in Go). No EasyFL runs in the dagstore: not `ValidatePartialContext`, which also evaluates the integrity script and applies the blocklist, a node policy that must not decide what history is, and not `ValidateFullContext`. A transaction failing the check has its bytes discarded and is pulled again, from the next source in the ladder. A sealed branch is never unsealed, and nothing in its past cone is ever changed or purged; on each lineage the sealed tip only moves forward. Canonical and non-canonical branches are sealed by the same rule; canonical sealing never waits for a non-canonical one.

### 6.5 Orphan and intake purge

A transaction at slot `s` that is not kept (in no stored branch's cone), once the canonical tip is past `s + OrphanPurgeSlots`, is deleted with its edges. Junk that parsed but was never included goes the same way, and so do milestones that no branch consolidated.

**Branches are never purged (decided).** Every branch, canonical or not, stays with its cone, sealed by the same rule, for analysis of branch competition ([`kb/research/split_slots.md`](https://github.com/lunfardo314/proxima/blob/develop/kb/research/split_slots.md) and `branch-bias.md` needed exactly that). Measured on history of November 2026 from before library upgrade 7 (section 8.2.1), about 86 branches were produced per slot, nearly every sequencer branching every slot, so about 85 losing branches per slot; since upgrade 7 branching needs 20,000 PROX of own capital or 100,000 of coverage, so fewer are expected. What keeping them costs is measured in section 8.2.1 and reopened in section 13.

`OrphanPurgeSlots` must exceed the longest delay with which a transaction can still enter a later canonical cone. Assumption to verify: node policy bounds that by the non-branch txID retention (`TxIDStateTTLSlots`, default 60, [`ledger/def_constants0.go`](https://github.com/lunfardo314/proxima/blob/develop/ledger/def_constants0.go)).

During the first sync, live pushes pile up in intake for slots the history walker has not reached. They stay until linked or purged; they are not orphans until the tip-relative rule says so, measured against the canonical tip, not the walker.

### 6.6 Forks

The dagstore stores DAG history, not a decision about it, so a fork is ordinary data.

**Fork table.** One row per fork tip that is not on the canonical lineage: the tip, the fork point (the canonical branch it leaves from), its length and its sealed tip. Normally every row is a branch that lost its slot: length 1, its predecessor the fork point. The branch table carries a fork reference on each non-canonical branch and a canonical number on each canonical one, so "is it canonical", "where does it leave" and "in which order" are each one lookup.

**A real fork.** If the network returns N slots and forks from there, the feeders' LRB moves to a lineage that branches off the canonical one at fork point `F`. The dagstore then:

- keeps everything on the old lineage as it is: its sealed branches stay sealed and their cones stay;
- turns the old lineage above `F` into a fork row (tip, fork point `F`, length) and clears its branches' canonical numbers;
- numbers the new lineage from `F` on, builds it like any other (branch table, cone walks, sealing) and announces its newest sealed branch from then on;
- recomputes the confirming branch of transactions confirmed above `F`, the only data that depends on which lineage is canonical.

In practice there will be no such fork; the design does not depend on that. Consistency of links holds across the whole tree: each branch's stem input points to its predecessor, each transaction's inputs and endorsements point to stored transactions, whichever lineage they belong to.

### 6.7 Built

Package `live`, run by `dagstore -config <file>` beside the query API. One goroutine owns the write connection and commits every second. It does all of this section:

- **Intake:** gossip from feeders only, dedup by txid, stage-1 parse, txid check.
- **Tip and branch table:** `PullLRB` every minute; the branch list fetched from the feeder's oldest txstore slot to the first LRB, then branches by push.
- **Canonical numbering:** at `CanonicalDepthSlots` behind the LRB, waiting for any branch of the lineage not stored yet.
- **The walk:** slot by slot, the canonical branch first, linking, checking each signature and writing edges and outputs.
- **Gaps:** recorded per branch in a table `waiting`; a walk resumes from a transaction when it arrives. A resumed canonical walk takes over what a later canonical branch reached meanwhile, since a transaction is confirmed by the first canonical branch whose cone holds it.
- **Pulls:** the feeder ladder, at most `PullsPerSecondPerFeeder` per feeder, rounds, then `lost`. The order matters: first the branches the canonical tip waits for (the LRB and its lineage), then the oldest transaction due. Sealing advances in slot order, so in random order each branch waited behind thousands of newer pulls. The budgets are config keys (`dagstore.pulls_per_second_per_feeder`, `dagstore.history_pulls_per_second`). A gap in the LRB's lineage, from pushes a feeder dropped in a burst, is filled by fetching the branch list again over the gap rather than by one pull per branch.
- **Sealing, forks, orphan purge, heartbeat:** sealing in slot order; fork rows; orphan purge at `OrphanPurgeSlots`; the heartbeat every `HeartbeatPeriod`.

The libraries of a new database are fetched from a node API (`dagstore.library_api`), each with its proof, and stored.

**Checked by replay.** `live/TestReplay` feeds 500 to 600 slots of a real txstore through the intake, the heaviest branch of each slot taken as the LRB, and compares with the offline importer's database of the same txstore. The two agree exactly away from the ends of the range: canonical lineage, the confirmations of every canonical branch, and every kept transaction. That held in order, and with one transaction in seven, branches included, delivered 160 slots late. No edge dangles after the orphan purge. Speed: about 2,000 to 3,000 transactions a second on one core.

**Not built yet:** the LRB most feeders agree on (with several feeders the last answer wins); broad pulls; renumbering after a real fork (the canonical tip stops moving and an error is logged); replication and the source side of section 5.4; archive segments.

## 7. Libraries, network identity, and the nodes' txstore

### 7.1 Libraries

The dagstore parses and decompiles a transaction at slot `s` with the library valid at `s`, so it stores every historic library in its database (table `library`: upgrade index, activation slot, hash, library JSON), whatever their source. Stored with the data, they make the database and its archive segments readable years later without the binary of that time.

The rendezvous number in the protocol names comes from `ledger.L(0).LibraryHash()` ([`peering/peers.go`](https://github.com/lunfardo314/proxima/blob/develop/peering/peers.go)), so the dagstore needs the ledger identity first: fetched from a feeder with `get_ledger_definition` and checked with `txbuildercore.VerifyLibraryCommitment` ([`kb/library_proof.md`](https://github.com/lunfardo314/proxima/blob/develop/kb/library_proof.md)). `ledger.L` loads libraries from the state store today ([`ledger/lib_singleton.go`](https://github.com/lunfardo314/proxima/blob/develop/ledger/lib_singleton.go)); the dagstore needs an initialiser from the identity plus stored libraries (section 10).

**Source of libraries (decided): both.** The dagstore fetches each library after its upgrade activates (`get_ledger_definition?slot=<s>` from a public access node, which serves the library valid at a slot with its proof), checks the proof against a branch it holds itself, and stores it. Where its own build knows the upgrade (`ledger.Upgrades`), it also compares the two and raises an alarm on any difference. A library its build does not know is accepted on the proof alone and marked as such.

Until the library of a new upgrade is available, transactions from its slot on are stored raw and marked `unparsed`, and parsed later. Linking, signature checks and sealing go on: none of them uses the library.

**A dagstore not upgraded.** Peering keeps working through any upgrade: the rendezvous number comes from the genesis library, which no upgrade changes. What happens next depends on the upgrade:

- **Library only** (the usual kind, no change in the transaction format or the txid derivation): the dagstore fetches and proves the new library, parses with it, and carries on. Nothing to do but upgrade at leisure.
- **A change in Go code** (a new transaction version with a new layout or a new txid derivation, as upgrade 6 changed the txid): the old build cannot be trusted to compute the txid, and everything rests on the txid. The dagstore recognises such a transaction by a transaction version newer than any its build knows, and stops there: it does not store it, keeps sealing up to the last branch before the upgrade slot, and raises an alarm that it must be upgraded. Its feeders are not penalised for sending what it cannot parse.

Nothing is lost while it waits. Nodes delete only below the announced sealed branch minus `keep_slots`; a stalled dagstore keeps announcing the same branch, so the nodes stop deleting and keep everything from there on. The cost is only that their txstores grow until the dagstore is upgraded, after which it catches up from the feeders as in section 6.3.

### 7.2 Purging the nodes' txstore

**Heartbeat.** Every `HeartbeatPeriod` (30 s) the dagstore sends to every connected peer, on a new protocol `/proxima/dagstore/<rendezvous>`, a small message: the newest sealed canonical branch (its txid carries its slot), starting branch, canonical tip, lost count. Layout, integers big-endian: `sealed branch (32) | starting branch (32) | canonical tip (32) | lost u64`, 104 bytes. The role (dagstore, section 5.1) is implicit: whoever speaks the protocol is a dagstore. libp2p authenticates the sender, so the message is not signed.

A node bans a peer for 10 minutes as "another ledger" when the peer refuses one of its protocols. The dagstore protocol is exempt, so a node without dagstore support that refuses the heartbeat stream is not banned; it costs one failed protocol negotiation per heartbeat. Only the side opening a stream ever bans, so such nodes behave as before. Its sends also keep dynamic peers from dropping the dagstore as idle (`idlePeerTimeout`), as connectivity records do.

**On the node,** a new module `txstore_purger` reads heartbeats only from its trusted dagstores and deletes txstore entries with slot below

```
floor = slot(newest announced sealed branch on the node's own lineage) - txstore.keep_slots
```

- **Purging is a separate opt-in (decided).** A node deletes only with `txstore.purge: true` (default false) and at least one trusted dagstore declared; declaring a dagstore in `peering.dagstores` alone never deletes anything.
- `txstore.keep_slots`: default 17480 (about two days, equal to `BranchTxIDStateTTLSlots`), never below 8740 (about one day, `sync.max_slots_behind`, the reach of forward sync, which other nodes serve from their txstore). The minimum leaves no margin: a peer catching up by forward sync pulls up to `sync.max_slots_behind` (8740) slots back plus a past-cone overhang, so at 8740 such pulls may miss; the default leaves a day.
- A sealed branch counts only when it is on the node's own lineage: the node has committed it (`multistate.FetchRootRecord`), which a branch minutes old always satisfies on a node on that lineage. A node on another fork deletes nothing until a dagstore seals a branch of its lineage.
- One trusted dagstore's announcement is enough (decided: no spot-check before deleting; with two dagstores an operator compares their `/status`). The floor only rises. Without a heartbeat nothing is deleted. A node with no trusted dagstore never deletes, which is today's behaviour.
- Orphans in the node's txstore are deleted too.
- A txid begins with its timestamp, slot first, so a slot is a key prefix: deletion is a paced prefix drop, one slot at a time, starting from the oldest slot held.
- **Disk space.** Badger frees the space of deleted values only through value-log GC, and today the node runs it on the state DB only ([`node/db.go`](https://github.com/lunfardo314/proxima/blob/develop/node/db.go), `databaseGC`), not on `txstore`. The purger runs it on `txstore` too, after each batch of slots; without it the bootstrap machines' disks would not shrink.

**Gradual cleaning of long-running nodes' txstores.** While the first sync runs, the sealed tip climbs from genesis, and the floor climbs behind it, `keep_slots` lower: the txstores of nodes running since genesis shrink from the oldest slot forward, at the pace of the sync, never ahead of it. Once the dagstores reach the present, every node keeps about `keep_slots` of history.

Checked by a survey of the node code (2026-11-22): nothing in the node reads txstore entries older than `keep_slots` except pull serving. Restart rebuilds the memDAG from the state, memDAG reattach reads a few hundred slots back, snapshots do not touch the txstore, and API reads of a purged transaction answer not-found. Pull serving to peers catching up by forward sync is the one coupling, covered by the margin of `keep_slots` above. A transaction received again after it was purged is written again below the floor and purged again on the next round.

**Pull routing on the node.** After purging, only dagstores hold old transactions. A node's pull for a txid older than `current slot - txstore.keep_slots` goes to its trusted dagstores only, and falls back to a broadcast when none is alive. Newer txids are broadcast as today, trusted dagstores included. Nodes that do not declare a dagstore still reach it by broadcast, as a dynamic peer.

## 8. Storage: SQLite

A key-value store does not support the queries of section 11. SQLite fits: one writer and many readers (WAL mode), one file, no server to run, simple backups. Driver: `modernc.org/sqlite` (pure Go) unless measured too slow, then `mattn/go-sqlite3`.

### 8.1 Schema sketch

Every transaction gets a small integer id (the SQLite rowid) and every edge refers to ids, not to 32-byte txids: an edge row shrinks from about 70 to about 20 bytes, and an index entry with it. A transaction referenced but not stored gets a **placeholder row** in `tx` (state `missing`, later `lost`), so every edge points to a row and every gap is a row anyone can query.

| Table | Columns | Notes |
|-------|---------|-------|
| `tx` | `id` (rowid), `txid` UNIQUE, `slot`, `tick`, `kind` (branch / sequencer / plain / mining), `sequencer` (chain id), `baseline` (the explicit baseline, an edge of the cone walk too), `size`, `state` (missing / lost / intake / linked), `kept`, `confirming_branch` (null if on no canonical cone), `sig_checked`, `parsed`, `source` (push / pull / broad / import), `received_at` | txids begin with the timestamp, so inserts land at the end of the txid index; index on `(sequencer, slot)` |
| `tx_bytes` | `id`, `bytes` | Separate, so index scans stay small; a rowid table keeps blobs up to about 4 KB inline in a 4 KB page, larger ones spill to overflow pages |
| `input` | `tx`, `idx`, `producer`, `out_idx` | Consumption edges; index on `(producer, out_idx)` for "who spent this output" |
| `endorsement` | `tx`, `idx`, `endorsed` | Index on `endorsed` |
| `output` | `tx`, `idx`, `amount`, `lock_kind`, `account`, `chain` | Parsed with `txbuildercore` (`ClassifyChain`, lock classifiers); indexes on `account`, `chain` |
| `account`, `chain`, `lock_kind` | `id`, holder ID, chain ID or lock name UNIQUE, chain kind | Dictionaries, so outputs carry integers |
| `branch` | `tx`, `slot`, `sequencer`, `predecessor`, `total_coverage`, `total_supply`, `slot_inflation`, `canonical_no` (null if not canonical), `fork` (null if canonical), `sealed`, `gaps` | The branch table: every branch known, canonical or not |
| `fork` | `id`, `tip`, `fork_point`, `length`, `sealed_tip` | One row per non-canonical tip; normally length 1 |
| `missing` | `tx`, `referenced_by`, `first_needed`, `tried` | Retry bookkeeping for placeholder rows |
| `library` | `upgrade_index`, `activation_slot`, `hash`, `json` | Section 7.1 |
| `meta` | key, value | Ledger identity, starting branch, canonical tip, schema version |

### 8.2 Size estimate

Measured on the network over one day in November 2026: about 50 transactions stored per second, about 32 confirmed, 682 bytes per transaction on average, 3.7 inputs plus endorsements per transaction. The network is already at the low end of the 50-100 TPS range.

Per kept transaction, as rough estimates to be checked by a prototype:

| Part | Bytes |
|------|-------|
| raw bytes in `tx_bytes`, with page fill | ~800 |
| `tx` row and its indexes | ~150 |
| inputs and endorsements (3.7 edges, row plus index) | ~200 |
| outputs (~2.5, row plus account and chain indexes) | ~300 |
| **total** | **~1.5 KB** |

With 32-byte txids in every edge instead of integer ids, the total is about 2.3 KB. SQLite's own overhead on the blobs is small (a few bytes of record header and a 2-byte cell pointer per row, plus unused page space); what costs is the indexes, and they are what the queries need.

| Kept TPS | Transactions per day | Raw bytes per day | Database per day | Database per year |
|----------|----------------------|-------------------|------------------|-------------------|
| 32 (today) | 2.8 M | 1.9 GB | ~4 GB | ~1.5 TB |
| 50 | 4.3 M | 2.9 GB | ~6.5 GB | ~2.4 TB |
| 100 | 8.6 M | 5.9 GB | ~13 GB | ~4.7 TB |

Check against real history (November 2026): the txstore of a node running since genesis held 71 GB after about 35 days, about 2 GB a day, below the 2.9 GB a day of raw bytes the load above would give, as expected since the load was lower earlier. For the dagstore: about two thirds of it is kept (32 of 50 transactions), so about 45 GB of raw bytes, about 85 GB with the tables and indexes above. That whole history fits a single SQLite file comfortably.

Only kept transactions count: the orphans (today about a third of what is stored) are purged. Writes are no problem at these rates: 100 TPS is about 1,500 rows a second, which SQLite does easily in batched transactions.

#### 8.2.1 Measured

The offline import prototype (`cmd/dagstore-import`) loaded the txstore of an access node of the November 2026 network: 10.1 M transactions, 6.75 GB of raw bytes, 11,977 slots (about 1.4 days, before library upgrade 7), an unbroken canonical lineage over all of them, 1.03 M branches.

- **Kept:** 9.49 M transactions, 94% of what was stored; 5.78 M of them are in a canonical cone, 3.71 M (39% of the kept, 2.66 GB of raw bytes) only in the cone of some losing branch. Only 6% were orphans.
- **What was kept:** 8.45 M sequencer transactions, 1.03 M branches, 11 k mining transits, 5 k others. 1.11 inputs, 1.92 endorsements and 1.11 outputs per kept transaction on average.
- **Size:** 10.8 GB, **1,142 bytes per kept transaction**, against the estimate of about 1.5 KB:

| Part | Bytes per kept transaction |
|------|----------------------------|
| raw bytes in `tx_bytes` (671 of payload, 16% of its pages unused) | 849 |
| `tx` row and txid index | 118 |
| inputs and endorsements, rows and indexes | 97 |
| outputs, rows and account and chain indexes | 52 |
| sequencer index | 16 |
| branch and fork tables | 9 |

  Edges and outputs cost far less than estimated, because nearly every transaction is a sequencer milestone with few outputs. The raw bytes are the size. The node's own Badger txstore held all 10.1 M transactions in 4.8 GB on disk, compressed, against the 10.8 GB here: compressing `tx_bytes` (for example zstd with a dictionary trained on transactions) is the first thing to try if size matters.
- **Rate:** at the load of that period, 6.7 M kept transactions a day, about 7.6 GB a day, about 2.8 TB a year; with canonical cones only, about 4.6 GB a day.
- **Checks:** every one of the 9.49 M signatures verified with the plain ed25519 check over the txid (section 5.3); no parse failure, no txid mismatch. 123 producers were missing, all in losing branches' cones: every canonical branch sealed, 105 losing branches did not.
- **Speed:** the cone walk ran at about 5,100 kept transactions a second on one core (31 minutes), with 10 GB of memory, most of it the in-memory map from txid to row id.

**One file for now (decided).** Today's history is about 85 GB; at 50 TPS the file grows by about 2.4 TB a year, so one file serves for many months.

**It can be pruned and split later, if two things are set from day one:**

- `PRAGMA auto_vacuum = INCREMENTAL` when the database is created (it cannot be switched on later without a full rewrite). SQLite does not shrink a file after `DELETE`: freed pages are only reused by later inserts. With incremental auto-vacuum, `PRAGMA incremental_vacuum` returns freed pages to the file system a batch at a time, while the store runs. Without it, the only way to shrink is a full `VACUUM`, which rewrites the whole file and needs as much free disk again.
- Raw bytes in their own table (`tx_bytes`), keyed by the stable integer id.

**Pruning** is then the archive export of section 8.3 followed by deleting the range, oldest first, and an incremental vacuum. All the large indexes are time-ordered, so a range of old slots occupies a contiguous part of each and its pages are freed together.

**Splitting** is a migration of `tx_bytes` (most of the size) into per-period files: `ATTACH` a new file, copy a slot range of `tx_bytes` into it, delete it from the main file, incremental vacuum. The ids do not change, so no other table is touched; readers look up bytes in the main file, then in the attached ones.

The one thing that does not split well is the account index, written all over the file. If the hot window must hold several TB, a server database (PostgreSQL) is the alternative; nothing in the design above depends on SQLite in particular.

### 8.3 Pruning to the archive

Below a configured slot, the dagstore exports a sealed range into an archive segment: raw bytes in slot order, zstd-compressed (ratio to be measured; signatures, keys and hashes do not compress, repeated bytecode does), with a manifest (slot range, first and last branch, count, a hash over the sorted txids, the lost records of the range). Then it deletes the range and moves the starting branch up. A reference to a pruned transaction resolves to "pruned, archive segment X". A segment can be imported into any dagstore; this is also how a new dagstore starts without pulling the whole history.

## 9. Constants (initial proposals)

| Name | Value | Why |
|------|-------|-----|
| `CanonicalDepthSlots` | 120 (about 20 min), never below 64 | A lineage is canonical only from this depth behind the LRB; split slots settle within a few slots. Tune on measurements ([`kb/research/split_slots.md`](https://github.com/lunfardo314/proxima/blob/develop/kb/research/split_slots.md)) |
| `OrphanPurgeSlots` | 360 (about 1 h) | Must exceed the latest inclusion delay (section 6.5) |
| `HeartbeatPeriod` | 30 s | As asked |
| `BranchListMaxSlots` | 150 | One `PullBranchList` request: at the 86 branches a slot measured before library upgrade 7 (section 8.2.1), about 12,900 entries, 13 reply messages of up to 1,023 entries (64 KB) |
| `PullTimeout` | 5 s | A feeder answers in well under a second; silence means it does not have it |
| `HistoryPullsPerSecond` / `PullsPerSecondPerFeeder` | 20 / 5 | Far below the 500/s per-stream server limit |
| `BroadPullPerSecond` | 1 | Every peer pays for a broad pull |
| `SourcePullsPerSecond` | 200 | History pulls from a dagstore feeder; a source is built to serve history, the nodes are not |
| `MissingRetryRounds` / `MissingRetryInterval` | 3 / 10 min | Covers a feeder restart |
| node `txstore.keep_slots` | 17480, min 8740 | Section 7.2 |

## 10. Changes in the proxima repository

Small, behind config, inactive unless a dagstore is declared. No ledger change, no hardfork.

1. **peering:** `peering.dagstores` (node) and `peering.feeders` (dagstore) config and the role flag on a peer, set also for any peer whose heartbeat says dagstore, which then gets no gossip; `PushTxBytesToDAGStores`; trusted dagstores left out of `GossipTxBytesToPeers`; `PullTransactionFromPeer(id, txid)`; the heartbeat protocol (sender and handler hook); old-txid pull routing; a hook for the gossip handler to drop messages from non-feeders before parsing.
2. **txinput_queue:** the one-line push after `MustPersistTxBytes`; export `inGate` (generic, self-contained in `ingate.go`).
3. **ledger/transaction** or **ledger/base:** a plain Go signature check, `ed25519.Verify` of the signature data over the txid with an exact-length check, if none is exported yet.
4. **txstore:** the cone walker carved out of [`proxi/db_cmd/txstore/audit.go`](https://github.com/lunfardo314/proxima/blob/develop/proxi/db_cmd/txstore/audit.go) into a package, `audit` rewritten on it; prefix delete of a slot in `SimpleTxBytesStore`.
5. **node:** `txstore_purger` module behind `txstore.purge` (default false) and a declared trusted dagstore; `txstore.keep_slots`; deletion oldest slot first in batches (about 20 slots per 10 s), not Badger `DropPrefix`, which stops writes; value-log GC on `txstore`. Metrics `proxima_txstore_purge_floor` and `proxima_txstore_slots_purged`.
6. **ledger:** a library initialiser from the ledger identity and `ledger.Upgrades`, without a state store.
7. **api/dag_explorer:** nothing to change. It already takes an interface, `TxStore` (lookup by txid and iteration by key prefix), which the dagstore implements over SQLite (section 11.2).
8. **peering:** the `PullLRB` and `PullBranchList` request types and their replies (the `PullLRB` reply also carries the feeder's oldest txstore slot and purge floor, for the dashboard, section 11.1), answered only to trusted dagstores (section 6.2.1); the branch list over the txstore only on a node. The request handlers sit behind an interface, so a dagstore acting as source answers them from its database (section 5.4); the push side is the same `PushTxBytesToDAGStores`.
9. **proxi:** a later `proxi dagstore ...` command group, a client of the dagstore API.

`peering.New` needs an environment satisfying `global.NodeGlobal`; the dagstore provides it with `proxima/global`, as the node does. `forward_sync` is not touched.

## 11. Query API

REST, JSON, read-only, on its own port, paged by cursor, with per-request limits:

| Endpoint | Returns |
|----------|---------|
| `/status` | starting branch, newest sealed canonical branch, canonical tip, forks longer than 1, intake size, pending and lost counts, feeders alive, history walker position |
| `/tx/{txid}` | raw, parsed, decompiled; state, kept, confirming branch; for each output, who consumed it |
| `/output/{oid}` | producer, consumer(s), parsed output |
| `/slot/{n}`, `/range?from_slot&to_slot&kind&status` | transactions in a range |
| `/branches?from_slot&to_slot&canonical` | branches with coverage, supply, canonical number or fork, any slot range; canonical only, non-canonical only, or all |
| `/forks?from_slot&to_slot&min_length` | fork tips with fork point and length; `min_length=2` lists real forks only |
| `/past_cone/{txid}?depth&limit`, `/future_cone/{txid}?...` | traversals |
| `/account/{holder}` | outputs produced and consumed by an account over time |
| `/chain/{chainID}` | every transition of a chain (sequencers, delegations, the mine chain, foundries) |
| `/confirmed_in/{branch}` | transactions whose confirming branch is this one |
| `/missing?state` | missing and lost records |
| `/export?from_slot&to_slot` | raw bytes in slot order, streamed, for another dagstore and for analysers |

Clients: ledger and DAG explorers, history analysers, `proxi`. The dagviz endpoints of [`api/dag_explorer`](https://github.com/lunfardo314/proxima/tree/develop/api/dag_explorer) are served on top.

### 11.1 Web dashboard

The dagstore serves a web dashboard on the API port, so an operator can see at a glance how far the sync is, whether the store is healthy and how fast it is moving. One page in the dagstore binary, read-only, no external dependencies, refreshing itself from `/status` and a `/stats` endpoint with the counters below. The same numbers are exported as Prometheus metrics (section 12). The DAG itself is browsed with the [`api/dag_explorer`](https://github.com/lunfardo314/proxima/tree/develop/api/dag_explorer) pages served beside it.

| Panel | Shows |
|-------|-------|
| **Sync progress** | Branch table fetch: slots covered of the whole range. History walker: its slot against the canonical tip, percent of slots done, speed in slots per minute, estimated time to the present |
| **Where the store stands** | Starting branch, newest sealed canonical branch, canonical tip, the feeders' LRB, and the lag of each behind the current slot; branches stored, forks (length 1 and longer) |
| **Feeders and replicas** | Per feeder: alive, kind (node or dagstore), pushes per second, pulls served, timeouts, last contact, and how far back its txstore reaches and its purge floor; replicas fed by this dagstore |
| **Intake and pulls** | Pushes per second, copies dropped as duplicates, intake size, pulls per second against the configured budgets, timeouts, broad pulls |
| **Gaps** | Missing records pending and lost, the oldest pending, branches sealed with gaps |
| **Database** | File size and growth per day, size per table, free pages awaiting incremental vacuum, disk free, archive segments and the starting branch they moved to |
| **Libraries** | Upgrades known, fetched with proof, checked against the build; the alarm when a transaction version unknown to the build has stopped the sync |
| **Main parameters** | The configuration in force: `CanonicalDepthSlots`, `OrphanPurgeSlots`, pull budgets and timeouts, `BranchListMaxSlots`, feeder and trusted-peer lists |

How far back a feeder's txstore reaches and its purge floor come from the feeder itself, added to the `PullLRB` reply (section 10).

### 11.2 Built

`cmd/dagstore` serves a database read-only, as the offline importer leaves it: the endpoints of the table above at the paths given there, the dashboard at `/`, and the explorer of `api/dag_explorer` at `/dag_explorer`, unchanged, over an adapter of its `TxStore` interface. What the table does not say:

- **Bounds.** Every list is paged: `limit` (default 100, at most 1,000) and `after`, the `next` value of the previous page. A slot range covers at most 1,000 slots, a cone at most 10,000 vertices, and the queries of one request stop after 15 s. `/export` alone is unbounded and untimed: it is meant for bulk transfer.
- **`/range`** filters by `kind` (`plain`, `sequencer`, `branch`, `mining`) and `status` (a state, `confirmed`, or `unconfirmed`: stored but on no canonical cone). Rows are in txid order, which is time order.
- **`/forks`** over the whole store, without a slot range, needs `min_length` of at least 2: forks of length 1 are about one per sequencer per slot.
- **`/future_cone`** follows inputs and endorsements but not baselines, which have no reverse index: a transaction linked to the walk only by its baseline is missed. An index on `tx(baseline)` would close it, if needed.
- **`/confirmed_in`** needs no index on `confirming_branch`: everything a branch newly confirms is reached from the branch through transactions it newly confirms, so it walks the cone through those only. Checked against a full scan of `tx` on two branches.
- **`/account`** and **`/chain`** list outputs in store order, close to time order. `/account` takes the holder in hex or in the form `proxi` prints, `addressED25519(0x…)`.
- **`/export`** writes each transaction as a 4-byte big-endian length followed by its bytes, in txid order.
- **`/status`** is cheap; **`/stats`** is a snapshot of whole-table aggregates and per-table sizes, recomputed every 10 minutes (11 s on the imported database, most of it SQLite's `dbstat`).
- **Libraries** come from the database's own `library` table (section 7.1), which the importer now fills, and are checked at start against the libraries this build makes for each upgrade; the dashboard shows the result and an alarm on a difference.
- **Printing a transaction** (`/tx`) uses proxima's printers, which read what partial-context validation sets up, so it runs there, with the library of the transaction's slot. It is display only: nothing stored or sealed depends on it, and a failure is shown beside the text.
- **The dashboard** shows the panels of section 11.1 from `/status` and `/stats`. The sync, feeder and intake panels say "not running" until the live modules exist.

On the imported database (9.5 M transactions, 10.8 GB): lookups by txid, output, account, chain, slot and branch answer in about 1 ms; a 1,000-slot range with a filter that matches nothing in 0.3 s; a past cone of 10,000 vertices in 0.5 s; the 512 transactions one branch confirms in 30 ms.

## 12. Rollout

1. Proxima release with section 10; nodes declare nothing yet, behaviour unchanged.
2. Start the dagstores with the chosen nodes as feeders; the nodes still declare nothing, so they push nothing and delete nothing. The dagstores fetch the branch table from the feeders' txstore and walk history from genesis, pulling from feeders at the history pace.
3. Declare the dagstores on the nodes (`peering.dagstores`): pushes start. Purging starts with it, trailing the sealed tip by `keep_slots`. Watch the long-running nodes' txstore size fall and `outMsgDropped` on the feeders.
4. When both dagstores are sealed near the present, every node holds about two days of history.
5. Declare the two dagstores as feeders of each other: from then on each is a replica of the other (section 5.4). A further replica, for example for a public explorer, starts from either of them without loading the nodes.

Metrics on the dagstore: intake rate, copies per feeder, history walker slot, sealed canonical lag, branches and forks stored, missing pending / lost, pulls per feeder, timeouts, broad pulls, DB size. On the node: purge floor, slots purged, txstore size after GC.

## 13. Open questions

**Open: the cones of losing branches.** Section 8.2.1 found 86 branches per slot on history before library upgrade 7, and their cones adding 39% to the kept transactions. The choices: keep them as decided; keep every losing branch's own transaction but not the part of its cone outside canonical history, recorded as placeholders of a new state; or decide after measuring history after upgrade 7, where fewer branches are expected.

Decided on 2026-11-22:

- role advertisement through the heartbeat (section 5.1);
- one trusted announcement is enough before a node deletes (section 7.2);
- every branch kept and sealed (sections 6.5, 6.6), reopened above for the cost of their cones;
- libraries fetched with their proof and checked against the build; a dagstore not upgraded stops at the first transaction version it does not know, and nodes keep everything meanwhile (section 7.1);
- one SQLite file, created with incremental auto-vacuum so it can be pruned and split later (section 8.2); the prototype loading real history is the first implementation milestone;
- tip and branch table over peering only, no node API; a lineage is canonical only from `CanonicalDepthSlots`, at least 64, behind the LRB (section 6.2).
