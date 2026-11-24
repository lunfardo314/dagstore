# TODO

What the dagstore does not do yet, measured against [SPEC.md](SPEC.md). Section numbers refer to it. Ordered by how soon each item matters.

## Before a real deployment

- **Transactions of a library not held** (7.1). They are dropped with an alarm; the spec wants them stored `unparsed` and parsed once the library arrives. Today they come back by pull after the restart that loads the library. Library refresh itself is built (SPEC 6.7).
- **Several feeders** (6.2, 6.3). The tip is the last LRB answer, not the one most feeders agree on. The pull ladder and the per-feeder budgets are written for several feeders but have run with one only.
- **Metrics** (12). Nothing is exported to Prometheus; `/status`, `/stats` and the dashboard are the only views.
- **First sync from genesis.** Only run so far from a snapshot, a few hundred slots of history. A full first sync fetches the branch list of the whole history from feeders whose txstore reaches genesis: the same code, not yet run at that scale.

## Missing parts of the design

- **Replication** (5.4). A dagstore cannot feed another: no push from its intake, no answers to `PullLRB` or `PullBranchList` from its database.
- **Broad pull and pulls from other dagstores** (6.3, steps 3 and 4 of the ladder). A transaction no feeder has is declared lost. It matters only for canonical cones.
- **Real forks** (6.6). If the feeders' lineage forks below the canonical tip, the tip stops with an error instead of renumbering.
- **Archive segments and pruning** (8.3). No export of old ranges to compressed segments, no import of a segment, no deleting a range; `PRAGMA incremental_vacuum` never runs.
- **Compression of transaction bytes** (8.2.1). Raw bytes are about three quarters of the database; zstd with a dictionary is the first thing to try.
- **`proxi dagstore` commands** (10.9), a client of the query API.
- **Dashboard**: per-feeder figures (pushes, pulls served, timeouts, last contact) and replicas (11.1).

## Node side, built but not rehearsed

- **Purging a node's txstore** from the dagstore's heartbeat (7.2), behind the node's `txstore.purge` key. Never run against a dagstore.

## Open decisions

- **The cones of losing branches** (13): keep them all, as decided, or only the branch transactions. On a well-connected node after library upgrade 7, most of a history pass serves losing branches' cones; to be measured over a longer run before deciding.
- **`OrphanPurgeSlots`** (6.5): rests on the assumption that node policy bounds how late a transaction can still be included. To verify in the node's code.
- **Access to the query API**: unauthenticated, including the bulk `/export`. Keep it local or behind a proxy until decided.

## Smaller

- Move the offline importer onto proxima's `txstore/conewalk`, if still worth it now that the row derivation is shared.
- Missing records do not yet say which feeders were tried, only how many rounds.
