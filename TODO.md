# TODO

What the dagstore does not do yet, measured against [SPEC.md](SPEC.md). Section numbers refer to it. Ordered by how soon each item matters.

## Before a real deployment

- **First sync from genesis.** Only run so far from a snapshot, a few hundred slots of history. A full first sync fetches the branch list of the whole history from feeders whose txstore reaches genesis: the same code, not yet run at that scale.
- **Replication and archive segments over the network.** Both are checked on real history on one machine (`live/TestReplication`, `live/TestArchiveRoundTrip`), not yet between two hosts or with a node feeding the source.

## Missing parts of the design

- **Renumbering below the starting branch** (6.6). A real fork whose fork point lies below the starting branch, deeper than the history held, is refused with an error.
- **Importing a segment below the history held.** A segment is imported into a new database or appended after its canonical tip; restoring a pruned range under a database's starting branch is not supported.
- **Retraining the compression dictionary** (8.2.1). The first dictionary is used for good; the schema allows later ones, but nothing decides when to train one.

## Node side, built but not rehearsed

- **Purging a node's txstore** from the dagstore's heartbeat (7.2), behind the node's `txstore.purge` key. Never run against a dagstore.

## Documentation and presentation

- **User-facing documentation and an operations guide**, for this repository (README) and for the public docs site: what the dagstore is, how to set it up beside a node, the config keys, the dashboard and the query API, archiving, replication, and running and recovering it.
- **Code comments**: explain the why throughout and point to SPEC.md where a rule comes from, naming the spec and the topic rather than section numbers, which drift.
- **A better-looking dashboard**: clearer layout and hierarchy, the important numbers first, light and dark themes kept.

## Open decisions

- **The cones of losing branches** (13): keep them all, as decided, or only the branch transactions. On a well-connected node after library upgrade 7, most of a history pass serves losing branches' cones; to be measured over a longer run before deciding.
- **Access to the query API**: unauthenticated, including the bulk `/export`. Keep it local or behind a proxy until decided.

## Smaller

- Move the offline importer onto proxima's `txstore/conewalk`, if still worth it now that the row derivation is shared.
- Counters of the live process are package-wide, so two dagstores in one process (the replication test) share them in `/status`; per-node counters if that ever matters beyond tests.
