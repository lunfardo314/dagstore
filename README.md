# dagstore

Long-term storage of the transaction history of the [Proxima](https://github.com/lunfardo314/proxima) ledger.

## What it is for

Proxima has no blocks. Its transactions form a graph, the *tangle*: each transaction points to the earlier transactions whose outputs it spends and to the ones it endorses. Every Proxima node keeps every transaction it ever received, so the storage of a node that has run from the start only grows, and it also holds many transactions that never became part of the ledger's history.

The dagstore is a special node that keeps that history once, from the first transaction on, in a database (SQLite) built for it:

- it keeps only the transactions that belong to the ledger's agreed history, with every link between them checked, and records explicitly anything that is missing;
- it answers queries about the history through an API and shows it on a dashboard;
- once it holds a part of the history, ordinary nodes may delete that part from their own storage and keep only the last day or two.

The dagstore does not take part in consensus, keeps no ledger state and validates nothing beyond the transactions' structure and signatures: what it stores is what the network has already agreed on.

The full design is in [SPEC.md](SPEC.md).

## Status

Design only. Nothing is built yet.

## How to run

To be written once there is something to run.

## License

MIT, see [LICENSE](LICENSE).
