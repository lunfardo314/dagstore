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

Early development. What exists is an offline importer: it reads the transaction store of a stopped Proxima node and writes the history it holds into a new dagstore database, to test the database design and measure its size on real data (results in [SPEC.md](SPEC.md), section 8.2.1). The dagstore node itself is not built yet.

## How to run

The importer, on a copy of a node's databases (the node must be stopped; opening its state database may write to it):

```
go build -o dagstore-import ./cmd/dagstore-import
./dagstore-import -txstore <node dir>/proximadb.txstore -state <node dir>/proximadb -out history.db
```

It prints what it found and the size of every table. During development the module builds against a local checkout of the Proxima repository (the `replace` line in `go.mod`).

## License

MIT, see [LICENSE](LICENSE).
