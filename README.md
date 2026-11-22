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

Early development. Two programs exist:

- `dagstore-import`, an offline importer: it reads the transaction store of a stopped Proxima node and writes the history it holds into a new dagstore database, to test the database design and measure its size on real data (results in [SPEC.md](SPEC.md), section 8.2.1).
- `dagstore`, which serves such a database read-only: the query API, a dashboard, and Proxima's DAG explorer pages. Receiving transactions from nodes, following the network and sealing history are not built yet.

## How to run

The importer, on a copy of a node's databases (the node must be stopped; opening its state database may write to it):

```
go build -o dagstore-import ./cmd/dagstore-import
./dagstore-import -txstore <node dir>/proximadb.txstore -state <node dir>/proximadb -out history.db
```

It prints what it found and the size of every table. It also stores the ledger's libraries in the database, so the database can be read without the node. For a database imported before that, `-libraries_into history.db` (with `-state`) adds them.

Serving a database:

```
go build -o dagstore ./cmd/dagstore
./dagstore -db history.db -listen 127.0.0.1:8090
```

It logs to standard error: startup, a stats line every 10 minutes, server errors and requests slower than 2 s (`-slow_request`); `-log_level debug` adds every request.

The dashboard is at `http://127.0.0.1:8090/`, the DAG explorer at `/dag_explorer`, and the API endpoints are listed in [SPEC.md](SPEC.md), section 11.

The dagstore depends on Proxima, never the other way round, and `go.mod` pins the exact Proxima commit it is built against ([SPEC.md](SPEC.md), section 2.3). To work on both at once, point the build at a local Proxima checkout with an untracked workspace file: `go work init . <path to proxima>`.

## License

MIT, see [LICENSE](LICENSE).
