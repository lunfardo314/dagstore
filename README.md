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

Early development. Three programs exist:

- `dagstore-import`, an offline importer: it reads the transaction store of a stopped Proxima node and writes the history it holds into a new dagstore database, to test the database design and measure its size on real data (results in [SPEC.md](SPEC.md), section 8.2.1).
- `dagstore`, which serves such a database: the query API, a dashboard, and Proxima's DAG explorer pages. With a config file it also follows the network through one or more Proxima nodes, its *feeders*: it receives every transaction they take in, pulls what is missing, links and checks the history branch by branch and seals it ([SPEC.md](SPEC.md), section 6.7). A dagstore can also feed another one, which then keeps its own copy of the history (section 5.4).
- `dagstore-archive`, which moves old history out of a database into compressed archive files, *segments*, and builds a database from them (section 8.3).

Deleting history from the nodes' own storage, once a dagstore holds it, is built on the node side but has not been run in production yet.

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

The dashboard is at `/`; Prometheus metrics are at `/metrics` on the same address (the list is in [SPEC.md](SPEC.md), section 12).

Following the network takes a config file with a `dagstore` section and Proxima's `peering` section, the feeding node listed as a static peer and named in `peering.feeders`; the node, on its side, lists the dagstore as a static peer and in `peering.dagstores`:

```
dagstore:
  db: ./live.db
  listen: 127.0.0.1:8091
  library_api: http://127.0.0.1:8000   # a node API: the libraries of a new database, with their proofs
peering:
  host: {id_private_key: ..., id: ..., port: 4004}
  peers: {feeder: /ip4/127.0.0.1/udp/4000/quic-v1/p2p/<node peer ID>}
  feeders: [feeder]
  max_dynamic_peers: 0
  allow_local_ips: true
```

```
./dagstore -config dagstore.yaml
```

The dashboard is at `http://127.0.0.1:8090/`, the DAG explorer at `/dag_explorer`, and the API endpoints are listed in [SPEC.md](SPEC.md), section 11.

A dagstore that feeds another lists it in `peering.dagstores`, the other lists it in `peering.feeders`; two dagstores may feed each other.

Old history can be moved to archive segments, with the dagstore stopped for the last two steps:

```
./dagstore-archive export -db live.db -to <last slot> -out archive/    # reads beside a running dagstore
./dagstore-archive prune  -db live.db -segment archive/segment-<from>-<to>.json
./dagstore-archive import -db other.db -segment archive/segment-<from>-<to>.json -libraries_from live.db
```

`prune` deletes the range only after checking the segment against the database. A pruned transaction still shows in `/tx`, with the segment that holds it.

`proxi dagstore status|tx|branches|missing|export` queries a dagstore from the command line (Proxima's `proxi`, flag or profile key `dagstore.url`; not in a Proxima release yet).

The dagstore depends on Proxima, never the other way round, and `go.mod` pins the exact Proxima commit it is built against ([SPEC.md](SPEC.md), section 2.3). To work on both at once, point the build at a local Proxima checkout with an untracked workspace file: `go work init . <path to proxima>`.

## License

MIT, see [LICENSE](LICENSE).
