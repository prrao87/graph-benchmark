# lance-csr

This section describes how to build and query a graph of the social network data
using [lance-csr](https://github.com/hyperdimensionalcomputingai/lance-csr), a fork
of lance-graph. The graph is stored as Lance datasets on disk, loaded into memory as
Arrow tables, and queried through a single `CypherEngine` that caches its catalog
across queries. The Cypher queries are identical to the ones in `lance_graph/`.

> [!NOTE]
> All timing numbers shown below are on an M5 MacBook Pro (10-core CPU: 4 performance + 6 efficiency cores) with 24 GB of RAM, running macOS 26.6.2.

## Setup

```sh
uv add lance-csr
```

lance-csr 0.11.0 needs `pylance` 13.x to write Lance datasets.

## Build graph

The script `build_graph.py` converts the Parquet node/edge datasets under
`data/output` into Lance datasets under `lance_csr/graph_lance`.

```sh
uv run build_graph.py
```

## Query graph

```sh
uv run query.py
```

The results match `lance_graph/query.py`, except that the two cities tied at 67
in query 6 can come back in either order.

## Run the benchmark

```sh
uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
```

The engine runs with the experimental in-memory CSR adjacency
(`CypherEngine(..., use_csr=True)`), which is off by default in lance-csr. The index
is built once with the engine, before the timed queries.

0.11.0 runs at the same speed as 0.9.1 on this benchmark, apart from q8 and q9, which
are about 3–5% faster. Back-to-back runs of both versions on the same machine were
within noise on the other queries.

Reading the Lance datasets lazily
instead of loading them into memory was slower on 0.5.x, by roughly 5–15%.
