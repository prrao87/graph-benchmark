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

lance-csr 0.5.1 needs `pylance` 13.x to write Lance datasets.

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

The engine runs with its default settings. The experimental in-memory CSR adjacency
(`CypherEngine(..., use_csr=True)`) was slower on this benchmark: about 52 ms vs 14 ms
on q1, 172 ms vs 33 ms on q2, 30 ms vs 21 ms on q8 and 24 ms vs 20 ms on q9. That run
is kept in `results/archive/lance-csr-0.5.1-use-csr.txt`. Reading the Lance datasets lazily
instead of loading them into memory was also slower, by roughly 5–15%.
