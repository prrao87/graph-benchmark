# Relate

This section describes how to build and query a graph of the social network data
using [Relate](https://github.com/relatedb/relate), Neo4j's open source embedded graph
database written in Rust. Relate runs in-process (no server) and speaks Cypher, so the
queries are identical to the ones in `neo4j/`.

> [!NOTE]
> All timing numbers shown below are on an M5 MacBook Pro (10-core CPU: 4 performance + 6 efficiency cores) with 24 GB of RAM, running macOS 26.6.2.

## Setup

```sh
uv add relate-db   # the import is `import relate`
```

## Build graph

The script `build_graph.py` reads the Parquet node/edge files under `data/output` and
writes them to a Relate store at `relate/social_network.rl`, recreating it on each run.
Rows go in as parameterized `UNWIND ... CREATE` statements (50K rows each by default),
after a uniqueness constraint on each node label's ID.

```sh
uv run build_graph.py
```

```
Nodes loaded in 2.7s
Edges loaded in 30.5s
```

## Query graph

```sh
uv run query.py
```

The results match `neo4j/query.py`, except that the two cities tied at 65 in query 6
can come back in either order.

## Run the benchmark

The benchmark opens the store read-only, once, before the timed queries.

```sh
uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
```
