"""
Use the `pytest-benchmark` library to benchmark Lance Graph queries.

Command used:
```
uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
```

Note: `query.get_engine()` uses an identity-based, module-level cache intended
for short-lived benchmark runs (session-scoped fixture). It's not designed as a
general-purpose cache for long-running processes.
"""

from pathlib import Path

import pytest

import query


@pytest.fixture(scope="session")
def graph_context():
    if not Path(query.GRAPH_ROOT).is_dir():
        raise RuntimeError("Missing graph_lance data. Run build_graph.py first.")
    cfg = query.build_config()
    datasets = query.load_datasets(query.GRAPH_ROOT)
    engine = None
    if getattr(query, "CypherEngine", None) is not None:
        engine = query.get_engine(cfg, datasets)
        # Verify engine caching returns the same instance.
        assert engine is query.get_engine(cfg, datasets)
    return cfg, datasets, engine


def test_benchmark_query1(benchmark, graph_context):
    cfg, datasets, engine = graph_context
    result = benchmark(query.run_query1, cfg, datasets, engine)
    result = result.to_dicts()

    assert len(result) == 3


def test_benchmark_query2(benchmark, graph_context):
    cfg, datasets, engine = graph_context
    result = benchmark(query.run_query2, cfg, datasets, engine)
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query3(benchmark, graph_context):
    cfg, datasets, engine = graph_context
    result = benchmark(query.run_query3, cfg, datasets, {"country": "United States"}, engine)
    result = result.to_dicts()

    assert len(result) == 5


def test_benchmark_query4(benchmark, graph_context):
    cfg, datasets, engine = graph_context
    result = benchmark(query.run_query4, cfg, datasets, {"age_lower": 30, "age_upper": 40}, engine)
    result = result.to_dicts()

    assert len(result) == 3


def test_benchmark_query5(benchmark, graph_context):
    cfg, datasets, engine = graph_context
    result = benchmark(
        query.run_query5,
        cfg,
        datasets,
        {
            "gender": "male",
            "city": "London",
            "country": "United Kingdom",
            "interest": "Fine Dining",
        },
        engine,
    )
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query6(benchmark, graph_context):
    cfg, datasets, engine = graph_context
    result = benchmark(
        query.run_query6,
        cfg,
        datasets,
        {
            "gender": "female",
            "interest": "Tennis",
        },
        engine,
    )
    result = result.to_dicts()

    assert len(result) == 5


def test_benchmark_query7(benchmark, graph_context):
    cfg, datasets, engine = graph_context
    result = benchmark(
        query.run_query7,
        cfg,
        datasets,
        {
            "country": "United States",
            "age_lower": 23,
            "age_upper": 30,
            "interest": "Photography",
        },
        engine,
    )
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query8(benchmark, graph_context):
    cfg, datasets, engine = graph_context
    result = benchmark(query.run_query8, cfg, datasets, engine)
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query9(benchmark, graph_context):
    cfg, datasets, engine = graph_context
    result = benchmark(query.run_query9, cfg, datasets, {"age_1": 50, "age_2": 25}, engine)
    result = result.to_dicts()

    assert len(result) == 1
