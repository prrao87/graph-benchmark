"""
Use the `pytest-benchmark` library to benchmark Lance Graph queries.

Command used:
```
uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
```
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
    return cfg, datasets


def test_benchmark_query1(benchmark, graph_context):
    cfg, datasets = graph_context
    result = benchmark(query.run_query1, cfg, datasets)
    result = result.to_dicts()

    assert len(result) == 3


def test_benchmark_query2(benchmark, graph_context):
    cfg, datasets = graph_context
    result = benchmark(query.run_query2, cfg, datasets)
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query3(benchmark, graph_context):
    cfg, datasets = graph_context
    result = benchmark(query.run_query3, cfg, datasets, {"country": "United States"})
    result = result.to_dicts()

    assert len(result) == 5


def test_benchmark_query4(benchmark, graph_context):
    cfg, datasets = graph_context
    result = benchmark(
        query.run_query4, cfg, datasets, {"age_lower": 30, "age_upper": 40}
    )
    result = result.to_dicts()

    assert len(result) == 3


def test_benchmark_query5(benchmark, graph_context):
    cfg, datasets = graph_context
    result = benchmark(
        query.run_query5,
        cfg,
        datasets,
        {
            "gender": "male",
            "city": "London",
            "country": "United Kingdom",
            "interest": "fine dining",
        },
    )
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query6(benchmark, graph_context):
    cfg, datasets = graph_context
    result = benchmark(
        query.run_query6,
        cfg,
        datasets,
        {
            "gender": "female",
            "interest": "tennis",
        },
    )
    result = result.to_dicts()

    assert len(result) == 5


def test_benchmark_query7(benchmark, graph_context):
    cfg, datasets = graph_context
    result = benchmark(
        query.run_query7,
        cfg,
        datasets,
        {
            "country": "United States",
            "age_lower": 23,
            "age_upper": 30,
            "interest": "photography",
        },
    )
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query8(benchmark, graph_context):
    cfg, datasets = graph_context
    result = benchmark(query.run_query8, cfg, datasets)
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query9(benchmark, graph_context):
    cfg, datasets = graph_context
    result = benchmark(query.run_query9, cfg, datasets, {"age_1": 50, "age_2": 25})
    result = result.to_dicts()

    assert len(result) == 1
