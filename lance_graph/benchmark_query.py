"""Benchmarks for Lance Graph queries (via `pytest-benchmark`)."""

import pytest

import query


@pytest.fixture(scope="session")
def graph_context():
    if not query.GRAPH_ROOT.is_dir():
        raise RuntimeError("Missing graph_lance data. Run build_graph.py first.")
    cfg = query.build_config()
    datasets = query.load_datasets(query.GRAPH_ROOT)
    return query.CypherEngine(cfg, datasets)


def test_benchmark_query1(benchmark, graph_context):
    engine = graph_context
    result = benchmark(query.run_query1, engine)
    result = result.to_dicts()

    assert len(result) == 3


def test_benchmark_query2(benchmark, graph_context):
    engine = graph_context
    result = benchmark(query.run_query2, engine)
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query3(benchmark, graph_context):
    engine = graph_context
    result = benchmark(query.run_query3, engine, {"country": "United States"})
    result = result.to_dicts()

    assert len(result) == 5


def test_benchmark_query4(benchmark, graph_context):
    engine = graph_context
    result = benchmark(query.run_query4, engine, {"age_lower": 30, "age_upper": 40})
    result = result.to_dicts()

    assert len(result) == 3


def test_benchmark_query5(benchmark, graph_context):
    engine = graph_context
    result = benchmark(
        query.run_query5,
        engine,
        {
            "gender": "male",
            "city": "London",
            "country": "United Kingdom",
            "interest": "Fine Dining",
        },
    )
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query6(benchmark, graph_context):
    engine = graph_context
    result = benchmark(
        query.run_query6,
        engine,
        {
            "gender": "female",
            "interest": "Tennis",
        },
    )
    result = result.to_dicts()

    assert len(result) == 5


def test_benchmark_query7(benchmark, graph_context):
    engine = graph_context
    result = benchmark(
        query.run_query7,
        engine,
        {
            "country": "United States",
            "age_lower": 23,
            "age_upper": 30,
            "interest": "Photography",
        },
    )
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query8(benchmark, graph_context):
    engine = graph_context
    result = benchmark(query.run_query8, engine)
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query9(benchmark, graph_context):
    engine = graph_context
    result = benchmark(query.run_query9, engine, {"age_1": 50, "age_2": 25})
    result = result.to_dicts()

    assert len(result) == 1
