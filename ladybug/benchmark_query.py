"""
Use the `pytest-benchmark` library benchmark queries with warmup and iterations.
`uv add pytest-benchmark`

Command used:
```
uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
```
"""
import pytest
import real_ladybug as lb

import query


@pytest.fixture
def connection():
    db = lb.Database("social_network.lbug")
    conn = lb.Connection(db)
    yield conn


def test_benchmark_query1(benchmark, connection):
    result = benchmark(query.run_query1, connection)
    result = result.to_dicts()

    assert len(result) == 3


def test_benchmark_query2(benchmark, connection):
    result = benchmark(query.run_query2, connection)
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query3(benchmark, connection):
    result = benchmark(query.run_query3, connection, {"country": "United States"})
    result = result.to_dicts()

    assert len(result) == 5


def test_benchmark_query4(benchmark, connection):
    result = benchmark(query.run_query4, connection, {"age_lower": 30, "age_upper": 40})
    result = result.to_dicts()

    assert len(result) == 3


def test_benchmark_query5(benchmark, connection):
    result = benchmark(
        query.run_query5,
        connection,
        {
            "gender": "male",
            "city": "London",
            "country": "United Kingdom",
            "interest": "fine dining",
        },
    )
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query6(benchmark, connection):
    result = benchmark(
        query.run_query6,
        connection,
        {
            "gender": "female",
            "interest": "tennis",
        },
    )
    result = result.to_dicts()

    assert len(result) == 5


def test_benchmark_query7(benchmark, connection):
    result = benchmark(
        query.run_query7,
        connection,
        {
            "country": "United States",
            "age_lower": 23,
            "age_upper": 30,
            "interest": "photography",
        },
    )
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query8(benchmark, connection):
    result = benchmark(query.run_query8, connection)
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query9(benchmark, connection):
    result = benchmark(query.run_query9, connection, {"age_1": 50, "age_2": 25})
    result = result.to_dicts()

    assert len(result) == 1
