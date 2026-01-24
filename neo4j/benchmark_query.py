"""
Use the `pytest-benchmark` library benchmark queries with warmup and iterations.
`uv add pytest-benchmark`

Command used:
```
uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
```
"""
import os

import pytest
from dotenv import load_dotenv
from neo4j import GraphDatabase

import query

load_dotenv()


@pytest.fixture(scope="session")
def session():
    URI = "bolt://localhost:7687"
    NEO4J_USER = os.environ.get("NEO4J_USER")
    NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
    with GraphDatabase.driver(URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as driver:
        with driver.session(database="neo4j") as session:
            yield session


def test_benchmark_query1(benchmark, session):
    result = benchmark(query.run_query1, session)
    result = result.to_dicts()

    assert len(result) == 3


def test_benchmark_query2(benchmark, session):
    result = benchmark(query.run_query2, session)
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query3(benchmark, session):
    result = benchmark(query.run_query3, session, "United States")
    result = result.to_dicts()

    assert len(result) == 5


def test_benchmark_query4(benchmark, session):
    result = benchmark(query.run_query4, session, 30, 40)
    result = result.to_dicts()

    assert len(result) == 3

def test_benchmark_query5(benchmark, session):
    result = benchmark(query.run_query5, session, "male", "London", "United Kingdom", "fine dining")
    result = result.to_dicts()

    assert len(result) == 1
    # assert result[0]["numPersons"] == 52


def test_benchmark_query6(benchmark, session):
    result = benchmark(query.run_query6, session, "female", "tennis")
    result = result.to_dicts()

    assert len(result) == 5


def test_benchmark_query7(benchmark, session):
    result = benchmark(query.run_query7, session, "United States", 23, 30, "photography")
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query8(benchmark, session):
    result = benchmark(query.run_query8, session)
    result = result.to_dicts()

    assert len(result) == 1


def test_benchmark_query9(benchmark, session):
    result = benchmark(query.run_query9, session, 50, 25)
    result = result.to_dicts()

    assert len(result) == 1
