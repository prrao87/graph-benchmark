"""Benchmark query helpers for Lance Graph.

This module is intentionally simple:
- Build `GraphConfig` + load datasets once.
- Reuse a lightweight query context across benchmark queries.
"""

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lance
import polars as pl
import pyarrow as pa
from lance_graph import CypherQuery, GraphConfig

SCRIPT_ROOT = Path(__file__).resolve().parent
GRAPH_ROOT = SCRIPT_ROOT / "graph_lance"
NODE_LABELS = ("Person", "City", "State", "Country", "Interest")
REL_TYPES = ("FOLLOWS", "LIVES_IN", "HAS_INTEREST", "CITY_IN", "STATE_IN")


@dataclass(frozen=True)
class QueryContext:
    config: GraphConfig
    datasets: dict[str, pa.Table]


def build_config() -> GraphConfig:
    builder = GraphConfig.builder()
    for label in NODE_LABELS:
        builder = builder.with_node_label(label, "id")
    for rel_type in REL_TYPES:
        builder = builder.with_relationship(rel_type, "src", "dst")
    return builder.build()


def load_datasets(root: Path) -> dict[str, pa.Table]:
    datasets: dict[str, pa.Table] = {}
    for name in NODE_LABELS + REL_TYPES:
        datasets[name] = lance.dataset(str(root / f"{name}.lance")).to_table()
    return datasets


def to_polars(result: pa.Table) -> pl.DataFrame:
    if isinstance(result, pl.DataFrame):
        return result
    if isinstance(result, pa.Table):
        return pl.from_arrow(result)
    if isinstance(result, pa.RecordBatch):
        return pl.from_arrow(pa.Table.from_batches([result]))
    if hasattr(result, "to_pydict"):
        return pl.DataFrame(result.to_pydict())
    raise TypeError(f"Unsupported result type: {type(result)}")


def execute_query(
    context: QueryContext,
    query: str,
    params: dict[str, Any] | None = None,
) -> pl.DataFrame:
    cypher_query = CypherQuery(query).with_config(context.config)
    if params:
        for key, value in params.items():
            cypher_query = cypher_query.with_parameter(key, value)
    result = cypher_query.execute(context.datasets)
    return to_polars(result)


def rename_result(result: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
    if not mapping:
        return result
    return result.rename(mapping)


def _execute(
    context: QueryContext,
    idx: int,
    query: str,
    *,
    params: dict[str, Any] | None = None,
    rename: dict[str, str] | None = None,
) -> pl.DataFrame:
    print(f"\nQuery {idx}:\n {query}")
    result = execute_query(context, query, params=params)
    if rename:
        result = rename_result(result, rename)
    print(result)
    return result


def run_query1(context: QueryContext) -> pl.DataFrame:
    "Who are the top 3 most-followed persons in the network?"
    query = """
        MATCH (follower:Person)-[:FOLLOWS]->(person:Person)
        RETURN person.id AS personid, person.name AS name, count(follower.id) AS numfollowers
        ORDER BY numfollowers DESC LIMIT 3
    """
    return _execute(
        context,
        1,
        query,
        rename={"personid": "personID", "numfollowers": "numFollowers"},
    )


def run_query2(context: QueryContext) -> pl.DataFrame:
    "In which city does the most-followed person in the network live?"
    query = """
        MATCH (follower:Person)-[:FOLLOWS]->(person:Person)-[:LIVES_IN]->(city:City)
        RETURN person.name AS name, count(follower.id) as numfollowers, city.city AS city, city.state AS state, city.country AS country
        ORDER BY numfollowers DESC LIMIT 1
    """
    return _execute(context, 2, query, rename={"numfollowers": "numFollowers"})


def run_query3(context: QueryContext, params: dict[str, Any]) -> pl.DataFrame:
    "Which 5 cities in a particular country have the lowest average age in the network?"
    query = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)-[:CITY_IN]->(s:State)-[:STATE_IN]->(co:Country)
        WHERE co.country = $country
        RETURN c.city AS city, avg(p.age) AS averageage
        ORDER BY averageage LIMIT 5
    """
    return _execute(
        context,
        3,
        query,
        params=params,
        rename={"averageage": "averageAge"},
    )


def run_query4(
    context: QueryContext,
    params: dict[str, Any],
) -> pl.DataFrame:
    "How many persons between a certain age range are in each country?"
    query = """
        MATCH (p:Person)-[:LIVES_IN]->(ci:City)-[:CITY_IN]->(s:State)-[:STATE_IN]->(country:Country)
        WHERE p.age >= $age_lower AND p.age <= $age_upper
        RETURN country.country AS countries, count(country) AS personcounts
        ORDER BY personcounts DESC LIMIT 3
    """
    return _execute(
        context,
        4,
        query,
        params=params,
        rename={"personcounts": "personCounts"},
    )


def run_query5(
    context: QueryContext,
    params: dict[str, Any],
) -> pl.DataFrame:
    "How many men in a particular city have an interest in the same thing?"
    query = """
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest)
        MATCH (p)-[:LIVES_IN]->(c:City)
        WHERE i.interest = $interest
        AND p.gender = $gender
        AND c.city = $city AND c.country = $country
        RETURN count(p) AS numpersons
    """
    return _execute(
        context,
        5,
        query,
        params=params,
        rename={"numpersons": "numPersons"},
    )


def run_query6(
    context: QueryContext,
    params: dict[str, Any],
) -> pl.DataFrame:
    "Which city has the maximum number of people of a particular gender that share a particular interest"
    query = """
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest)
        MATCH (p)-[:LIVES_IN]->(c:City)
        WHERE i.interest = $interest
        AND p.gender = $gender
        RETURN count(p.id) AS numpersons, c.city AS city, c.country AS country
        ORDER BY numpersons DESC LIMIT 5
    """
    return _execute(
        context,
        6,
        query,
        params=params,
        rename={"numpersons": "numPersons"},
    )


def run_query7(
    context: QueryContext,
    params: dict[str, Any],
) -> pl.DataFrame:
    "Which U.S. state has the maximum number of persons between a specified age who enjoy a particular interest?"
    query = """
        MATCH (p:Person)-[:LIVES_IN]->(:City)-[:CITY_IN]->(s:State)
        MATCH (p)-[:HAS_INTEREST]->(i:Interest)
        WHERE p.age >= $age_lower AND p.age <= $age_upper AND s.country = $country
        AND i.interest = $interest
        RETURN count(p.id) AS numpersons, s.state AS state, s.country AS country
        ORDER BY numpersons DESC LIMIT 1
    """
    return _execute(
        context,
        7,
        query,
        params=params,
        rename={"numpersons": "numPersons"},
    )


def run_query8(
    context: QueryContext,
) -> pl.DataFrame:
    "How many second-degree paths exist in the graph?"
    query = """
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
        RETURN count(*) AS numpaths
    """
    return _execute(context, 8, query, rename={"numpaths": "numPaths"})


def run_query9(
    context: QueryContext,
    params: dict[str, Any],
) -> pl.DataFrame:
    "How many paths exist in the graph through persons below a certain age to persons above a certain age?"
    query = """
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
        WHERE b.age < $age_1 AND c.age > $age_2
        RETURN count(*) as numpaths
    """
    return _execute(
        context,
        9,
        query,
        params=params,
        rename={"numpaths": "numPaths"},
    )


def main() -> None:
    context = QueryContext(build_config(), load_datasets(GRAPH_ROOT))
    start = time.perf_counter()
    _ = run_query1(context)
    _ = run_query2(context)
    _ = run_query3(context, {"country": "United States"})
    _ = run_query4(context, {"age_lower": 30, "age_upper": 40})
    _ = run_query5(
        context,
        params={
            "gender": "male",
            "city": "London",
            "country": "United Kingdom",
            "interest": "Fine Dining",
        },
    )
    _ = run_query6(context, {"gender": "female", "interest": "Tennis"})
    _ = run_query7(
        context,
        {
            "country": "United States",
            "age_lower": 23,
            "age_upper": 30,
            "interest": "Photography",
        },
    )
    _ = run_query8(context)
    _ = run_query9(context, {"age_1": 50, "age_2": 25})
    elapsed = time.perf_counter() - start
    print(f"Queries completed in {elapsed:.4f}s")


if __name__ == "__main__":
    if not GRAPH_ROOT.is_dir():
        raise RuntimeError(f"Missing {GRAPH_ROOT}. Run build_graph.py first.")
    main()
