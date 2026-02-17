"""Benchmark query helpers for Lance Graph.

This module is intentionally simple:
- Build `GraphConfig` + load datasets once.
- Create a single `CypherEngine` and reuse it across queries.
"""

import time
from pathlib import Path
from typing import Any

import lance
import polars as pl
import pyarrow as pa
from lance_graph import CypherEngine, GraphConfig

SCRIPT_ROOT = Path(__file__).resolve().parent
GRAPH_ROOT = SCRIPT_ROOT / "graph_lance"
NODE_LABELS = ("Person", "City", "State", "Country", "Interest")
REL_TYPES = ("FOLLOWS", "LIVES_IN", "HAS_INTEREST", "CITY_IN", "STATE_IN")


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


def format_cypher_value(value: Any) -> str:
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    return str(value)


def apply_params(query: str, params: dict[str, Any]) -> str:
    # Lance Graph currently doesn't parse $param placeholders; inline values here
    # so the query string is fully concrete before parsing.
    for key, value in params.items():
        query = query.replace(f"${key}", format_cypher_value(value))
    return query


def execute_query(
    engine: CypherEngine,
    query: str,
    params: dict[str, Any] | None = None,
) -> pl.DataFrame:
    if params:
        # Inline params instead of using CypherQuery.with_parameter, which isn't
        # respected by the current parser.
        query = apply_params(query, params)
    result = engine.execute(query)
    return to_polars(result)


def rename_result(result: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
    if not mapping:
        return result
    return result.rename(mapping)


def _execute(
    engine: CypherEngine,
    idx: int,
    query: str,
    *,
    params: dict[str, Any] | None = None,
    rename: dict[str, str] | None = None,
) -> pl.DataFrame:
    print(f"\nQuery {idx}:\n {query}")
    result = execute_query(engine, query, params=params)
    if rename:
        result = rename_result(result, rename)
    print(result)
    return result


def run_query1(engine: CypherEngine) -> pl.DataFrame:
    "Who are the top 3 most-followed persons in the network?"
    query = """
        MATCH (follower:Person)-[:FOLLOWS]->(person:Person)
        RETURN person.id AS personid, person.name AS name, count(follower.id) AS numfollowers
        ORDER BY numfollowers DESC LIMIT 3
    """
    return _execute(
        engine,
        1,
        query,
        rename={"personid": "personID", "numfollowers": "numFollowers"},
    )


def run_query2(engine: CypherEngine) -> pl.DataFrame:
    "In which city does the most-followed person in the network live?"
    query = """
        MATCH (follower:Person)-[:FOLLOWS]->(person:Person)-[:LIVES_IN]->(city:City)
        RETURN person.name AS name, count(follower.id) as numfollowers, city.city AS city, city.state AS state, city.country AS country
        ORDER BY numfollowers DESC LIMIT 1
    """
    return _execute(engine, 2, query, rename={"numfollowers": "numFollowers"})


def run_query3(engine: CypherEngine, params: dict[str, Any]) -> pl.DataFrame:
    "Which 5 cities in a particular country have the lowest average age in the network?"
    query = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)-[:CITY_IN]->(s:State)-[:STATE_IN]->(co:Country)
        WHERE co.country = $country
        RETURN c.city AS city, avg(p.age) AS averageage
        ORDER BY averageage LIMIT 5
    """
    return _execute(
        engine,
        3,
        query,
        params=params,
        rename={"averageage": "averageAge"},
    )


def run_query4(
    engine: CypherEngine,
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
        engine,
        4,
        query,
        params=params,
        rename={"personcounts": "personCounts"},
    )


def run_query5(
    engine: CypherEngine,
    params: dict[str, Any],
) -> pl.DataFrame:
    "How many men in a particular city have an interest in the same thing?"
    query = """
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest),
              (p)-[:LIVES_IN]->(c:City)
        WHERE tolower(i.interest) = tolower($interest)
        AND tolower(p.gender) = tolower($gender)
        AND c.city = $city AND c.country = $country
        RETURN count(p) AS numpersons
    """
    return _execute(
        engine,
        5,
        query,
        params=params,
        rename={"numpersons": "numPersons"},
    )


def run_query6(
    engine: CypherEngine,
    params: dict[str, Any],
) -> pl.DataFrame:
    "Which city has the maximum number of people of a particular gender that share a particular interest"
    query = """
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest),
              (p)-[:LIVES_IN]->(c:City)
        WHERE tolower(i.interest) = tolower($interest)
        AND tolower(p.gender) = tolower($gender)
        RETURN count(p.id) AS numpersons, c.city AS city, c.country AS country
        ORDER BY numpersons DESC LIMIT 5
    """
    return _execute(
        engine,
        6,
        query,
        params=params,
        rename={"numpersons": "numPersons"},
    )


def run_query7(
    engine: CypherEngine,
    params: dict[str, Any],
) -> pl.DataFrame:
    "Which U.S. state has the maximum number of persons between a specified age who enjoy a particular interest?"
    query = """
        MATCH (p:Person)-[:LIVES_IN]->(:City)-[:CITY_IN]->(s:State),
              (p)-[:HAS_INTEREST]->(i:Interest)
        WHERE p.age >= $age_lower AND p.age <= $age_upper AND s.country = $country
        AND tolower(i.interest) = tolower($interest)
        RETURN count(p.id) AS numpersons, s.state AS state, s.country AS country
        ORDER BY numpersons DESC LIMIT 1
    """
    return _execute(
        engine,
        7,
        query,
        params=params,
        rename={"numpersons": "numPersons"},
    )


def run_query8(
    engine: CypherEngine,
) -> pl.DataFrame:
    "How many second-degree paths exist in the graph?"
    query = """
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
        RETURN count(*) AS numpaths
    """
    return _execute(engine, 8, query, rename={"numpaths": "numPaths"})


def run_query9(
    engine: CypherEngine,
    params: dict[str, Any],
) -> pl.DataFrame:
    "How many paths exist in the graph through persons below a certain age to persons above a certain age?"
    query = """
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
        WHERE b.age < $age_1 AND c.age > $age_2
        RETURN count(*) as numpaths
    """
    return _execute(
        engine,
        9,
        query,
        params=params,
        rename={"numpaths": "numPaths"},
    )


def main() -> None:
    cfg = build_config()
    datasets = load_datasets(GRAPH_ROOT)
    # Build catalog once so the benchmark timing focuses on query execution.
    engine = CypherEngine(cfg, datasets)
    start = time.perf_counter()
    _ = run_query1(engine)
    _ = run_query2(engine)
    _ = run_query3(engine, {"country": "United States"})
    _ = run_query4(engine, {"age_lower": 30, "age_upper": 40})
    _ = run_query5(
        engine,
        params={
            "gender": "male",
            "city": "London",
            "country": "United Kingdom",
            "interest": "fine dining",
        },
    )
    _ = run_query6(engine, {"gender": "female", "interest": "tennis"})
    _ = run_query7(
        engine,
        {
            "country": "United States",
            "age_lower": 23,
            "age_upper": 30,
            "interest": "photography",
        },
    )
    _ = run_query8(engine)
    _ = run_query9(engine, {"age_1": 50, "age_2": 25})
    elapsed = time.perf_counter() - start
    print(f"Queries completed in {elapsed:.4f}s")


if __name__ == "__main__":
    if not GRAPH_ROOT.is_dir():
        raise RuntimeError(f"Missing {GRAPH_ROOT}. Run build_graph.py first.")
    main()
