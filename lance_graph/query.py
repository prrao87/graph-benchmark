"""
Run benchmark queries against Lance datasets.

- Lance stores each node/edge label as a separate on-disk columnar dataset.
- GraphConfig maps those datasets into a logical graph schema so Cypher
  queries can join across files.
- Results are printed as Polars DataFrames to match the Ladybug output.
"""

import time
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
    query: str,
    cfg: GraphConfig,
    datasets: dict[str, pa.Table],
    params: dict[str, Any] | None = None,
) -> pl.DataFrame:
    if params:
        # Inline params instead of using CypherQuery.with_parameter, which isn't
        # respected by the current parser.
        query = apply_params(query, params)
    cypher = CypherQuery(query)
    result = cypher.with_config(cfg).execute(datasets)
    return to_polars(result)


def rename_result(result: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
    if not mapping:
        return result
    return result.rename(mapping)


def run_query1(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Who are the top 3 most-followed persons in the network?"
    query = """
        MATCH (follower:Person)-[:FOLLOWS]->(person:Person)
        RETURN person.id AS personid, person.name AS name, count(follower.id) AS numfollowers
        ORDER BY numfollowers DESC LIMIT 3
    """
    print(f"\nQuery 1:\n {query}")
    result = execute_query(query, cfg, datasets)
    result = rename_result(result, {"personid": "personID", "numfollowers": "numFollowers"})
    print(f"Top 3 most-followed persons:\n{result}")
    return result


def run_query2(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "In which city does the most-followed person in the network live?"
    query = """
        MATCH (follower:Person)-[:FOLLOWS]->(person:Person)-[:LIVES_IN]->(city:City)
        RETURN person.name AS name, count(follower.id) as numfollowers, city.city AS city, city.state AS state, city.country AS country
        ORDER BY numfollowers DESC LIMIT 1
    """
    print(f"\nQuery 2:\n {query}")
    result = execute_query(query, cfg, datasets)
    result = rename_result(result, {"numfollowers": "numFollowers"})
    print(f"City in which most-followed person lives:\n{result}")
    return result


def run_query3(
    cfg: GraphConfig, datasets: dict[str, pa.Table], params: dict[str, Any]
) -> pl.DataFrame:
    "Which 5 cities in a particular country have the lowest average age in the network?"
    query = """
        MATCH (p:Person)-[:LIVES_IN]->(c:City)-[:CITY_IN]->(s:State)-[:STATE_IN]->(co:Country)
        WHERE co.country = $country
        RETURN c.city AS city, avg(p.age) AS averageage
        ORDER BY averageage LIMIT 5
    """
    print(f"\nQuery 3:\n {query}")
    result = execute_query(query, cfg, datasets, params=params)
    result = rename_result(result, {"averageage": "averageAge"})
    print(f"Cities with lowest average age in {params['country']}:\n{result}")
    return result


def run_query4(
    cfg: GraphConfig, datasets: dict[str, pa.Table], params: dict[str, Any]
) -> pl.DataFrame:
    "How many persons between a certain age range are in each country?"
    query = """
        MATCH (p:Person)-[:LIVES_IN]->(ci:City)-[:CITY_IN]->(s:State)-[:STATE_IN]->(country:Country)
        WHERE p.age >= $age_lower AND p.age <= $age_upper
        RETURN country.country AS countries, count(country) AS personcounts
        ORDER BY personcounts DESC LIMIT 3
    """
    print(f"\nQuery 4:\n {query}")
    result = execute_query(query, cfg, datasets, params=params)
    result = rename_result(result, {"personcounts": "personCounts"})
    print(
        f"Persons between ages {params['age_lower']}-{params['age_upper']} in each country:\n{result}"
    )
    return result


def run_query5(
    cfg: GraphConfig, datasets: dict[str, pa.Table], params: dict[str, Any]
) -> pl.DataFrame:
    "How many men in a particular city have an interest in the same thing?"
    query = """
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest)
        MATCH (p)-[:LIVES_IN]->(c:City)
        WHERE lower(i.interest) = lower($interest)
        AND lower(p.gender) = lower($gender)
        AND c.city = $city AND c.country = $country
        RETURN count(p) AS numpersons
    """
    print(f"\nQuery 5:\n {query}")
    result = execute_query(query, cfg, datasets, params=params)
    result = rename_result(result, {"numpersons": "numPersons"})
    print(
        f"Number of {params['gender']} users in {params['city']}, {params['country']} who have an interest in {params['interest']}:\n{result}"
    )
    return result


def run_query6(
    cfg: GraphConfig, datasets: dict[str, pa.Table], params: dict[str, Any]
) -> pl.DataFrame:
    "Which city has the maximum number of people of a particular gender that share a particular interest"
    query = """
        MATCH (p:Person)-[:HAS_INTEREST]->(i:Interest)
        MATCH (p)-[:LIVES_IN]->(c:City)
        WHERE lower(i.interest) = lower($interest)
        AND lower(p.gender) = lower($gender)
        RETURN count(p.id) AS numpersons, c.city AS city, c.country AS country
        ORDER BY numpersons DESC LIMIT 5
    """
    print(f"\nQuery 6:\n {query}")
    result = execute_query(query, cfg, datasets, params=params)
    result = rename_result(result, {"numpersons": "numPersons"})
    print(
        f"City with the most {params['gender']} users who have an interest in {params['interest']}:\n{result}"
    )
    return result


def run_query7(
    cfg: GraphConfig, datasets: dict[str, pa.Table], params: dict[str, Any]
) -> pl.DataFrame:
    "Which U.S. state has the maximum number of persons between a specified age who enjoy a particular interest?"
    query = """
        MATCH (p:Person)-[:LIVES_IN]->(:City)-[:CITY_IN]->(s:State)
        MATCH (p)-[:HAS_INTEREST]->(i:Interest)
        WHERE p.age >= $age_lower AND p.age <= $age_upper AND s.country = $country
        AND lower(i.interest) = lower($interest)
        RETURN count(p.id) AS numpersons, s.state AS state, s.country AS country
        ORDER BY numpersons DESC LIMIT 1
    """
    print(f"\nQuery 7:\n {query}")
    result = execute_query(query, cfg, datasets, params=params)
    result = rename_result(result, {"numpersons": "numPersons"})
    print(
        f"""
        State in {params["country"]} with the most users between ages {params["age_lower"]}-{params["age_upper"]} who have an interest in {params["interest"]}:\n{result}
        """
    )
    return result


def run_query8(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "How many second-degree paths exist in the graph?"
    query = """
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
        RETURN count(*) AS numpaths
    """
    print(f"\nQuery 8:\n {query}")
    result = execute_query(query, cfg, datasets)
    result = rename_result(result, {"numpaths": "numPaths"})
    print(
        f"""
        Number of second-degree paths:\n{result}
        """
    )
    return result


def run_query9(
    cfg: GraphConfig, datasets: dict[str, pa.Table], params: dict[str, Any]
) -> pl.DataFrame:
    "How many paths exist in the graph through persons below a certain age to persons above a certain age?"
    query = """
        MATCH (a:Person)-[r1:FOLLOWS]->(b:Person)-[r2:FOLLOWS]->(c:Person)
        WHERE b.age < $age_1 AND c.age > $age_2
        RETURN count(*) as numpaths
    """

    print(f"\nQuery 9:\n {query}")
    result = execute_query(query, cfg, datasets, params=params)
    result = rename_result(result, {"numpaths": "numPaths"})
    print(
        f"""
        Number of paths through persons below {params["age_1"]} to persons above {params["age_2"]}:\n{result}
        """
    )
    return result


def main() -> None:
    cfg = build_config()
    datasets = load_datasets(GRAPH_ROOT)
    start = time.perf_counter()
    _ = run_query1(cfg, datasets)
    _ = run_query2(cfg, datasets)
    _ = run_query3(cfg, datasets, params={"country": "United States"})
    _ = run_query4(cfg, datasets, params={"age_lower": 30, "age_upper": 40})
    _ = run_query5(
        cfg,
        datasets,
        params={
            "gender": "male",
            "city": "London",
            "country": "United Kingdom",
            "interest": "fine dining",
        },
    )
    _ = run_query6(cfg, datasets, params={"gender": "female", "interest": "tennis"})
    _ = run_query7(
        cfg,
        datasets,
        params={
            "country": "United States",
            "age_lower": 23,
            "age_upper": 30,
            "interest": "photography",
        },
    )
    _ = run_query8(cfg, datasets)
    _ = run_query9(cfg, datasets, params={"age_1": 50, "age_2": 25})
    elapsed = time.perf_counter() - start
    print(f"Queries completed in {elapsed:.4f}s")


if __name__ == "__main__":
    if not GRAPH_ROOT.is_dir():
        raise RuntimeError(f"Missing {GRAPH_ROOT}. Run build_graph.py first.")
    main()
