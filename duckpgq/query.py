"""
Run a series of SQL/PGQ queries on an existing DuckDB database via DuckPGQ.

Query parameters are passed through DuckDB session variables (`SET VARIABLE` +
`getvariable()`), because prepared-statement parameters are not currently
supported inside `GRAPH_TABLE (...)` clauses by the DuckPGQ parser.
"""

import time
from typing import Any

import duckdb
import polars as pl

from common import DB_PATH, connect


def set_params(conn: duckdb.DuckDBPyConnection, params: dict[str, Any]) -> None:
    for key, value in params.items():
        conn.execute(f"SET VARIABLE {key} = ?", [value])


def run_query1(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    "Who are the top 3 most-followed persons in the network?"
    query = """
        SELECT personID, name, count(*) AS numFollowers
        FROM GRAPH_TABLE (social_network
            MATCH (follower:Person)-[f:Follows]->(person:Person)
            COLUMNS (person.id AS personID, person.name AS name)
        )
        GROUP BY personID, name
        ORDER BY numFollowers DESC LIMIT 3;
    """
    print(f"\nQuery 1:\n {query}")
    result = conn.execute(query).pl()
    print(f"Top 3 most-followed persons:\n{result}")
    return result


def run_query2(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    "In which city does the most-followed person in the network live?"
    query = """
        WITH most_followed AS (
            SELECT personID, count(*) AS numFollowers
            FROM GRAPH_TABLE (social_network
                MATCH (follower:Person)-[f:Follows]->(person:Person)
                COLUMNS (person.id AS personID)
            )
            GROUP BY personID
            ORDER BY numFollowers DESC LIMIT 1
        )
        SELECT name, numFollowers, city, state, country
        FROM GRAPH_TABLE (social_network
            MATCH (person:Person)-[l:LivesIn]->(city:City)
            COLUMNS (
                person.id AS personID,
                person.name AS name,
                city.city AS city,
                city.state AS state,
                city.country AS country
            )
        )
        JOIN most_followed USING (personID);
    """
    print(f"\nQuery 2:\n {query}")
    result = conn.execute(query).pl()
    print(f"City in which most-followed person lives:\n{result}")
    return result


def run_query3(conn: duckdb.DuckDBPyConnection, params: dict[str, Any]) -> pl.DataFrame:
    "Which 5 cities in a particular country have the lowest average age in the network?"
    query = """
        SELECT city, avg(age) AS averageAge
        FROM GRAPH_TABLE (social_network
            MATCH (p:Person)-[l:LivesIn]->(c:City)-[ci:CityIn]->(s:State)-[si:StateIn]->(co:Country)
            WHERE co.country = getvariable('country')
            COLUMNS (c.city AS city, p.age AS age)
        )
        GROUP BY city
        ORDER BY averageAge LIMIT 5;
    """
    print(f"\nQuery 3:\n {query}")
    set_params(conn, params)
    result = conn.execute(query).pl()
    print(f"Cities with lowest average age in {params['country']}:\n{result}")
    return result


def run_query4(conn: duckdb.DuckDBPyConnection, params: dict[str, Any]) -> pl.DataFrame:
    "How many persons between a certain age range are in each country?"
    query = """
        SELECT countries, count(*) AS personCounts
        FROM GRAPH_TABLE (social_network
            MATCH (p:Person)-[l:LivesIn]->(ci:City)-[cin:CityIn]->(s:State)-[sin:StateIn]->(country:Country)
            WHERE p.age >= getvariable('age_lower') AND p.age <= getvariable('age_upper')
            COLUMNS (country.country AS countries)
        )
        GROUP BY countries
        ORDER BY personCounts DESC LIMIT 3;
    """
    print(f"\nQuery 4:\n {query}")
    set_params(conn, params)
    result = conn.execute(query).pl()
    print(f"Persons between ages {params['age_lower']}-{params['age_upper']} in each country:\n{result}")
    return result


def run_query5(conn: duckdb.DuckDBPyConnection, params: dict[str, Any]) -> pl.DataFrame:
    "How many men in a particular city have an interest in the same thing?"
    query = """
        SELECT count(*) AS numPersons
        FROM GRAPH_TABLE (social_network
            MATCH (p:Person)-[h:HasInterest]->(i:Interest), (p:Person)-[l:LivesIn]->(c:City)
            WHERE lower(i.interest) = lower(getvariable('interest'))
              AND lower(p.gender) = lower(getvariable('gender'))
              AND c.city = getvariable('city') AND c.country = getvariable('country')
            COLUMNS (p.id AS personID)
        );
    """
    print(f"\nQuery 5:\n {query}")
    set_params(conn, params)
    result = conn.execute(query).pl()
    print(
        f"Number of {params['gender']} users in {params['city']}, {params['country']} who have an interest in {params['interest']}:\n{result}"
    )
    return result


def run_query6(conn: duckdb.DuckDBPyConnection, params: dict[str, Any]) -> pl.DataFrame:
    "Which city has the maximum number of people of a particular gender that share a particular interest"
    query = """
        SELECT count(*) AS numPersons, city, country
        FROM GRAPH_TABLE (social_network
            MATCH (p:Person)-[h:HasInterest]->(i:Interest), (p:Person)-[l:LivesIn]->(c:City)
            WHERE lower(i.interest) = lower(getvariable('interest'))
              AND lower(p.gender) = lower(getvariable('gender'))
            COLUMNS (c.city AS city, c.country AS country)
        )
        GROUP BY city, country
        ORDER BY numPersons DESC LIMIT 5;
    """
    print(f"\nQuery 6:\n {query}")
    set_params(conn, params)
    result = conn.execute(query).pl()
    print(
        f"City with the most {params['gender']} users who have an interest in {params['interest']}:\n{result}"
    )
    return result


def run_query7(conn: duckdb.DuckDBPyConnection, params: dict[str, Any]) -> pl.DataFrame:
    "Which U.S. state has the maximum number of persons between a specified age who enjoy a particular interest?"
    query = """
        SELECT count(*) AS numPersons, state, country
        FROM GRAPH_TABLE (social_network
            MATCH (p:Person)-[l:LivesIn]->(c:City)-[ci:CityIn]->(s:State), (p:Person)-[h:HasInterest]->(i:Interest)
            WHERE p.age >= getvariable('age_lower') AND p.age <= getvariable('age_upper')
              AND s.country = getvariable('country')
              AND lower(i.interest) = lower(getvariable('interest'))
            COLUMNS (s.state AS state, s.country AS country)
        )
        GROUP BY state, country
        ORDER BY numPersons DESC LIMIT 1;
    """
    print(f"\nQuery 7:\n {query}")
    set_params(conn, params)
    result = conn.execute(query).pl()
    print(
        f"""
        State in {params['country']} with the most users between ages {params['age_lower']}-{params['age_upper']} who have an interest in {params['interest']}:\n{result}
        """
    )
    return result


def run_query8(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    "How many second-degree paths exist in the graph?"
    query = """
        SELECT count(*) AS numPaths
        FROM GRAPH_TABLE (social_network
            MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
            COLUMNS (a.id AS personID)
        );
    """
    print(f"\nQuery 8:\n {query}")
    result = conn.execute(query).pl()
    print(
        f"""
        Number of second-degree paths:\n{result}
        """
    )
    return result


def run_query9(conn: duckdb.DuckDBPyConnection, params: dict[str, Any]) -> pl.DataFrame:
    "How many paths exist in the graph through persons below a certain age to persons above a certain age?"
    query = """
        SELECT count(*) AS numPaths
        FROM GRAPH_TABLE (social_network
            MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
            WHERE b.age < getvariable('age_1') AND c.age > getvariable('age_2')
            COLUMNS (a.id AS personID)
        );
    """
    print(f"\nQuery 9:\n {query}")
    set_params(conn, params)
    result = conn.execute(query).pl()
    print(
        f"""
        Number of paths through persons below {params['age_1']} to persons above {params['age_2']}:\n{result}
        """
    )
    return result


def main(conn: duckdb.DuckDBPyConnection) -> None:
    start = time.perf_counter()
    _ = run_query1(conn)
    _ = run_query2(conn)
    _ = run_query3(conn, params={"country": "United States"})
    _ = run_query4(conn, params={"age_lower": 30, "age_upper": 40})
    _ = run_query5(
        conn,
        params={
            "gender": "male",
            "city": "London",
            "country": "United Kingdom",
            "interest": "fine dining",
        },
    )
    _ = run_query6(conn, params={"gender": "female", "interest": "tennis"})
    _ = run_query7(
        conn,
        params={
            "country": "United States",
            "age_lower": 23,
            "age_upper": 30,
            "interest": "photography",
        },
    )
    _ = run_query8(conn)
    _ = run_query9(conn, params={"age_1": 50, "age_2": 25})
    elapsed = time.perf_counter() - start
    print(f"Queries completed in {elapsed:.4f}s")


if __name__ == "__main__":
    if not DB_PATH.is_file():
        raise RuntimeError(
            f"Missing DuckDB database at {DB_PATH}. "
            "Create it with `uv run build_graph.py` from the `duckpgq` directory."
        )
    with connect(DB_PATH) as CONNECTION:
        main(CONNECTION)
