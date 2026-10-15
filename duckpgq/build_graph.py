"""
Build a DuckDB database with a DuckPGQ property graph of the social network.

Nodes and edges are read from the Parquet files under `data/output` into plain
DuckDB tables, and a SQL/PGQ property graph is declared on top of them so that
the tables can be queried with `GRAPH_TABLE (... MATCH ...)` via DuckPGQ.
"""

import time
from pathlib import Path

import duckdb

from common import DB_PATH, PROPERTY_GRAPH_NAME, connect

DATA_PATH = Path(__file__).resolve().parents[1] / "data"
NODES_PATH = DATA_PATH / "output" / "nodes"
EDGES_PATH = DATA_PATH / "output" / "edges"


def create_node_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"""
        CREATE TABLE Person (
            id INT64 PRIMARY KEY,
            name VARCHAR,
            gender VARCHAR,
            birthday DATE,
            age INT64,
            isMarried BOOLEAN
        );
        INSERT INTO Person
            SELECT id, name, gender, birthday, age, isMarried
            FROM read_parquet('{NODES_PATH}/persons.parquet');
        """
    )
    conn.execute(
        f"""
        CREATE TABLE City (
            id INT64 PRIMARY KEY,
            city VARCHAR,
            state VARCHAR,
            country VARCHAR,
            lat DOUBLE,
            lon DOUBLE,
            population INT32
        );
        INSERT INTO City
            SELECT id, city, state, country, lat, lng, population
            FROM read_parquet('{NODES_PATH}/cities.parquet');
        """
    )
    conn.execute(
        f"""
        CREATE TABLE State (
            id INT64 PRIMARY KEY,
            state VARCHAR,
            country VARCHAR
        );
        INSERT INTO State
            SELECT id, state, country FROM read_parquet('{NODES_PATH}/states.parquet');
        """
    )
    conn.execute(
        f"""
        CREATE TABLE Country (
            id INT64 PRIMARY KEY,
            country VARCHAR
        );
        INSERT INTO Country
            SELECT id, country FROM read_parquet('{NODES_PATH}/countries.parquet');
        """
    )
    conn.execute(
        f"""
        CREATE TABLE Interest (
            id INT64 PRIMARY KEY,
            interest VARCHAR
        );
        INSERT INTO Interest
            SELECT id, interest FROM read_parquet('{NODES_PATH}/interests.parquet');
        """
    )


def create_edge_tables(conn: duckdb.DuckDBPyConnection) -> None:
    # The Parquet files use `from`/`to` as column names, which are reserved
    # words in SQL. Rename them to `src`/`dst` on the way in.
    edges = {
        "Follows": "follows.parquet",
        "LivesIn": "lives_in.parquet",
        "HasInterest": "interested_in.parquet",
        "CityIn": "city_in.parquet",
        "StateIn": "state_in.parquet",
    }
    for table, filename in edges.items():
        conn.execute(
            f"""
            CREATE TABLE {table} AS
                SELECT "from"::INT64 AS src, "to"::INT64 AS dst
                FROM read_parquet('{EDGES_PATH}/{filename}');
            """
        )


def create_property_graph(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"""
        CREATE PROPERTY GRAPH {PROPERTY_GRAPH_NAME}
        VERTEX TABLES (
            Person,
            City,
            State,
            Country,
            Interest
        )
        EDGE TABLES (
            Follows
                SOURCE KEY (src) REFERENCES Person (id)
                DESTINATION KEY (dst) REFERENCES Person (id)
                LABEL Follows,
            LivesIn
                SOURCE KEY (src) REFERENCES Person (id)
                DESTINATION KEY (dst) REFERENCES City (id)
                LABEL LivesIn,
            HasInterest
                SOURCE KEY (src) REFERENCES Person (id)
                DESTINATION KEY (dst) REFERENCES Interest (id)
                LABEL HasInterest,
            CityIn
                SOURCE KEY (src) REFERENCES City (id)
                DESTINATION KEY (dst) REFERENCES State (id)
                LABEL CityIn,
            StateIn
                SOURCE KEY (src) REFERENCES State (id)
                DESTINATION KEY (dst) REFERENCES Country (id)
                LABEL StateIn
        );
        """
    )


def main(conn: duckdb.DuckDBPyConnection) -> None:
    nodes_start = time.perf_counter()
    create_node_tables(conn)
    nodes_elapsed = time.perf_counter() - nodes_start
    print(f"Nodes loaded in {nodes_elapsed:.4f}s")

    edges_start = time.perf_counter()
    create_edge_tables(conn)
    edges_elapsed = time.perf_counter() - edges_start
    print(f"Edges loaded in {edges_elapsed:.4f}s")

    create_property_graph(conn)
    print(f"Created property graph '{PROPERTY_GRAPH_NAME}'")
    print("Successfully loaded nodes and edges into DuckDB")


if __name__ == "__main__":
    # Delete the database file each time to start from a clean slate
    DB_PATH.unlink(missing_ok=True)
    Path(f"{DB_PATH}.wal").unlink(missing_ok=True)
    with connect(DB_PATH) as CONNECTION:
        main(CONNECTION)
