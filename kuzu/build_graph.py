import asyncio
import time
from pathlib import Path

import kuzu

DATA_PATH = Path(__file__).resolve().parents[1] / "data"
NODES_PATH = DATA_PATH / "output" / "nodes"
EDGES_PATH = DATA_PATH / "output" / "edges"


async def create_person_node_table(conn: kuzu.AsyncConnection) -> None:
    await conn.execute(
        """
        CREATE NODE TABLE
            Person(
                id INT64,
                name STRING,
                gender STRING,
                birthday DATE,
                age INT64,
                isMarried BOOLEAN,
                PRIMARY KEY (id)
            )
        """
    )


async def create_city_node_table(conn: kuzu.AsyncConnection) -> None:
    await conn.execute(
        """
        CREATE NODE TABLE
            City(
                id INT64,
                city STRING,
                state STRING,
                country STRING,
                lat DOUBLE,
                lon DOUBLE,
                population INT32,
                PRIMARY KEY (id)
            )
        """
    )


async def create_state_node_table(conn: kuzu.AsyncConnection) -> None:
    await conn.execute(
        """
        CREATE NODE TABLE
            State(
                id INT64,
                state STRING,
                country STRING,
                PRIMARY KEY (id)
            )
        """
    )


async def create_country_node_table(conn: kuzu.AsyncConnection) -> None:
    await conn.execute(
        """
        CREATE NODE TABLE
            Country(
                id INT64,
                country STRING,
                PRIMARY KEY (id)
            )
        """
    )


async def create_interest_node_table(conn: kuzu.AsyncConnection) -> None:
    await conn.execute(
        """
        CREATE NODE TABLE
            Interest(
                id INT64,
                interest STRING,
                PRIMARY KEY (id)
            )
        """
    )


async def create_edge_tables(conn: kuzu.AsyncConnection) -> None:
    # Create edge schemas
    await conn.execute("CREATE REL TABLE Follows(FROM Person TO Person)")
    await conn.execute("CREATE REL TABLE LivesIn(FROM Person TO City)")
    await conn.execute("CREATE REL TABLE HasInterest(FROM Person TO Interest)")
    await conn.execute("CREATE REL TABLE CityIn(FROM City TO State)")
    await conn.execute("CREATE REL TABLE StateIn(FROM State TO Country)")


async def main(conn: kuzu.AsyncConnection) -> None:
    nodes_start = time.perf_counter()
    # Nodes
    await create_person_node_table(conn)
    await create_city_node_table(conn)
    await create_state_node_table(conn)
    await create_country_node_table(conn)
    await create_interest_node_table(conn)
    await conn.execute(f"COPY Person FROM '{NODES_PATH}/persons.parquet';")
    await conn.execute(f"COPY City FROM '{NODES_PATH}/cities.parquet';")
    await conn.execute(f"COPY State FROM '{NODES_PATH}/states.parquet';")
    await conn.execute(f"COPY Country FROM '{NODES_PATH}/countries.parquet';")
    await conn.execute(f"COPY Interest FROM '{NODES_PATH}/interests.parquet';")
    nodes_elapsed = time.perf_counter() - nodes_start
    print(f"Nodes loaded in {nodes_elapsed:.4f}s")

    edges_start = time.perf_counter()
    # Edges
    await create_edge_tables(conn)
    await conn.execute(f"COPY Follows FROM '{EDGES_PATH}/follows.parquet';")
    await conn.execute(f"COPY LivesIn FROM '{EDGES_PATH}/lives_in.parquet';")
    await conn.execute(f"COPY HasInterest FROM '{EDGES_PATH}/interested_in.parquet';")
    await conn.execute(f"COPY CityIn FROM '{EDGES_PATH}/city_in.parquet';")
    await conn.execute(f"COPY StateIn FROM '{EDGES_PATH}/state_in.parquet';")
    edges_elapsed = time.perf_counter() - edges_start
    print(f"Edges loaded in {edges_elapsed:.4f}s")

    print("Successfully loaded nodes and edges into Kuzu")


if __name__ == "__main__":
    DB_NAME = "social_network.kuzu"
    db_path = Path(f"./{DB_NAME}")
    db_path.unlink(missing_ok=True)
    # Create database
    db = kuzu.Database(f"./{DB_NAME}")
    CONNECTION = kuzu.AsyncConnection(db)

    asyncio.run(main(CONNECTION))
