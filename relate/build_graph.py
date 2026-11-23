import argparse
import shutil
import time
from pathlib import Path

import polars as pl
import relate

DATA_PATH = Path(__file__).resolve().parents[1] / "data"
NODES_PATH = DATA_PATH / "output" / "nodes"
EDGES_PATH = DATA_PATH / "output" / "edges"
DB_PATH = Path(__file__).resolve().parent / "social_network.rl"

# (label, key, parquet file)
NODES = [
    ("Person", "personID", "persons"),
    ("Interest", "interestID", "interests"),
    ("City", "cityID", "cities"),
    ("State", "stateID", "states"),
    ("Country", "countryID", "countries"),
]
# (from label, from key, rel type, to label, to key, parquet file)
EDGES = [
    ("Person", "personID", "FOLLOWS", "Person", "personID", "follows"),
    ("Person", "personID", "HAS_INTEREST", "Interest", "interestID", "interested_in"),
    ("Person", "personID", "LIVES_IN", "City", "cityID", "lives_in"),
    ("City", "cityID", "CITY_IN", "State", "stateID", "city_in"),
    ("State", "stateID", "STATE_IN", "Country", "countryID", "state_in"),
]


def batches(df: pl.DataFrame, size: int):
    for chunk in df.iter_slices(size):
        yield chunk.to_dicts()


def main(batch_size: int) -> None:
    # Start from an empty store so CREATE (not MERGE) is safe
    if DB_PATH.is_dir():
        shutil.rmtree(DB_PATH)
    else:
        DB_PATH.unlink(missing_ok=True)
    conn = relate.connect(str(DB_PATH))

    for label, key, _ in NODES:
        conn.execute(f"CREATE CONSTRAINT {key} IF NOT EXISTS FOR (n:{label}) REQUIRE n.{key} IS UNIQUE")

    start = time.perf_counter()
    for label, key, name in NODES:
        df = pl.read_parquet(NODES_PATH / f"{name}.parquet")
        for rows in batches(df, batch_size):
            conn.execute(f"UNWIND $rows AS row CREATE (n:{label}) SET n = row, n.{key} = row.id", {"rows": rows})
        print(f"Created {len(df)} {label} nodes")
    print(f"Nodes loaded in {time.perf_counter() - start:.4f}s")

    start = time.perf_counter()
    for src, src_key, rel, dst, dst_key, name in EDGES:
        df = pl.read_parquet(EDGES_PATH / f"{name}.parquet")
        for rows in batches(df, batch_size):
            conn.execute(
                f"""
                UNWIND $rows AS row
                MATCH (a:{src} {{{src_key}: row.from}})
                MATCH (b:{dst} {{{dst_key}: row.to}})
                CREATE (a)-[:{rel}]->(b)
                """,
                {"rows": rows},
            )
        print(f"Created {len(df)} {rel} edges")
    print(f"Edges loaded in {time.perf_counter() - start:.4f}s")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Build Relate graph from files")
    parser.add_argument("--batch_size", "-b", type=int, default=50_000, help="Rows per UNWIND statement")
    main(parser.parse_args().batch_size)
