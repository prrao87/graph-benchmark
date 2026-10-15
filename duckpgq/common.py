"""
Shared helpers for connecting to DuckDB with the DuckPGQ extension loaded.
"""

import os
from pathlib import Path

import duckdb

SCRIPT_ROOT = Path(__file__).resolve().parent
DB_NAME = "social_network.duckdb"
DB_PATH = SCRIPT_ROOT / DB_NAME
PROPERTY_GRAPH_NAME = "social_network"

# By default the DuckPGQ community extension is installed and loaded. To test a
# locally built extension (e.g. from DuckPGQ's `main` branch), point this
# environment variable at the `duckpgq.duckdb_extension` binary instead.
EXTENSION_PATH_ENV = "DUCKPGQ_EXTENSION_PATH"


def connect(db_path: Path | str = DB_PATH, **kwargs) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection and load DuckPGQ."""
    extension_path = os.environ.get(EXTENSION_PATH_ENV)
    config = dict(kwargs.pop("config", {}))
    if extension_path:
        config["allow_unsigned_extensions"] = "true"
    conn = duckdb.connect(str(db_path), config=config, **kwargs)
    load_duckpgq(conn, extension_path)
    return conn


def load_duckpgq(conn: duckdb.DuckDBPyConnection, extension_path: str | None = None) -> None:
    if extension_path:
        conn.execute(f"LOAD '{extension_path}';")
    else:
        conn.execute("INSTALL duckpgq FROM community;")
        conn.execute("LOAD duckpgq;")


def duckpgq_version(conn: duckdb.DuckDBPyConnection) -> str:
    row = conn.execute(
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'duckpgq'"
    ).fetchone()
    return row[0] if row else "unknown"
