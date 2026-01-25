"""
Docstring for lance_graph.build_graph
"""
"""
Builds Lance datasets for the benchmark graph from Parquet inputs.

Reads node/edge Parquet files under `data/output`, normalizes edge endpoint
columns to `src`/`dst`, casts them to the referenced node id types, and writes
one Lance dataset per label/relationship into `lance_graph/graph_lance`.
"""

from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import lance

SCRIPT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_ROOT.parent
GRAPH_ROOT = SCRIPT_ROOT / "graph_lance"
NODES_ROOT = REPO_ROOT / "data" / "output" / "nodes"
EDGES_ROOT = REPO_ROOT / "data" / "output" / "edges"


# --- simple helpers ---


def write_lance(table: pa.Table, name: str) -> str:
    GRAPH_ROOT.mkdir(parents=True, exist_ok=True)
    path = GRAPH_ROOT / f"{name}.lance"
    lance.write_dataset(table, str(path), mode="overwrite")
    return str(path)


def require_column(t: pa.Table, col: str, where: str) -> None:
    if col not in t.column_names:
        raise ValueError(f"Missing column '{col}' in {where}. Found: {t.column_names}")


def assert_no_nulls(arr: pa.Array, where: str) -> None:
    if pc.any(pc.is_null(arr)).as_py():
        raise ValueError(f"Nulls found in {where}")


def cast_to(arr: pa.Array, typ: pa.DataType) -> pa.Array:
    if arr.type == typ:
        return arr
    return pc.cast(arr, typ)


def normalize_columns(t: pa.Table, where: str) -> pa.Table:
    """
    Column names are case-insensitive in lance-graph's internals
    due to DataFusion, so we lowercase all column names here.
    """
    # DataFusion lowercases unquoted identifiers during planning, so we
    # lowercase columns here to avoid "No field named ..." errors on camelCase.
    names = t.column_names
    lower = [name.lower() for name in names]
    if len(set(lower)) != len(lower):
        raise ValueError(
            f"Lowercasing column names would create duplicates in {where}: {names}"
        )
    if lower != names:
        t = t.rename_columns(lower)
    return t


def normalize_edge_columns(t: pa.Table, where: str) -> pa.Table:
    """
    Column names are case-insensitive in lance-graph's internals
    due to DataFusion, so we lowercase all column names here.
    """
    names = t.column_names
    if "from" in names and "src" in names:
        raise ValueError(f"Both 'from' and 'src' present in {where}")
    if "to" in names and "dst" in names:
        raise ValueError(f"Both 'to' and 'dst' present in {where}")

    if "from" in names or "to" in names:
        new_names = []
        for name in names:
            if name == "from":
                new_names.append("src")
            elif name == "to":
                new_names.append("dst")
            else:
                new_names.append(name)
        t = t.rename_columns(new_names)
    return t


def load_nodes(path: Path, id_col: str = "id") -> tuple[pa.Table, pa.DataType]:
    t = pq.read_table(path)
    t = normalize_columns(t, str(path))
    require_column(t, id_col, path)
    assert_no_nulls(t[id_col], f"{path}:{id_col}")
    return t, t.schema.field(id_col).type


def load_edges(path: Path, src_type: pa.DataType, dst_type: pa.DataType) -> pa.Table:
    t = pq.read_table(path)
    t = normalize_columns(t, str(path))
    t = normalize_edge_columns(t, str(path))

    require_column(t, "src", path)
    require_column(t, "dst", path)

    # cast to match node id types
    src = cast_to(t["src"], src_type)
    dst = cast_to(t["dst"], dst_type)
    assert_no_nulls(src, f"{path}:src")
    assert_no_nulls(dst, f"{path}:dst")

    # replace columns (preserve any extra edge props)
    cols = []
    for name in t.column_names:
        if name == "src":
            cols.append(src)
        elif name == "dst":
            cols.append(dst)
        else:
            cols.append(t[name])
    return pa.table(cols, names=t.column_names)


def main() -> None:
    # ---- load nodes (capture the id type per label) ----
    persons, person_id_type = load_nodes(NODES_ROOT / "persons.parquet")
    cities, city_id_type = load_nodes(NODES_ROOT / "cities.parquet")
    states, state_id_type = load_nodes(NODES_ROOT / "states.parquet")
    countries, country_id_type = load_nodes(NODES_ROOT / "countries.parquet")
    interests, interest_id_type = load_nodes(NODES_ROOT / "interests.parquet")

    write_lance(persons, "Person")
    write_lance(cities, "City")
    write_lance(states, "State")
    write_lance(countries, "Country")
    write_lance(interests, "Interest")

    # ---- load edges (cast src/dst to referenced node id types) ----
    follows = load_edges(
        EDGES_ROOT / "follows.parquet",
        person_id_type,
        person_id_type,
    )
    lives_in = load_edges(
        EDGES_ROOT / "lives_in.parquet",
        person_id_type,
        city_id_type,
    )
    city_in = load_edges(
        EDGES_ROOT / "city_in.parquet",
        city_id_type,
        state_id_type,
    )
    state_in = load_edges(
        EDGES_ROOT / "state_in.parquet",
        state_id_type,
        country_id_type,
    )
    has_interest = load_edges(
        EDGES_ROOT / "interested_in.parquet",
        person_id_type,
        interest_id_type,
    )

    write_lance(follows, "FOLLOWS")
    write_lance(lives_in, "LIVES_IN")
    write_lance(city_in, "CITY_IN")
    write_lance(state_in, "STATE_IN")
    write_lance(has_interest, "HAS_INTEREST")

    print(f"Wrote Lance datasets to: {GRAPH_ROOT.resolve()}")


if __name__ == "__main__":
    main()
