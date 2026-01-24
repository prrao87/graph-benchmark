#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path

HEADER_RE = re.compile(r"Name \(time in (?P<unit>[^)]+)\)")
UNIT_TO_MS = {
    "s": 1000.0,
    "ms": 1.0,
    "us": 0.001,
    "µs": 0.001,
    "ns": 0.000001,
}
ROUND_MS_DECIMALS = 0
SPEEDUP_DECIMALS = 1


def parse_benchmark_file(path: Path) -> dict[str, float]:
    unit: str | None = None
    unit_scale: float | None = None
    means_ms: dict[str, float] = {}
    for line in path.read_text().splitlines():
        if unit_scale is None:
            match = HEADER_RE.search(line)
            if match:
                unit = match.group("unit").strip()
                unit_scale = UNIT_TO_MS.get(unit)
                if unit_scale is None:
                    raise ValueError(f"Unsupported time unit '{unit}' in {path.name}")
                continue
        if not line.startswith("test_"):
            continue
        if unit_scale is None:
            raise ValueError(f"Missing header with time unit in {path.name}")
        columns = re.split(r"\s{2,}", line.strip())
        if len(columns) < 4:
            continue
        name = columns[0]
        mean_value = float(columns[3].split()[0])
        means_ms[name] = mean_value * unit_scale
    if unit_scale is None:
        raise ValueError(f"Missing header with time unit in {path.name}")
    return means_ms


def sort_query_key(name: str) -> tuple[int, int | str]:
    match = re.search(r"query(\d+)", name)
    if match:
        return (0, int(match.group(1)))
    return (1, name)


def to_markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def main() -> None:
    results_dir = Path(__file__).resolve().parent
    files = sorted(results_dir.glob("*.txt"))
    if not files:
        raise SystemExit("No .txt files found in results directory.")

    systems = sorted(path.stem for path in files)
    if "neo4j" in systems:
        systems.remove("neo4j")
        systems.insert(0, "neo4j")
    system_results = {path.stem: parse_benchmark_file(path) for path in files}
    all_queries = sorted(
        {query for results in system_results.values() for query in results},
        key=sort_query_key,
    )

    headers = ["Query"] + [f"{system} (ms)" for system in systems]
    rows = []
    for query in all_queries:
        display_query = query.replace("test_benchmark_query", "q")
        row = [display_query]
        neo4j_value = system_results.get("neo4j", {}).get(query)
        for system in systems:
            value = system_results[system].get(query)
            if value is None:
                row.append("n/a")
                continue
            value_text = f"{format(value, f'.{ROUND_MS_DECIMALS}f')}ms"
            if (
                system != "neo4j"
                and neo4j_value is not None
                and neo4j_value > 0
                and value > 0
            ):
                speedup = neo4j_value / value
                value_text = f"{value_text} ({speedup:.{SPEEDUP_DECIMALS}f}x)"
            row.append(value_text)
        rows.append(row)

    print(to_markdown_table(headers, rows))


if __name__ == "__main__":
    main()
