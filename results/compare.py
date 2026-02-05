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


def normalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


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


def resolve_color(system: str, color_map: dict[str, str]) -> str | None:
    normalized_system = normalize_name(system)
    normalized_map = {normalize_name(key): value for key, value in color_map.items()}
    for key in sorted(normalized_map, key=len, reverse=True):
        if key in normalized_system:
            return normalized_map[key]
    return None


def plot_results(
    systems: list[str],
    queries: list[str],
    values: list[list[float | None]],
    output_path: Path,
) -> None:
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError as exc:
        raise SystemExit(
            "matplotlib (and numpy) are required for plotting. "
            "Install them and retry."
        ) from exc

    color_map = {
        "ladybug": "#d62728",
        "lance-graph": "#7f3fbf",
        "neo4j": "#1f77b4",
        "kuzu": "#ff7f0e",
    }
    x = np.arange(len(queries))
    bar_width = 0.8 / max(len(systems), 1)
    for idx, system in enumerate(systems):
        offsets = x + (idx - (len(systems) - 1) / 2) * bar_width
        series = []
        for row in values:
            value = row[idx]
            series.append(float("nan") if value is None else value)
        plt.bar(
            offsets,
            series,
            width=bar_width,
            label=system,
            color=resolve_color(system, color_map),
        )

    plt.xticks(x, queries, rotation=45, ha="right")
    plt.ylabel("Time (ms, log scale)")
    plt.yscale("log")
    plt.title("Graph Benchmark Timing (Lower is Better)")
    plt.legend(loc="best")
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    print(f"\nWrote plot to {output_path}")


def main() -> None:
    results_dir = Path(__file__).resolve().parent
    files = sorted(results_dir.glob("*.txt"))
    if not files:
        raise SystemExit("No .txt files found in results directory.")

    neo4j_system: str | None = None
    for idx, path in enumerate(files):
        if "neo4j" in path.stem:
            files.insert(0, files.pop(idx))
            neo4j_system = path.stem
            break

    systems = [path.stem for path in files]
    system_results = {path.stem: parse_benchmark_file(path) for path in files}
    all_queries = sorted(
        {query for results in system_results.values() for query in results},
        key=sort_query_key,
    )

    headers = ["Query"] + [f"{system} (ms)" for system in systems]
    rows = []
    plot_queries: list[str] = []
    plot_values: list[list[float | None]] = []
    for query in all_queries:
        display_query = query.replace("test_benchmark_query", "q")
        row = [display_query]
        neo4j_value = (
            system_results.get(neo4j_system, {}).get(query)
            if neo4j_system
            else None
        )
        series: list[float | None] = []
        for system in systems:
            value = system_results[system].get(query)
            series.append(value)
            if value is None:
                row.append("n/a")
                continue
            value_text = f"{format(value, f'.{ROUND_MS_DECIMALS}f')}ms"
            if (
                neo4j_system
                and system != neo4j_system
                and neo4j_value is not None
                and neo4j_value > 0
                and value > 0
            ):
                speedup = neo4j_value / value
                value_text = f"{value_text} ({speedup:.{SPEEDUP_DECIMALS}f}x)"
            row.append(value_text)
        rows.append(row)
        plot_queries.append(display_query)
        plot_values.append(series)

    print(to_markdown_table(headers, rows))
    plot_results(
        systems,
        plot_queries,
        plot_values,
        results_dir / "benchmark_plot.png",
    )


if __name__ == "__main__":
    main()
