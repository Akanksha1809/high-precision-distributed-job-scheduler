"""Parses the JSONL output of monitor_resources.sh (raw `docker stats --format json`
lines) into per-container avg/max CPU% and memory usage for the benchmark report.

Usage:
    python -m benchmarks.summarize_resources --run-dir benchmarks/results/latest
"""
import argparse
import json
import re
import statistics
from pathlib import Path


def _parse_mem_mb(mem_usage: str) -> float:
    # e.g. "123.4MiB / 1.943GiB" -> 123.4
    used = mem_usage.split("/")[0].strip()
    m = re.match(r"([\d.]+)\s*([A-Za-z]+)", used)
    if not m:
        return 0.0
    value, unit = float(m.group(1)), m.group(2).lower()
    if unit.startswith("gi") or unit.startswith("g"):
        return value * 1024
    if unit.startswith("ki") or unit.startswith("k"):
        return value / 1024
    return value  # MiB/MB


def summarize(run_dir: Path) -> dict:
    samples_path = run_dir / "resource_samples.jsonl"
    if not samples_path.exists() or samples_path.stat().st_size == 0:
        return {"note": "no resource samples found — did monitor_resources.sh run?"}

    by_container: dict[str, dict[str, list[float]]] = {}
    for line in samples_path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = d.get("Name", "unknown")
        cpu = float(d.get("CPUPerc", "0%").rstrip("%") or 0)
        mem_mb = _parse_mem_mb(d.get("MemUsage", "0MiB / 0MiB"))
        entry = by_container.setdefault(name, {"cpu": [], "mem_mb": []})
        entry["cpu"].append(cpu)
        entry["mem_mb"].append(mem_mb)

    return {
        name: {
            "cpu_pct_avg": round(statistics.mean(v["cpu"]), 2) if v["cpu"] else None,
            "cpu_pct_max": round(max(v["cpu"]), 2) if v["cpu"] else None,
            "mem_mb_avg": round(statistics.mean(v["mem_mb"]), 1) if v["mem_mb"] else None,
            "mem_mb_max": round(max(v["mem_mb"]), 1) if v["mem_mb"] else None,
            "samples": len(v["cpu"]),
        }
        for name, v in by_container.items()
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--run-dir", type=Path, default=Path("benchmarks/results/latest"))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    report = summarize(args.run_dir)
    (args.run_dir / "resource_summary.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
