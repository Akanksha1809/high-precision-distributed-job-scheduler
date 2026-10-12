"""Benchmark workload generator.

Creates jobs via POST /jobs against a running Chronos API at a controlled rate, with
a configurable mix of near-future vs long-future scheduling, a controlled payload
size, and a realistic (lognormal) distribution of execution durations fed to the
`bench.task` handler. Records per-request API latency and writes a machine-readable
summary plus a sidecar file of every created job id (for the post-run lateness
analysis in measure_pipeline.py).

This script measures exactly one thing: API ingestion throughput and latency. It
makes no claim about dispatch, queue, or completion throughput — those are measured
separately, after the run, from the database and from Prometheus.

Usage:
    python -m benchmarks.generate_load --duration-seconds 30 --rate 200 \\
        --concurrency 50 --output-dir benchmarks/results/run1
"""
import argparse
import asyncio
import json
import math
import random
import statistics
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx


@dataclass
class RequestRecord:
    sent_at: float
    latency_ms: float
    status_code: int
    job_id: str | None
    scheduled_at: str
    bucket: str  # "near_future" | "long_future" | "burst"


@dataclass
class RunResult:
    config: dict
    started_at: str
    ended_at: str
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    status_code_counts: dict = field(default_factory=dict)
    records: list[RequestRecord] = field(default_factory=list)


def _lognormal_duration_ms(mean_ms: float, sigma: float) -> float:
    # Parameterized so the *median* duration is approximately mean_ms, with a
    # realistic right tail (a few requests take much longer than the median) rather
    # than every job taking an identical, unrealistic fixed time.
    mu = math.log(max(mean_ms, 1.0))
    return max(1.0, random.lognormvariate(mu, sigma))


def _padded_payload(target_bytes: int, duration_ms: float) -> dict:
    base = {"duration_ms": round(duration_ms, 1)}
    base_size = len(json.dumps(base).encode("utf-8"))
    filler_len = max(0, target_bytes - base_size - len('"filler":""'))
    if filler_len > 0:
        base["filler"] = "x" * filler_len
    return base


async def _send_one(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    bucket: str,
    scheduled_at: datetime,
    payload: dict,
) -> RequestRecord:
    body = {
        "task_type": "bench.task",
        "payload": payload,
        "scheduled_at": scheduled_at.isoformat(),
        "idempotency_key": str(uuid.uuid4()),
    }
    async with semaphore:
        start = time.perf_counter()
        try:
            resp = await client.post("/jobs", json=body)
            latency_ms = (time.perf_counter() - start) * 1000
            job_id = resp.json().get("id") if resp.status_code == 201 else None
            return RequestRecord(
                sent_at=start, latency_ms=latency_ms, status_code=resp.status_code,
                job_id=job_id, scheduled_at=scheduled_at.isoformat(), bucket=bucket,
            )
        except httpx.HTTPError:
            latency_ms = (time.perf_counter() - start) * 1000
            return RequestRecord(
                sent_at=start, latency_ms=latency_ms, status_code=-1,
                job_id=None, scheduled_at=scheduled_at.isoformat(), bucket=bucket,
            )


def _pick_scheduled_at(args: argparse.Namespace) -> tuple[str, datetime]:
    now = datetime.now(timezone.utc)
    if random.random() < args.near_future_fraction:
        offset = random.uniform(args.near_future_min_s, args.near_future_max_s)
        return "near_future", now + timedelta(seconds=offset)
    offset = random.uniform(args.long_future_min_s, args.long_future_max_s)
    return "long_future", now + timedelta(seconds=offset)


async def run(args: argparse.Namespace) -> RunResult:
    semaphore = asyncio.Semaphore(args.concurrency)
    result = RunResult(config=vars(args), started_at=datetime.now(timezone.utc).isoformat(), ended_at="")

    limits = httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=args.concurrency)
    async with httpx.AsyncClient(base_url=args.base_url, timeout=30.0, limits=limits) as client:
        tasks: list[asyncio.Task] = []

        # Burst: fire `burst_size` requests immediately, uncontrolled by --rate.
        if args.burst_size > 0:
            for _ in range(args.burst_size):
                bucket, scheduled_at = _pick_scheduled_at(args)
                duration_ms = _lognormal_duration_ms(args.mean_duration_ms, args.duration_sigma)
                payload = _padded_payload(args.payload_bytes, duration_ms)
                tasks.append(asyncio.create_task(_send_one(client, semaphore, "burst", scheduled_at, payload)))

        # Steady-state: issue requests at approximately `rate` jobs/sec for duration_seconds.
        interval = 1.0 / args.rate if args.rate > 0 else 0
        deadline = time.monotonic() + args.duration_seconds
        next_send = time.monotonic()
        while time.monotonic() < deadline:
            bucket, scheduled_at = _pick_scheduled_at(args)
            duration_ms = _lognormal_duration_ms(args.mean_duration_ms, args.duration_sigma)
            payload = _padded_payload(args.payload_bytes, duration_ms)
            tasks.append(asyncio.create_task(_send_one(client, semaphore, bucket, scheduled_at, payload)))
            next_send += interval
            sleep_for = next_send - time.monotonic()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)

        result.records = await asyncio.gather(*tasks)

    result.ended_at = datetime.now(timezone.utc).isoformat()
    result.attempted = len(result.records)
    for r in result.records:
        if r.status_code == 201:
            result.succeeded += 1
        else:
            result.failed += 1
        result.status_code_counts[str(r.status_code)] = result.status_code_counts.get(str(r.status_code), 0) + 1
    return result


def _percentiles(values: list[float]) -> dict:
    if not values:
        return {"p50": None, "p95": None, "p99": None, "max": None}
    s = sorted(values)
    def pct(p: float) -> float:
        idx = min(len(s) - 1, int(len(s) * p))
        return s[idx]
    return {"p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99), "max": s[-1], "mean": statistics.mean(s)}


def write_results(result: RunResult, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    wall_seconds = (
        datetime.fromisoformat(result.ended_at) - datetime.fromisoformat(result.started_at)
    ).total_seconds()
    latencies = [r.latency_ms for r in result.records]

    summary = {
        "config": result.config,
        "started_at": result.started_at,
        "ended_at": result.ended_at,
        "wall_seconds": wall_seconds,
        "requests_attempted": result.attempted,
        "requests_succeeded": result.succeeded,
        "requests_failed": result.failed,
        "status_code_counts": result.status_code_counts,
        "ingestion_throughput_achieved_per_sec": result.succeeded / wall_seconds if wall_seconds > 0 else 0,
        "api_latency_ms": _percentiles(latencies),
    }
    (output_dir / "ingestion_summary.json").write_text(json.dumps(summary, indent=2, default=str))

    with (output_dir / "job_ids.jsonl").open("w") as f:
        for r in result.records:
            if r.job_id:
                f.write(json.dumps({"job_id": r.job_id, "scheduled_at": r.scheduled_at, "bucket": r.bucket}) + "\n")

    print(json.dumps(summary, indent=2, default=str))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Chronos benchmark load generator")
    p.add_argument("--base-url", default="http://localhost:8000")
    p.add_argument("--duration-seconds", type=float, default=30.0)
    p.add_argument("--rate", type=float, default=100.0, help="target jobs/sec in steady state")
    p.add_argument("--burst-size", type=int, default=0, help="jobs fired immediately at t=0, uncontrolled by --rate")
    p.add_argument("--concurrency", type=int, default=50, help="max in-flight HTTP requests")
    p.add_argument("--near-future-fraction", type=float, default=0.8)
    p.add_argument("--near-future-min-s", type=float, default=2.0)
    p.add_argument("--near-future-max-s", type=float, default=10.0)
    p.add_argument("--long-future-min-s", type=float, default=600.0)
    p.add_argument("--long-future-max-s", type=float, default=3600.0)
    p.add_argument("--mean-duration-ms", type=float, default=50.0, help="median simulated handler execution time")
    p.add_argument("--duration-sigma", type=float, default=0.8, help="lognormal sigma (spread) for execution time")
    p.add_argument("--payload-bytes", type=int, default=512, help="target JSON payload size in bytes")
    p.add_argument("--output-dir", type=Path, default=Path("benchmarks/results/latest"))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    result = asyncio.run(run(args))
    write_results(result, args.output_dir)


if __name__ == "__main__":
    main()
