"""Post-run pipeline analysis: reads the job_ids produced by generate_load.py and
queries Postgres directly for dispatch/queue/completion throughput and
schedule-to-start lateness. This is deliberately separate from the load generator —
ingestion (API accepted the request) and completion (a worker actually ran it) are
different things, and conflating them is exactly how a benchmark ends up lying.

Lateness is defined as started_at - scheduled_at. Negative values (the job started
before its scheduled time — should not happen given the worker's early-arrival
deferral, but is reported explicitly rather than assumed away) are reported
separately from positive lateness.

Usage:
    python -m benchmarks.measure_pipeline --run-dir benchmarks/results/latest
"""
import argparse
import asyncio
import json
import statistics
from pathlib import Path

from sqlalchemy import select

from app.db.session import get_sessionmaker
from app.models.job import Job
from app.models.states import JobStatus


def _percentiles(values: list[float]) -> dict:
    if not values:
        return {"p50": None, "p95": None, "p99": None, "max": None, "mean": None, "count": 0}
    s = sorted(values)
    def pct(p: float) -> float:
        idx = min(len(s) - 1, int(len(s) * p))
        return s[idx]
    return {
        "p50": pct(0.50), "p95": pct(0.95), "p99": pct(0.99),
        "max": s[-1], "mean": statistics.mean(s), "count": len(s),
    }


def _rate_over_window(timestamps: list) -> float:
    ts = [t for t in timestamps if t is not None]
    if len(ts) < 2:
        return 0.0
    span = (max(ts) - min(ts)).total_seconds()
    return len(ts) / span if span > 0 else float(len(ts))


async def analyze(run_dir: Path) -> dict:
    job_ids_path = run_dir / "job_ids.jsonl"
    entries = [json.loads(line) for line in job_ids_path.read_text().splitlines() if line.strip()]
    id_to_bucket = {e["job_id"]: e["bucket"] for e in entries}
    job_ids = list(id_to_bucket.keys())

    session_factory = get_sessionmaker()
    async with session_factory() as session:
        rows = (
            await session.execute(select(Job).where(Job.id.in_(job_ids)))
        ).scalars().all()

    by_status: dict[str, int] = {}
    for j in rows:
        by_status[j.status] = by_status.get(j.status, 0) + 1

    dispatched_ts = [j.dispatched_at for j in rows if j.dispatched_at]
    queued_ts = [j.queued_at for j in rows if j.queued_at]
    completed_ts = [j.completed_at for j in rows if j.completed_at]

    lateness_positive = []
    lateness_negative = []
    for j in rows:
        if j.started_at is None:
            continue
        delta = (j.started_at - j.scheduled_at).total_seconds()
        if delta >= 0:
            lateness_positive.append(delta)
        else:
            lateness_negative.append(delta)

    terminal_count = sum(
        by_status.get(s.value, 0)
        for s in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.DEAD_LETTER, JobStatus.CANCELLED)
    )
    failed_count = by_status.get(JobStatus.FAILED.value, 0) + by_status.get(JobStatus.DEAD_LETTER.value, 0)
    retried_count = sum(1 for j in rows if j.attempt_count > 1)

    lateness_sla_total = len(lateness_positive) + len(lateness_negative)
    within_2s = sum(1 for d in lateness_positive if d <= 2.0) + len(lateness_negative)

    return {
        "jobs_analyzed": len(rows),
        "by_status": by_status,
        "by_bucket": {
            bucket: sum(1 for jid in id_to_bucket if id_to_bucket[jid] == bucket)
            for bucket in set(id_to_bucket.values())
        },
        "dispatch_throughput_per_sec": _rate_over_window(dispatched_ts),
        "queue_publish_throughput_per_sec": _rate_over_window(queued_ts),
        "completion_throughput_per_sec": _rate_over_window(completed_ts),
        "lateness_seconds": {
            "late_or_on_time": _percentiles(lateness_positive),
            "early_starts": _percentiles([abs(d) for d in lateness_negative]),
            "early_start_count": len(lateness_negative),
        },
        "pct_within_2s_of_scheduled_time": (
            100.0 * within_2s / lateness_sla_total if lateness_sla_total else None
        ),
        "retry_rate": retried_count / terminal_count if terminal_count else None,
        "failure_rate": failed_count / terminal_count if terminal_count else None,
        "terminal_count": terminal_count,
        "still_pending_count": len(rows) - terminal_count - sum(
            by_status.get(s.value, 0) for s in (JobStatus.RUNNING,)
        ),
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze a Chronos benchmark run from the database")
    p.add_argument("--run-dir", type=Path, default=Path("benchmarks/results/latest"))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    report = asyncio.run(analyze(args.run_dir))
    (args.run_dir / "pipeline_report.json").write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
