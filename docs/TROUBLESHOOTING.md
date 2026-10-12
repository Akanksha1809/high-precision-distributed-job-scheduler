# Troubleshooting Guide

## Local development (docker-compose)

**`docker-compose up` hangs or fails pulling an image.** Transient Docker Hub
registry timeouts (`i/o timeout` during `failed to fetch anonymous token`) were
hit repeatedly while building this project — unrelated to Chronos itself. Just
retry: `docker pull <image>` directly, then `docker-compose up -d` again. It
succeeded on retry every time encountered during development (postgres, localstack,
prometheus, grafana images all hit this at least once).

**A job never leaves `SCHEDULED`.** Check the dispatcher container is actually
running (`docker-compose ps`, `docker-compose logs dispatcher`) and that the job's
`scheduled_at` is within `DISPATCH_HORIZON_SECONDS` (default 120s) of now. A job
scheduled further in the future than that simply hasn't entered the dispatch
pipeline yet — that's correct behavior (see `ARCHITECTURE.md`'s horizon-based
dispatch explanation), not a bug. Query `GET /jobs?status=SCHEDULED` to see what's
waiting.

**A job is stuck `DISPATCH_PENDING`.** Check the outbox-publisher container's logs.
If it's been stuck for longer than `RECONCILIATION_STALE_DISPATCH_SECONDS` (default
60s), the reconciliation service should recover it automatically on its next sweep
(default every 30s) — check `docker-compose logs reconciliation` for
`recovered_stale_dispatch_pending`. If it's not recovering, check whether the
outbox row for that job is still legitimately `PENDING` (publisher is just behind,
reconciliation correctly leaves it alone) vs `FAILED` (exhausted its publish
budget, reconciliation should pick it up).

**A job is stuck `QUEUED`.** The queue and the worker are deliberately decoupled —
a job can sit `QUEUED` indefinitely if no worker is consuming. Check
`docker-compose logs worker` and `docker-compose ps worker` (is it running at
all?). If using `QUEUE_BACKEND=sqs` with LocalStack, confirm the `init-queues`
one-shot container completed successfully (`docker-compose logs init-queues`) —
if the SQS queues were never created, publishing silently has nowhere to go.

**Tests fail with `RuntimeError: ... attached to a different loop`.** This is a
known asyncpg/pytest-asyncio pitfall: a session-scoped (or otherwise
longer-than-one-test) async SQLAlchemy engine fixture hands out connections bound
to the event loop it was created on, but pytest-asyncio gives each test function
its own event loop by default. `tests/conftest.py`'s `db_engine` fixture is
function-scoped specifically to avoid this — if you add a new fixture that creates
its own engine, keep it function-scoped too, or set `asyncio_default_fixture_loop_
scope` explicitly (already set to `"function"` in `pyproject.toml`) and make sure
your fixture's scope matches.

**A test asserts stale data after another session wrote to the same row.** SQLAlchemy's
identity map returns the *same Python object* for a given primary key within one
session, even after a *different* session commits a change to that row in the
database. If a test creates a job via `db_session` and then expects to see a
worker/dispatcher's update (made via a different `session_factory()` session)
reflected in that same `job` object, call `await db_session.refresh(job)` first —
a fresh `select()` through the same session will otherwise return the cached,
unrefreshed object. This bit the test suite for real during M4; see `TODO.md`'s M4
notes.

**Prometheus shows a scrape target as `down`.** The background services
(dispatcher/outbox-publisher/worker/reconciliation) take a few seconds after
startup to bind their metrics HTTP server — check
`docker-compose logs <service> | grep metrics_server_started`. If it's not there
at all, check for a Python import error earlier in the log (a broken entrypoint
fails before reaching `start_metrics_server`).

**Grafana dashboard doesn't appear.** Check `docker-compose logs grafana` for
`provisioning.dashboard` errors — a common cause is a volume-mount path mismatch
(see `docker-compose.yml`'s `grafana` service: `./dashboards` must mount to the
exact path `infra/grafana/provisioning/dashboards/dashboards.yml` configures as
its `path:`). Mounting a second bind volume *inside* a path that's itself the
target of another read-only bind mount fails at container start with a
"read-only file system" error when creating the mountpoint — keep dashboard and
provisioning-config mounts at non-overlapping paths (this happened during M6; see
`docker-compose.yml`'s `grafana` volumes for the fix).

**LocalStack's returned queue URL doesn't match what's in `docker-compose.yml`.**
LocalStack 3.x's `create_queue` response uses a virtual-hosted-style URL
(`http://sqs.us-east-1.localhost.localstack.cloud:4566/<account>/<name>`), but the
classic path-style URL configured in `docker-compose.yml`
(`http://localstack:4566/<account>/<name>`) still resolves to the same underlying
queue when used as `QueueUrl` in subsequent API calls — both work from inside the
Docker network. This looked like a bug the first time it was observed; it isn't.

## Benchmarks

**`run_benchmark.sh` seems to hang.** A high `--burst-size` combined with the
default `--concurrency` can make the load generator itself take much longer than
the requested `--duration-seconds` to finish sending all requests (observed
directly: a 20s-nominal run with a 2000-job burst took 152s wall-clock — see
`BENCHMARKS.md` §4.3). This is expected under overload, not a hang — check whether
requests are still completing by watching `docker stats` or the API's own logs.

**`measure_pipeline.py` reports way more `still_pending_count` than expected.**
Increase `DRAIN_WAIT_SECONDS` (env var passed to `run_benchmark.sh`, default 20) —
a high-burst run needs more time after load generation stops for the backlog to
actually drain through dispatch → queue → execution before analysis runs.

## AWS deployment (documentation only — nothing has been deployed)

See `infra/aws/DEPLOYMENT.md` for setup steps. Common `terraform plan`/`apply`
issues for this kind of stack generally come down to: AWS credentials not
configured via the standard chain, the default VPC not existing in the target
region/account (this config assumes one exists — see `network.tf`), or the ECR
image not yet pushed when `apply` tries to create the ECS services (push the image
after `terraform apply` creates the repository, or target the repository resource
first — both documented in `DEPLOYMENT.md` §3).
