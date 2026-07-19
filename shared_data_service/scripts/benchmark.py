"""Shared Data Service benchmark.

Measures, on the real stack (PostgreSQL + RabbitMQ + uvicorn):

1. postgres  — DAL throughput through the real UnitOfWork/repository
               (create service-path, get, list), sequential and concurrent.
2. api       — HTTP endpoint throughput + latency percentiles (p50/p95/p99)
               for POST/GET/PATCH/LIST under concurrent load.
3. rabbit    — end-to-end event path: publish CloudEvents → consumer →
               business → committed row. Throughput and publish→commit
               latency percentiles.
4. scaling   — the same API and consumer loads against 1 vs K processes
               (client-side round-robin = horizontal scaling).

Usage: .venv/bin/python scripts/benchmark.py [--quick]
Writes benchmark_results.json next to this script and prints a report.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx
from sqlalchemy import text

HERE = Path(__file__).resolve().parent
SERVICE_DIR = HERE.parent
sys.path.insert(0, str(SERVICE_DIR))

from app.bootstrap.container import Container  # noqa: E402
from app.config.settings import Settings  # noqa: E402
from app.messaging.cloudevents import CloudEvent, now_utc  # noqa: E402
from app.modules.user import UserData  # noqa: E402
from hs_rabbit_client import RabbitClient  # noqa: E402

PYTHON = str(SERVICE_DIR / ".venv" / "bin" / "python")
BENCH_IN_QUEUE = "sds-bench.events.in"
BENCH_OUT_QUEUE = "sds-bench.events.out"
BASE_PORT = 8091

RESULTS: dict[str, object] = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "sections": {},
}


def record(section: str, name: str, **metrics) -> None:
    RESULTS["sections"].setdefault(section, {})[name] = metrics  # type: ignore[union-attr]
    pretty = ", ".join(
        f"{k}={v:,.1f}" if isinstance(v, float) else f"{k}={v}"
        for k, v in metrics.items()
    )
    print(f"  [{section}] {name}: {pretty}")


def percentiles(samples_ms: list[float]) -> dict[str, float]:
    if len(samples_ms) < 2:
        return {"p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0}
    qs = statistics.quantiles(samples_ms, n=100, method="inclusive")
    return {
        "p50_ms": round(statistics.median(samples_ms), 2),
        "p95_ms": round(qs[94], 2),
        "p99_ms": round(qs[98], 2),
        "mean_ms": round(statistics.fmean(samples_ms), 2),
    }


def bench_settings(**overrides) -> Settings:
    defaults = dict(
        consume_queues=[BENCH_IN_QUEUE],
        publish_queue=BENCH_OUT_QUEUE,
        service_mode="api",
        log_level="WARNING",
    )
    defaults.update(overrides)
    return Settings(**defaults)


async def truncate(container: Container) -> None:
    async with container.engine.begin() as conn:
        await conn.execute(text("TRUNCATE users, processed_events"))


def make_user_data(n: int) -> UserData:
    return UserData(
        id=uuid.uuid4(), name=f"bench-{n}", email=f"b{n}@example.com",
        attributes={"n": n},
    )


# ------------------------------------------------------------------ postgres

async def bench_postgres(container: Container, n: int, concurrency: int) -> None:
    service = container.services["user"]
    await truncate(container)

    # sequential create (full business path: validate, insert, event, commit)
    start = time.perf_counter()
    ids = []
    for i in range(n):
        user, _ = await service.create(make_user_data(i))
        ids.append(user.id)
    dur = time.perf_counter() - start
    record("postgres", "create_sequential", ops_per_sec=n / dur, n=n)

    # concurrent create
    await truncate(container)
    sem = asyncio.Semaphore(concurrency)

    async def one_create(i: int) -> uuid.UUID:
        async with sem:
            user, _ = await service.create(make_user_data(10_000 + i))
            return user.id

    start = time.perf_counter()
    ids = list(await asyncio.gather(*(one_create(i) for i in range(n))))
    dur = time.perf_counter() - start
    record("postgres", f"create_concurrent_c{concurrency}", ops_per_sec=n / dur, n=n)

    # concurrent get
    async def one_get(uid: uuid.UUID) -> None:
        async with sem:
            await service.get(uid)

    start = time.perf_counter()
    await asyncio.gather(*(one_get(ids[i % len(ids)]) for i in range(n)))
    dur = time.perf_counter() - start
    record("postgres", f"get_concurrent_c{concurrency}", ops_per_sec=n / dur, n=n)

    # list page of 50
    m = max(n // 10, 50)
    start = time.perf_counter()
    for _ in range(m):
        await service.list_page(limit=50, offset=0, sort="-created_at")
    dur = time.perf_counter() - start
    record("postgres", "list_page50_sequential", ops_per_sec=m / dur, n=m)


# ----------------------------------------------------------------------- api

def start_api_procs(k: int) -> list[tuple[subprocess.Popen, int]]:
    procs = []
    for i in range(k):
        port = BASE_PORT + i
        env = os.environ | {
            "SDS_SERVICE_MODE": "api",
            "SDS_LOG_LEVEL": "WARNING",
            "SDS_PUBLISH_QUEUE": BENCH_OUT_QUEUE,
            "SDS_CONSUME_QUEUES": json.dumps([BENCH_IN_QUEUE]),
        }
        proc = subprocess.Popen(
            [PYTHON, "-m", "uvicorn", "app.bootstrap.api_app:create_app_from_env",
             "--factory", "--host", "127.0.0.1", "--port", str(port),
             "--log-level", "warning", "--no-access-log"],
            cwd=SERVICE_DIR, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        procs.append((proc, port))
    return procs


async def wait_ready(ports: list[int]) -> None:
    async with httpx.AsyncClient() as client:
        deadline = time.monotonic() + 30
        for port in ports:
            while True:
                try:
                    r = await client.get(f"http://127.0.0.1:{port}/ready", timeout=1.0)
                    if r.status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                if time.monotonic() > deadline:
                    raise RuntimeError(f"API on :{port} never became ready")
                await asyncio.sleep(0.2)


async def hammer(
    client: httpx.AsyncClient,
    n: int,
    concurrency: int,
    request_factory,
) -> tuple[float, list[float]]:
    """Run n requests at the given concurrency; return (duration_s, latencies_ms)."""
    sem = asyncio.Semaphore(concurrency)
    latencies: list[float] = []
    fail_statuses: dict[int, int] = {}
    fail_sample: dict[int, str] = {}

    async def one(i: int) -> None:
        async with sem:
            t0 = time.perf_counter()
            r = await request_factory(client, i)
            latencies.append((time.perf_counter() - t0) * 1000)
            if r.status_code >= 400:
                fail_statuses[r.status_code] = fail_statuses.get(r.status_code, 0) + 1
                fail_sample.setdefault(r.status_code, r.text[:300])

    start = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(n)))
    duration = time.perf_counter() - start
    if fail_statuses:
        raise RuntimeError(
            f"{sum(fail_statuses.values())}/{n} requests failed; "
            f"statuses={fail_statuses}; samples={fail_sample}"
        )
    return duration, latencies


PHASES = ("create", "get", "patch", "list")
PHASE_LABEL = {
    "create": "POST /users",
    "get": "GET /users/{id}",
    "patch": "PATCH /users/{id}",
    "list": "GET /users?limit=50",
}


def request_factory(phase: str, base_url: str, ids: list[str]):
    async def do(c: httpx.AsyncClient, i: int):
        if phase == "create":
            return await c.post(f"{base_url}/users", json={
                "id": ids[i], "name": f"api-{i}", "email": f"a{i}@ex.com",
                "attributes": {"n": i}})
        if phase == "get":
            return await c.get(f"{base_url}/users/{ids[i]}")
        if phase == "patch":
            return await c.patch(f"{base_url}/users/{ids[i]}",
                                 json={"name": f"api-{i}-v2"})
        return await c.get(f"{base_url}/users",
                           params={"limit": 50, "sort": "-created_at"})

    return do


async def run_loadgen(spec: dict) -> None:
    """Child-process mode: hammer one API instance, print JSON to stdout."""
    limits = httpx.Limits(max_connections=spec["concurrency"] + 10,
                          max_keepalive_connections=spec["concurrency"] + 10)
    async with httpx.AsyncClient(limits=limits, timeout=60.0) as client:
        factory = request_factory(spec["phase"], spec["base_url"], spec["ids"])
        duration, latencies = await hammer(
            client, spec["n"], spec["concurrency"], factory)
    print(json.dumps({"duration": duration, "latencies": latencies}))


async def bench_api(container: Container, k: int, n: int, concurrency: int,
                    section: str) -> None:
    await truncate(container)
    procs = start_api_procs(k)
    ports = [p for _, p in procs]
    try:
        await wait_ready(ports)
        ids = [str(uuid.uuid4()) for _ in range(n)]
        chunk = n // k

        for phase in PHASES:
            count = max(n // 4, 100) if phase == "list" else n
            per_gen = count // k
            # One load-generator process per API instance: the client side
            # scales with the server side, so it never caps the measurement.
            gens = []
            for gi, port in enumerate(ports):
                id_slice = ids[gi * chunk:(gi + 1) * chunk] if phase != "list" \
                    else ids[:per_gen]
                spec = {
                    "phase": phase,
                    "base_url": f"http://127.0.0.1:{port}",
                    "n": per_gen,
                    "concurrency": max(concurrency // k, 1),
                    "ids": id_slice[:per_gen] if phase != "list" else ids[:1],
                }
                gens.append(subprocess.Popen(
                    [PYTHON, str(HERE / "benchmark.py"), "--loadgen", json.dumps(spec)],
                    cwd=SERVICE_DIR, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE, text=True,
                ))
            outputs = []
            for gen in gens:
                out, err = gen.communicate(timeout=600)
                if gen.returncode != 0:
                    raise RuntimeError(
                        f"loadgen failed for phase {phase}: {(err or out)[-500:]}"
                    )
                outputs.append(json.loads(out))
            total = per_gen * k
            wall = max(o["duration"] for o in outputs)
            lats = [ms for o in outputs for ms in o["latencies"]]
            record(section, f"{PHASE_LABEL[phase]} (procs={k}, c={concurrency})",
                   req_per_sec=total / wall, **percentiles(lats))
    finally:
        for proc, _ in procs:
            proc.terminate()
        for proc, _ in procs:
            proc.wait(timeout=10)


# -------------------------------------------------------------------- rabbit

def start_consumer_procs(k: int) -> list[subprocess.Popen]:
    env = os.environ | {
        "SDS_SERVICE_MODE": "consumer",
        "SDS_LOG_LEVEL": "WARNING",
        "SDS_PUBLISH_QUEUE": BENCH_OUT_QUEUE,
        "SDS_CONSUME_QUEUES": json.dumps([BENCH_IN_QUEUE]),
    }
    return [
        subprocess.Popen(
            [PYTHON, "-c",
             "from app.bootstrap.consumer_runner import main; main()"],
            cwd=SERVICE_DIR, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(k)
    ]


def make_event_body(uid: str, i: int) -> bytes:
    return CloudEvent(
        id=f"bench-{uuid.uuid4()}", source="urn:bench", type="user.created",
        time=now_utc(),
        data={"id": uid, "name": f"ev-{i}", "email": f"e{i}@ex.com", "version": 1},
    ).to_bytes()


async def pg_clock_skew_s(container: Container) -> float:
    """PostgreSQL clock minus host clock, RTT-corrected (for latency math)."""
    async with container.engine.connect() as conn:
        t0 = time.time()
        pg_now = (await conn.execute(text("SELECT extract(epoch FROM now())"))).scalar_one()
        rtt = time.time() - t0
    return float(pg_now) - (t0 + rtt / 2)


async def bench_rabbit(container: Container, k: int, n: int, section: str) -> None:
    await truncate(container)
    aux = RabbitClient(bench_settings().amqp_url)
    await aux.connect()
    await aux.delete_queue(BENCH_IN_QUEUE)
    consumers = start_consumer_procs(k)
    try:
        await asyncio.sleep(2.0)  # consumers connect + declare

        async def committed_count() -> int:
            async with container.engine.connect() as conn:
                return (await conn.execute(
                    text("SELECT count(*) FROM users"))).scalar_one()

        async def wait_committed(target: int, timeout: float = 180.0) -> None:
            deadline = time.monotonic() + timeout
            while await committed_count() < target:
                if time.monotonic() > deadline:
                    raise RuntimeError(
                        f"only {await committed_count()}/{target} committed")
                await asyncio.sleep(0.05)

        # (a) true per-event latency: one event in flight at a time, measured
        # on the host clock (publish → row visible), no queue wait involved.
        lat_ms: list[float] = []
        for i in range(200):
            t0 = time.perf_counter()
            await aux.publish(BENCH_IN_QUEUE, make_event_body(str(uuid.uuid4()), i))
            while await committed_count() < i + 1:
                await asyncio.sleep(0.001)
            lat_ms.append((time.perf_counter() - t0) * 1000)
        record(section, f"latency single-inflight (consumers={k})",
               n=200, **percentiles(lat_ms))
        await truncate(container)

        # (b) latency under sustained load: paced publishing at 1,000 ev/s,
        # per-event publish→commit from row created_at (clock-skew corrected).
        rate, n_paced = 1_000, 3_000
        skew = await pg_clock_skew_s(container)
        publish_at: dict[str, float] = {}
        interval = 1.0 / rate
        next_slot = time.perf_counter()
        perf_start = time.perf_counter()
        for i in range(n_paced):
            uid = str(uuid.uuid4())
            publish_at[uid] = time.time()
            await aux.publish(BENCH_IN_QUEUE, make_event_body(uid, i))
            next_slot += interval
            delay = next_slot - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
        achieved = n_paced / (time.perf_counter() - perf_start)
        await wait_committed(n_paced)
        async with container.engine.connect() as conn:
            rows = (await conn.execute(text(
                "SELECT id, extract(epoch FROM created_at) FROM users"))).all()
        paced_lat = [
            max((float(epoch) - skew) - publish_at[str(row_id)], 0.0) * 1000
            for row_id, epoch in rows
        ]
        record(section, f"latency @{rate}ev/s sustained (consumers={k})",
               publish_rate=achieved, n=n_paced, **percentiles(paced_lat))
        await truncate(container)

        # (c) burst throughput: n events published at once. Sample the
        # committed count over time and take the 10%→90% slope, so neither
        # publish overlap nor the tail skews the consume rate.
        bodies = [make_event_body(str(uuid.uuid4()), i) for i in range(n)]
        start = time.perf_counter()
        samples: list[tuple[float, int]] = []

        async def sampler() -> None:
            while True:
                samples.append((time.perf_counter() - start, await committed_count()))
                if samples[-1][1] >= n:
                    return
                await asyncio.sleep(0.02)

        sample_task = asyncio.create_task(sampler())
        await aux.publish_many(BENCH_IN_QUEUE, bodies)
        await asyncio.wait_for(sample_task, timeout=180)
        total_duration = samples[-1][0]

        def time_at(target: int) -> float:
            return next(t for t, c in samples if c >= target)

        t10, t90 = time_at(int(n * 0.1)), time_at(int(n * 0.9))
        record(section, f"burst drain (consumers={k})",
               events_per_sec=(n * 0.8) / (t90 - t10),
               end_to_end_per_sec=n / total_duration, n=n)
    finally:
        for proc in consumers:
            proc.terminate()
        for proc in consumers:
            proc.wait(timeout=10)
        await aux.delete_queue(BENCH_IN_QUEUE)
        await aux.close()


# ---------------------------------------------------------------------- main

async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true", help="smaller counts")
    parser.add_argument("--loadgen", help="internal: child load-generator spec")
    args = parser.parse_args()

    if args.loadgen:
        await run_loadgen(json.loads(args.loadgen))
        return

    n_pg = 1_000 if args.quick else 3_000
    n_api = 1_000 if args.quick else 3_000
    n_rabbit = 2_000 if args.quick else 10_000
    concurrency = 50

    container = Container(bench_settings())
    await container.start()

    async def reset_queues() -> None:
        # The out queue receives a persistent event per create/patch and is
        # never consumed; without cleanup it grows without bound across runs.
        aux = RabbitClient(bench_settings().amqp_url)
        await aux.connect()
        for q in (BENCH_IN_QUEUE, BENCH_OUT_QUEUE):
            await aux.delete_queue(q)
        await aux.close()

    await reset_queues()
    try:
        print("== 1/4 PostgreSQL layer (business path, real UoW) ==")
        await bench_postgres(container, n_pg, concurrency)

        print("== 2/4 HTTP API (1 process) ==")
        await bench_api(container, 1, n_api, concurrency, "api")

        print("== 3/4 RabbitMQ event path (1 consumer) ==")
        await bench_rabbit(container, 1, n_rabbit, "rabbit")

        print("== 4/4 Scalability (4 API processes / 4 consumers) ==")
        await bench_api(container, 4, n_api, concurrency, "scaling")
        await bench_rabbit(container, 4, n_rabbit, "scaling")

        await truncate(container)
    finally:
        await reset_queues()
        await container.stop()

    out = HERE / "benchmark_results.json"
    out.write_text(json.dumps(RESULTS, indent=2))
    print(f"\nresults written to {out}")


if __name__ == "__main__":
    asyncio.run(main())
