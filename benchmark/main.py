"""CLI entry point: run the suite, persist results, generate the report."""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone

from benchmark.config import BenchmarkConfig
from benchmark.reporting.report_generator import generate_report
from benchmark.results import save_csv, save_json
from benchmark.runner import run_suite


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark pika vs aio-pika against RabbitMQ.")
    p.add_argument("--config")
    p.add_argument("--amqp-url")
    p.add_argument("--message-count", type=int)
    p.add_argument("--iterations", type=int)
    p.add_argument("--clients", help="comma-separated client names")
    p.add_argument("--output-dir")
    p.add_argument("--confirms", action=argparse.BooleanOptionalAction, default=None,
                   help="publisher confirms on/off (default: config)")
    p.add_argument("--durable", action=argparse.BooleanOptionalAction, default=None,
                   help="durable queue + persistent messages (default: config)")
    p.add_argument("--no-report", action="store_true")
    return p.parse_args(argv)


async def async_main(argv: list[str]) -> str:
    ns = parse_args(argv)
    overrides = {
        "amqp_url": ns.amqp_url,
        "message_count": ns.message_count,
        "iterations": ns.iterations,
        "clients": ns.clients.split(",") if ns.clients else None,
        "output_dir": ns.output_dir,
        "publisher_confirms": ns.confirms,
        "durable": ns.durable,
    }
    config = BenchmarkConfig.load(ns.config, overrides={k: v for k, v in overrides.items() if v is not None})

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_dir = os.path.join(config.output_dir, stamp)
    os.makedirs(run_dir, exist_ok=True)

    print(f"[1/3] Running benchmarks for {config.clients} ...", flush=True)
    suite = await run_suite(config, show_progress=True)

    print("[2/3] Writing raw results (JSON + CSV) ...", flush=True)
    save_json(suite, os.path.join(run_dir, "results.json"))
    save_csv(suite, os.path.join(run_dir, "results.csv"))
    print(f"      {run_dir}", flush=True)

    if not ns.no_report:
        print("[3/3] Generating report ...", flush=True)
        paths = generate_report(suite, run_dir)
        print(f"      Report: {paths['html']}"
              + (f" / {paths['pdf']}" if paths["pdf"] else " (HTML only)"), flush=True)
    else:
        print("[3/3] Report skipped (--no-report).", flush=True)

    return run_dir


def main() -> None:
    asyncio.run(async_main(sys.argv[1:]))


if __name__ == "__main__":
    main()
