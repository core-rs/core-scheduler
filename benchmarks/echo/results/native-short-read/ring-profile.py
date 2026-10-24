#!/usr/bin/env python3
"""Measure ring controls separately with CPU counters, work tracing, and sampled stacks."""
import argparse
import csv
import hashlib
import importlib.util
import itertools
import json
from pathlib import Path
import random
import shlex
import subprocess
import tempfile
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[4]
spec = importlib.util.spec_from_file_location("echo_run", ROOT / "benchmarks/echo/run.py")
run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run)
parser = argparse.ArgumentParser(description=__doc__)
for name in ("before", "after", "client", "output"):
    parser.add_argument("--" + name, type=Path, required=True)
parser.add_argument("--workers", type=int, default=1)
parser.add_argument("--connections", type=int, default=32)
parser.add_argument("--stats-only", action="store_true")
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
settings = SimpleNamespace(server_cpus=list(range(args.workers)), client_cpus=[4, 5, 6, 7],
    client_binary=args.client.resolve(), client_workers=4, warmup=0, seconds=3)
variants = {"baseline": (args.before.resolve(), "iouring-monitor"),
    "deferred": (args.after.resolve(), "iouring-monitor"), "tokio": (args.before.resolve(), "tokio")}
events = {
    "cpu": ["cycles:u", "instructions:u", "cycles:k", "instructions:k", "context-switches", "cpu-migrations"],
    "work": ["io_uring:io_uring_task_work_run", "io_uring:io_uring_local_work_run",
        "io_uring:io_uring_submit_req", "io_uring:io_uring_poll_arm", "io_uring:io_uring_complete",
        "syscalls:sys_enter_recvfrom", "syscalls:sys_enter_sendto", "syscalls:sys_enter_io_uring_enter",
        "syscalls:sys_enter_epoll_wait", "syscalls:sys_enter_epoll_pwait", "syscalls:sys_enter_epoll_pwait2"],
}
report = {"variants": {name: {"binary": str(binary), "backend": backend,
    "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()} for name, (binary, backend) in variants.items()},
    "settings": vars(settings) | {"client_binary": str(settings.client_binary)}, "events": events,
    "note": "CPU counters run without tracepoints. Traced work and sampled stacks are separate mechanism checks, not capacity results.",
    "samples": [], "profiles": []}

def save():
    (args.output / "stats.json").write_text(json.dumps(report, indent=2) + "\n")

with tempfile.TemporaryDirectory(prefix="core-scheduler-ring-profile-") as directory:
    wrapper = Path(directory) / "server"

    def server(command):
        wrapper.write_text("#!/bin/sh\nexec " + shlex.join(["sudo", "-n", "perf", *command]) + ' "$@"\n')
        wrapper.chmod(0o700)
        return wrapper

    jobs = list(itertools.product(variants, events, range(3)))
    random.Random(20261024).shuffle(jobs)
    for name, mode, repeat in jobs:
        binary, backend = variants[name]
        raw = args.output / f"{name}-{mode}-{repeat}.stat.csv"
        sample = run.sample(server(["stat", "-x,", "-e", ",".join(events[mode]), "-o", str(raw), "--", str(binary)]),
            backend, args.workers, args.connections, 64, settings)
        values = {row[2]: float(row[0]) for row in csv.reader(raw.read_text().splitlines())
            if len(row) >= 3 and row[2] in events[mode]}
        report["samples"].append(dict(sample, variant=name, mode=mode, repeat=repeat, counters=values,
            per_request={key: value / sample["requests"] for key, value in values.items()}))
        save()
        print(name, mode, repeat, report["samples"][-1]["per_request"], flush=True)
    if args.stats_only:
        raise SystemExit(0)
    settings.seconds = 0.1
    for name in ("baseline", "deferred"):
        binary, backend = variants[name]
        data = args.output / f"{name}.setup.data"
        run.sample(server(["record", "-e", "io_uring:io_uring_create", "-o", str(data), "--", str(binary)]),
            backend, args.workers, args.connections, 64, settings)
        with (args.output / f"{name}.setup.txt").open("w") as output:
            subprocess.run(["sudo", "-n", "perf", "script", "-i", str(data)], stdout=output, check=True)
    settings.seconds = 6
    for name in ("deferred", "tokio", "baseline"):
        binary, backend = variants[name]
        data = args.output / f"{name}.data"
        sample = run.sample(server(["record", "-F", "499", "-e", "cycles", "-g", "--call-graph", "dwarf,16384",
            "-o", str(data), "--", str(binary)]), backend, args.workers, args.connections, 64, settings)
        report["profiles"].append(dict(sample, variant=name, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest()))
        for suffix, flag in (("self", "--no-children"), ("children", "--children")):
            with (args.output / f"{name}.{suffix}.txt").open("w") as output:
                subprocess.run(["sudo", "-n", "perf", "report", "--stdio", "--header", "--percent-limit", "0.2",
                    "-i", str(data), flag], stdout=output, check=True)
        save()
        print("profiled", name, flush=True)
