#!/usr/bin/env python3
"""Current echo gap: separate throughput controls, syscall counters and profiles."""

import argparse
import datetime
import fcntl
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import random
import subprocess
import types

from sample import sample


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    cli = parser.parse_args()
    args = types.SimpleNamespace(server=cli.server.resolve(), client=cli.client.resolve(), output=cli.output.resolve(), privileged_perf=True, perf_event="cycles")
    args.output.mkdir(parents=True, exist_ok=False)
    source = cli.source.resolve()
    patch = subprocess.check_output(["git", "diff", "--", "benchmarks/echo/src/server.rs"], cwd=source)
    (args.output / "control.patch").write_bytes(patch)
    report = dict(
        source=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip(),
        patch_sha256=hashlib.sha256(patch).hexdigest(),
        build="CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --release --locked -p core-scheduler-echo-benchmark",
        binaries={key: dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest()) for key, path in [("server", args.server), ("client", args.client)]},
        platform=platform.platform(), hostname=platform.node(),
        rustc=subprocess.check_output(["rustc", "--version"], text=True).strip(),
        cpu_topology=subprocess.check_output(["lscpu", "-e=CPU,CORE,SOCKET,NODE,CACHE"], text=True),
        server_cpus=[0], client_cpus=[4, 5, 6, 7], client_workers=4,
        method="Controls: 1s warmup, 3s measurement. Stats: zero warmup, 3s measurement; counters cover process lifetime. Profiles: 1s warmup, 5s measurement. Instrumented throughput is diagnostic only.",
        samples=[],
    )
    backends = ["tokio", "tokio-conservative", "iouring-monitor", "iouring"]
    with open("/tmp/core-scheduler-perf.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        report["started_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        report["load_average_before"] = os.getloadavg()
        for mode, repeats, sizes in [("control", 3, [64, 4096]), ("stat", 2, [64]), ("perf", 1, [64])]:
            cases = list(itertools.product(range(1, repeats + 1), backends, sizes))
            random.Random(20261024).shuffle(cases)
            for repeat, backend, size in cases:
                result = sample(args, mode, backend, 1, 32, size, repeat)
                report["samples"].append(result)
                (args.output / "aiko.json").write_text(json.dumps(report, indent=2) + "\n")
                print(f"{len(report['samples'])}/36 {result['identifier']}: {result['requests_per_second']:,.0f} req/s", flush=True)
        report["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        report["load_average_after"] = os.getloadavg()
        (args.output / "aiko.json").write_text(json.dumps(report, indent=2) + "\n")
        for result in report["samples"]:
            if result["mode"] != "perf":
                continue
            name = result["identifier"]
            for suffix, options in [("self", ["--no-children", "-g", "none", "--percent-limit", "0.1"]), ("callgraph", ["--children", "-g", "graph,1,caller", "--percent-limit", "1"])]:
                with (args.output / f"{name}.{suffix}.txt").open("w") as output:
                    subprocess.run(["sudo", "-n", "perf", "report", "--stdio", "--header", "--show-nr-samples", "--sort", "symbol", *options, "-i", str(args.output / f"{name}.data")], stdout=output, check=True)


if __name__ == "__main__":
    main()
