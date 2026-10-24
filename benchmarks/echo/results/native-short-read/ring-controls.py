#!/usr/bin/env python3
"""Compare safe Native ring controls with one fixed validating client."""

import argparse
import hashlib
import importlib.util
import itertools
import json
from pathlib import Path
import platform
import random
import statistics
import subprocess
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[4]
spec = importlib.util.spec_from_file_location("echo_run", ROOT / "benchmarks/echo/run.py")
run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--deferred", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--backends", nargs="+", default=["iouring-monitor", "iouring", "tokio-adapter-monitor"])
    parser.add_argument("--cases", nargs="+", default=["1:1:64", "1:32:64", "4:32:64"])
    parser.add_argument("--repeats", type=int, default=4)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists")
    cases = [tuple(map(int, case.split(":"))) for case in args.cases]
    variants = {f"{version}-{backend}": (binary.resolve(), backend)
                for version, binary in [("before", args.before), ("after", args.after)]
                for backend in args.backends}
    variants["tokio"] = (args.before.resolve(), "tokio")
    variants["deferred-iouring-monitor"] = (args.deferred.resolve(), "iouring-monitor")
    settings = SimpleNamespace(server_cpus=[0, 1, 2, 3], client_cpus=[4, 5, 6, 7],
                               client_binary=args.client.resolve(), client_workers=4, warmup=1, seconds=3)
    report = {
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "source_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT)).hexdigest(),
        "platform": platform.platform(), "hostname": platform.node(),
        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True),
        "settings": vars(settings) | {"client_binary": str(settings.client_binary)},
        "variants": {name: {"binary": str(binary), "backend": backend,
                            "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
                     for name, (binary, backend) in variants.items()},
        "client_sha256": hashlib.sha256(settings.client_binary.read_bytes()).hexdigest(),
        "cases": cases, "repeats": args.repeats, "seed": 20261024, "samples": [],
    }
    jobs = list(itertools.product(range(args.repeats), cases, variants))
    random.Random(report["seed"]).shuffle(jobs)
    for repeat, (workers, connections, size), variant in jobs:
        binary, backend = variants[variant]
        sample = run.sample(binary, backend, workers, connections, size, settings)
        report["samples"].append(dict(sample, repeat=repeat, variant=variant))
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(variant, workers, connections, repeat, round(sample["requests_per_second"]), flush=True)
    for case in cases:
        medians = {name: statistics.median(s["requests_per_second"] for s in report["samples"]
                   if s["variant"] == name and (s["workers"], s["connections"], s["bytes"]) == case)
                   for name in variants}
        print(case, json.dumps(medians), flush=True)


if __name__ == "__main__":
    main()
