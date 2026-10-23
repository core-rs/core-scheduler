#!/usr/bin/env python3
"""Run independent echo server/client processes and preserve every sample."""

import argparse
import datetime
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import random
import queue
import threading
import statistics
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]


def cpu_list(value):
    cpus = []
    for item in value.split(","):
        bounds = [int(part) for part in item.split("-")]
        cpus.extend(range(bounds[0], bounds[-1] + 1))
    return cpus


def affinity(cpus):
    return ["taskset", "--cpu-list", ",".join(map(str, cpus))] if cpus else []


def command_output(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def sample(binary, backend, workers, connections, size, args):
    server_cpus = args.server_cpus[:workers] if args.server_cpus else []
    server_command = affinity(server_cpus) + [str(binary), "server", backend, str(workers), str(connections)]
    client_prefix = affinity(args.client_cpus)
    with tempfile.TemporaryFile(mode="w+") as errors:
        server = subprocess.Popen(server_command, cwd=ROOT, stdout=subprocess.PIPE, stderr=errors, text=True)
        try:
            ready = queue.Queue()
            threading.Thread(target=lambda: ready.put(server.stdout.readline()), daemon=True).start()
            try:
                address = ready.get(timeout=30).strip()
            except queue.Empty:
                raise RuntimeError("server did not become ready within 30 seconds") from None
            if not address:
                raise RuntimeError("server exited before printing its address")
            client_command = client_prefix + [str(args.client_binary or binary), "client", address, str(args.client_workers), str(connections), str(size), str(args.warmup), str(args.seconds)]
            result = subprocess.run(client_command, cwd=ROOT, capture_output=True, text=True, timeout=args.warmup + args.seconds + 30, check=True)
            status = server.wait(timeout=30)
            if status:
                raise RuntimeError(f"server exited with status {status}")
            data = json.loads(result.stdout)
            return dict(data, backend=backend, workers=workers, client_workers=args.client_workers, server_cpus=server_cpus, client_cpus=args.client_cpus)
        except Exception as error:
            if isinstance(error, subprocess.CalledProcessError):
                print(error.stderr, file=sys.stderr)
            errors.seek(0)
            print(errors.read(), file=sys.stderr)
            raise
        finally:
            if server.poll() is None:
                server.kill()
            server.wait()
            server.stdout.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 4])
    parser.add_argument("--connections", type=int, nargs="+", default=[1, 32])
    parser.add_argument("--bytes", type=int, nargs="+", default=[64, 4096])
    parser.add_argument("--backends", nargs="+", default=["tokio", "tokio-adapter-monitor"] + (["native-monitor", "iouring-monitor", "iouring", "monoio", "monoio-legacy"] if sys.platform == "linux" else []))
    parser.add_argument("--client-binary", type=Path, help="reuse a fixed client executable instead of the newly built binary")
    parser.add_argument("--client-workers", type=int, default=4)
    parser.add_argument("--warmup", type=float, default=1)
    parser.add_argument("--seconds", type=float, default=3)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--server-cpus", type=cpu_list, default=[])
    parser.add_argument("--client-cpus", type=cpu_list, default=[])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.client_binary:
        args.client_binary = args.client_binary.resolve()
        if not args.client_binary.is_file():
            parser.error("client binary must exist")
    if min(args.workers + args.connections + args.bytes + [args.client_workers, args.repeats]) < 1 or args.warmup < 0 or args.seconds <= 0:
        parser.error("counts and measurement duration must be positive; warmup must be nonnegative")
    if args.server_cpus and len(args.server_cpus) < max(args.workers):
        parser.error("provide at least one server CPU per worker")
    if set(args.server_cpus) & set(args.client_cpus):
        parser.error("server and client CPU sets must be disjoint")
    artifacts = command_output("cargo", "build", "--release", "--locked", "-p", "core-scheduler-echo-benchmark", "--message-format=json")
    binary = next(Path(artifact["executable"]) for line in artifacts.splitlines()
                  if (artifact := json.loads(line)).get("executable")
                  and artifact["target"]["name"] == "core-scheduler-echo-benchmark")
    report = {
        "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "hostname": platform.node(), "platform": platform.platform(),
        "commit": command_output("git", "rev-parse", "HEAD"),
        "dirty": bool(command_output("git", "status", "--porcelain")),
        "rustc": command_output("rustc", "--version"),
        "cargo": command_output("cargo", "--version"),
        "arguments": dict(vars(args), output=str(args.output), client_binary=str(args.client_binary) if args.client_binary else None),
        "binaries": {name: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                     for name, path in [("server", binary), ("client", args.client_binary or binary)]},
        "cpu_topology": command_output("lscpu", "-e=CPU,CORE,SOCKET,NODE,CACHE") if sys.platform == "linux" else None,
        "cpu_model": command_output("lscpu") if sys.platform == "linux" else platform.processor(),
        "load_average_before": os.getloadavg() if hasattr(os, "getloadavg") else None,
        "samples": [],
    }
    rng = random.Random(20261019)
    for repeat in range(args.repeats):
        cases = list(itertools.product(args.workers, args.connections, args.bytes))
        rng.shuffle(cases)
        for workers, connections, size in cases:
            backends = args.backends.copy()
            rng.shuffle(backends)
            for backend in backends:
                data = sample(binary, backend, workers, connections, size, args)
                report["samples"].append(dict(data, repeat=repeat + 1))
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(json.dumps(report, indent=2) + "\n")
                print(f"{len(report['samples']):3} {backend:13} workers={workers} conns={connections:3} bytes={size:5}: {data['requests_per_second']:10.0f} req/s  p50={data['rtt_p50_us']:8.2f} us p99={data['rtt_p99_us']:8.2f} us", flush=True)
    report["load_average_after"] = os.getloadavg() if hasattr(os, "getloadavg") else None
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print("\nMedian requests/s across repetitions:")
    for workers, connections, size in itertools.product(args.workers, args.connections, args.bytes):
        values = []
        for backend in args.backends:
            selected = [row["requests_per_second"] for row in report["samples"] if (row["workers"], row["connections"], row["bytes"], row["backend"]) == (workers, connections, size, backend)]
            values.append(f"{backend}={statistics.median(selected):.0f}")
        print(f"workers={workers} conns={connections} bytes={size}: " + "  ".join(values))


if __name__ == "__main__":
    main()
