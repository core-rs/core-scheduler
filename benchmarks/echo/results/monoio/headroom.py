#!/usr/bin/env python3
"""Linux process-CPU and client-capacity checks for the fixed echo workload."""

import argparse
import hashlib
import itertools
import json
import os
from pathlib import Path
import queue
import random
import signal
import subprocess
import tempfile
import threading
import time


def finish(process, started, timeout=40):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        pid, status, usage = os.wait4(process.pid, os.WNOHANG)
        if pid:
            process.returncode = os.waitstatus_to_exitcode(status)
            elapsed = time.monotonic() - started
            if process.returncode:
                raise RuntimeError(f"process failed: {process.returncode}")
            return {
                "elapsed_seconds": elapsed,
                "user_seconds": usage.ru_utime,
                "system_seconds": usage.ru_stime,
                "average_cpu_cores": (usage.ru_utime + usage.ru_stime) / elapsed,
                "voluntary_context_switches": usage.ru_nvcsw,
                "involuntary_context_switches": usage.ru_nivcsw,
            }
        time.sleep(0.005)
    raise TimeoutError("echo process did not finish")


def sample(binary, backend, size, client_workers, client_cpu_start):
    clients = list(range(client_cpu_start, client_cpu_start + client_workers))
    processes = []
    with tempfile.TemporaryFile(mode="w+") as errors:
        try:
            server_started = time.monotonic()
            server = subprocess.Popen(["taskset", "-c", "0-3", str(binary), "server", backend, "4", "32"], stdout=subprocess.PIPE, stderr=errors, text=True, start_new_session=True)
            processes.append(server)
            ready = queue.Queue()
            threading.Thread(target=lambda: ready.put(server.stdout.readline()), daemon=True).start()
            address = ready.get(timeout=30).strip()
            if not address:
                raise RuntimeError("server did not report its address")
            client_started = time.monotonic()
            client = subprocess.Popen(["taskset", "-c", ",".join(map(str, clients)), str(binary), "client", address, str(client_workers), "32", str(size), "1", "3"], stdout=subprocess.PIPE, stderr=errors, text=True, start_new_session=True)
            processes.append(client)
            client_cpu = finish(client, client_started)
            server_cpu = finish(server, server_started)
            result = json.loads(client.stdout.read())
            return dict(result, backend=backend, workers=4, client_workers=client_workers, server_cpus=[0, 1, 2, 3], client_cpus=clients, server_process=server_cpu, client_process=client_cpu)
        except Exception:
            errors.seek(0)
            print(errors.read(), flush=True)
            raise
        finally:
            for process in processes:
                if process.returncode is None:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
                process.stdout.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--client-cpu-start", type=int, default=8, help="first client CPU; keep both client sizes within one L3 group")
    parser.add_argument("--backends", nargs="+", default=["tokio", "tokio-adapter-monitor", "native-monitor", "iouring-monitor", "iouring", "monoio", "monoio-legacy"])
    args = parser.parse_args()
    binary = args.binary.resolve()
    report = {
        "binary": str(binary), "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "method": "wait4 process CPU includes setup, warmup, measurement and cleanup; throughput counts only the shared three-second window",
        "cpu_topology": subprocess.check_output(["lscpu", "-e=CPU,CORE,SOCKET,NODE,CACHE"], text=True),
        "load_average_before": os.getloadavg(),
        "samples": [],
    }
    generator = random.Random(20261023)
    for repeat in range(args.repeats):
        cases = list(itertools.product(args.backends, [64, 4096], [4, 8]))
        generator.shuffle(cases)
        for backend, size, clients in cases:
            result = sample(binary, backend, size, clients, args.client_cpu_start)
            report["samples"].append(dict(result, repeat=repeat + 1))
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
            print(f"{len(report['samples'])}: {backend} bytes={size} clients={clients}: {result['requests_per_second']:.0f} req/s; client={result['client_process']['average_cpu_cores']:.2f} cores server={result['server_process']['average_cpu_cores']:.2f} cores", flush=True)
    report["load_average_after"] = os.getloadavg()
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
