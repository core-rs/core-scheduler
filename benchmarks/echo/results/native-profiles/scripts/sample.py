#!/usr/bin/env python3
"""Profile current native readiness, owned completions, and pure Tokio on Linux."""

import json
import os
from pathlib import Path
import queue
import resource
import signal
import subprocess
import sys
import threading
import time


def usage():
    value = resource.getrusage(resource.RUSAGE_CHILDREN)
    return dict(user=value.ru_utime, system=value.ru_stime,
                voluntary_switches=value.ru_nvcsw, involuntary_switches=value.ru_nivcsw)


def difference(before, after):
    return {key: after[key] - value for key, value in before.items()}


def sample(args, mode, backend, workers, connections, size, repeat):
    identifier = f"{mode}-{backend}-w{workers}-c{connections}-b{size}-r{repeat}"
    prefix = []
    if mode == "perf":
        prefix = ["perf", "record", "-q", "-m", "32", "-e", args.perf_event, "-F", "999",
                  "--call-graph", "dwarf,8192", "-o", str(args.output / (identifier + ".data")), "--"]
    elif mode == "stat":
        events = ["task-clock", "cycles:u", "cycles:k", "instructions:u", "instructions:k", "context-switches", "cpu-migrations"]
        events += ["syscalls:sys_enter_" + name for name in [
            "recvfrom", "sendto", "io_uring_enter", "epoll_ctl", "epoll_wait", "epoll_pwait", "read", "write", "futex",
        ]]
        prefix = ["perf", "stat", "-x", ";", "-e", ",".join(events),
                  "-e", "syscalls:sys_exit_recvfrom", "--filter", "ret == -11",
                  "-o", str(args.output / (identifier + ".stat")), "--"]
    elif mode == "strace":
        prefix = ["strace", "-f", "-c", "-S", "calls", "-o", str(args.output / (identifier + ".strace"))]
    privileged = args.privileged_perf and mode in ["perf", "stat"]
    if privileged:
        # Give only the profiler elevated access; execute the server as the original user.
        # setpriv execs in place, keeping descendants in the supervisor's group.
        prefix = ["sudo", "-n", sys.executable, str(Path(__file__).with_name("supervise.py"))] + prefix + [
            "setpriv", f"--reuid={os.getuid()}", f"--regid={os.getgid()}", "--init-groups", "--",
        ]
    warmup, seconds = (0, 0.5) if mode == "strace" else ((0, 3) if mode == "stat" else (1, 5 if mode == "perf" else 3))
    with (args.output / (identifier + ".stderr")).open("w+") as errors:
        started = time.monotonic()
        server = subprocess.Popen(
            prefix + ["taskset", "-c", ",".join(map(str, range(workers))), str(args.server),
                      "server", backend, str(workers), str(connections)],
            stdin=subprocess.PIPE if privileged else subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=errors, text=True, start_new_session=True,
        )
        try:
            ready = queue.Queue()
            threading.Thread(target=lambda: ready.put(server.stdout.readline()), daemon=True).start()
            address = ready.get(timeout=30).strip()
            if not address:
                raise RuntimeError("server exited before printing its address")
            before = usage()
            client = subprocess.run(
                ["taskset", "-c", "4-7", str(args.client), "client", address, "4",
                 str(connections), str(size), str(warmup), str(seconds)],
                capture_output=True, text=True, check=True, timeout=warmup + seconds + 60,
            )
            after_client = usage()
            if server.wait(timeout=30):
                raise RuntimeError("server or profiler failed")
            return dict(json.loads(client.stdout), identifier=identifier, mode=mode,
                        backend=backend, workers=workers, repeat=repeat,
                        client_cpu=difference(before, after_client),
                        server_cpu=difference(after_client, usage()),
                        lifecycle_seconds=time.monotonic() - started)
        except BaseException:
            errors.seek(0)
            print(errors.read(), flush=True)
            raise
        finally:
            if privileged:
                # The privileged supervisor owns termination of its process group.
                server.stdin.close()
                server.wait(timeout=10)
            else:
                # A profiler can exit while its server child still owns stdout.
                try:
                    os.killpg(server.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                server.wait()
            server.stdout.close()
