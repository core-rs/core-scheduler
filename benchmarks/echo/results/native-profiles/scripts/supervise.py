#!/usr/bin/env python3
"""Run a profiler in its own process group, retiring it when the caller leaves.

Used under sudo by run.py. The profiler must drop privileges before executing the
workload. Commands must not create another session or process group.
"""

import os
import selectors
import signal
import subprocess
import sys
import time


def supervise(command, timeout=120):
    def interrupted(signum, frame):
        raise InterruptedError(f"supervisor received signal {signum}")

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupted)
    child = subprocess.Popen(command, stdin=subprocess.DEVNULL, start_new_session=True)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(sys.stdin, selectors.EVENT_READ)
            deadline = time.monotonic() + timeout
            while child.poll() is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError("profiler exceeded supervisor deadline")
                if selector.select(timeout=0.1) and not os.read(sys.stdin.fileno(), 4096):
                    raise InterruptedError("profiler caller closed its control pipe")
            return child.returncode
    finally:
        # Also retire descendants when the profiler exits before its workload.
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, signal.SIG_IGN)
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait()


if __name__ == "__main__":
    sys.exit(supervise(sys.argv[1:]))
