#!/usr/bin/env python3
"""Verify the setup flags actually accepted by all four benchmark workers."""
import argparse
import importlib.util
from pathlib import Path
import shlex
import subprocess
import tempfile
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[4]
spec = importlib.util.spec_from_file_location('echo_run', ROOT / 'benchmarks/echo/run.py')
run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run)
parser = argparse.ArgumentParser(description=__doc__)
for name in ('baseline', 'deferred', 'single', 'client', 'output'):
    parser.add_argument('--' + name, type=Path, required=True)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
settings = SimpleNamespace(server_cpus=[0, 1, 2, 3], client_cpus=[4, 5, 6, 7],
    client_binary=args.client.resolve(), client_workers=4, warmup=0, seconds=0.1)
with tempfile.TemporaryDirectory(prefix='core-scheduler-ring-setup-') as directory:
    wrapper = Path(directory) / 'server'
    for name in ('baseline', 'deferred', 'single'):
        data = args.output / (name + '.data')
        wrapper.write_text('#!/bin/sh\nexec ' + shlex.join(['sudo', '-n', 'perf', 'record',
            '-e', 'io_uring:io_uring_create', '-o', str(data), '--', str(getattr(args, name).resolve())]) + ' "$@"\n')
        wrapper.chmod(0o700)
        run.sample(wrapper, 'iouring-monitor', 4, 1, 64, settings)
        with (args.output / (name + '.txt')).open('w') as output:
            subprocess.run(['sudo', '-n', 'perf', 'script', '-i', str(data)], stdout=output, check=True)
