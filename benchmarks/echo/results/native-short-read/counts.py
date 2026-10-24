#!/usr/bin/env python3
"""Count server receive syscalls separately from uninstrumented capacity runs."""
import argparse
import csv
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import tempfile
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[4]
spec = importlib.util.spec_from_file_location('echo_run', ROOT / 'benchmarks/echo/run.py')
run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run)

parser = argparse.ArgumentParser(description=__doc__)
for name in ('before', 'after', 'client', 'output'):
    parser.add_argument('--' + name, type=Path, required=True)
parser.add_argument('--sudo', action='store_true', help='Read restricted syscall tracepoints through sudo -n perf')
args = parser.parse_args()
if args.output.exists():
    parser.error('output already exists')
settings = SimpleNamespace(server_cpus=[0], client_cpus=[4, 5, 6, 7],
    client_binary=args.client.resolve(), client_workers=4, warmup=0, seconds=3)
report = {'event': 'syscalls:sys_enter_recvfrom', 'warmup': 0,
    'note': 'No warmup: reported requests and syscall counts cover the same interval, plus connection startup and EOF. Instrumented throughput is not capacity evidence.',
    'sudo_perf': args.sudo, 'samples': []}
with tempfile.TemporaryDirectory(prefix='core-scheduler-short-counts-') as directory:
    directory = Path(directory)
    for variant, binary, backend in [('baseline', args.before, 'iouring-monitor'),
                                     ('short-read', args.after, 'iouring-monitor'),
                                     ('tokio', args.before, 'tokio')]:
        binary = binary.resolve()
        raw = directory / (variant + '.csv')
        wrapper = directory / (variant + '.sh')
        wrapper.write_text('#!/bin/sh\nexec ' + shlex.join((['sudo', '-n'] if args.sudo else []) + ['perf', 'stat', '-x,', '-e',
            report['event'], '-o', str(raw), '--', str(binary)]) + ' "$@"\n')
        wrapper.chmod(0o700)
        sample = run.sample(wrapper, backend, 1, 32, 64, settings)
        text = raw.read_text()
        counts = [int(row[0]) for row in csv.reader(text.splitlines())
                  if len(row) >= 3 and row[2] == report['event']]
        if len(counts) != 1:
            raise RuntimeError(text)
        report['samples'].append(dict(sample, variant=variant, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
            receive_syscalls=counts[0], receives_per_echo=counts[0] / sample['requests'], perf_csv=text))
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        print(variant, 'receive syscalls per echo:', counts[0] / sample['requests'], flush=True)
