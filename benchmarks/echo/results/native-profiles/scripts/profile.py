#!/usr/bin/env python3
"""Measure the matched native baseline and deferred-worker implementation."""
import argparse, datetime, fcntl, hashlib, json, random, subprocess, types
from pathlib import Path
from sample import sample
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--variant', action='append', required=True, help='label:backend:binary')
p.add_argument('--client', type=Path, required=True)
p.add_argument('--connections', type=int, default=32)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
a.output.mkdir(parents=True, exist_ok=False)
variants = [v.split(':', 2) for v in a.variant]
report = dict(started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), binaries={label: dict(backend=backend, path=path, sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest()) for label, backend, path in variants}, client=dict(path=str(a.client), sha256=hashlib.sha256(a.client.read_bytes()).hexdigest()), samples=[])
with open('/tmp/core-scheduler-perf.lock', 'a') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    report['started_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for mode, repeats in [('stat', 2), ('perf', 1)]:
        cases = [(r, v) for r in range(1, repeats + 1) for v in variants]
        random.Random(20261024).shuffle(cases)
        for repeat, (label, backend, path) in cases:
            output = a.output / label
            output.mkdir(exist_ok=True)
            args = types.SimpleNamespace(server=Path(path), client=a.client, output=output, privileged_perf=True, perf_event='cycles')
            s = sample(args, mode, backend, 1, a.connections, 64, repeat)
            s['variant'] = label
            report['samples'].append(s)
            (a.output / 'aiko.json').write_text(json.dumps(report, indent=2) + '\n')
            print(f"{label} {s['identifier']}: {s['requests_per_second']:,.0f}", flush=True)
    for s in report['samples']:
        if s['mode'] != 'perf':
            continue
        output = a.output / s['variant']
        for suffix, options in [('self', ['--no-children', '-g', 'none', '--percent-limit', '0.1']), ('callgraph', ['--children', '-g', 'graph,1,caller', '--percent-limit', '1'])]:
            with (output / f"{s['identifier']}.{suffix}.txt").open('w') as f:
                subprocess.run(['sudo', '-n', 'perf', 'report', '--stdio', '--header', '--show-nr-samples', '--sort', 'symbol', *options, '-i', str(output / f"{s['identifier']}.data")], stdout=f, check=True)
report['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
(a.output / 'aiko.json').write_text(json.dumps(report, indent=2) + '\n')
