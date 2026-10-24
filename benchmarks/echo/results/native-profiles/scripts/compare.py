#!/usr/bin/env python3
"""Randomized, matched echo controls for separately built server variants."""
import argparse, contextlib, datetime, fcntl, hashlib, itertools, json, os, random, subprocess, types
from pathlib import Path
from sample import sample
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--variant', action='append', required=True, help='label:backend:binary')
p.add_argument('--client', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--repeats', type=int, default=5)
p.add_argument('--matrix', action='store_true')
p.add_argument('--low-latency', action='store_true', help='Hold a temporary zero-latency CPU idle QoS request')
p.add_argument('--performance-governor', action='store_true', help='Temporarily select performance on benchmark CPUs; restore every setting afterward')
p.add_argument('--case', action='append', help='workers,connections,bytes; repeat for selected cases')
a = p.parse_args()
a.output.mkdir(parents=True, exist_ok=False)
variants = [v.split(':', 2) for v in a.variant]
report = dict(started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), load_before=os.getloadavg(), platform=subprocess.check_output(['uname', '-a'], text=True).strip(), binaries={label: dict(backend=backend, path=path, sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest()) for label, backend, path in variants}, client=dict(path=str(a.client), sha256=hashlib.sha256(a.client.read_bytes()).hexdigest()), samples=[])
geometries = [tuple(map(int, case.split(','))) for case in a.case] if a.case else list(itertools.product([1, 4] if a.matrix else [1], [1, 32] if a.matrix else [32], [64, 4096]))
cases = [(r, *geometry, v) for r, geometry, v in itertools.product(range(1, a.repeats + 1), geometries, variants)]
random.Random(20261024).shuffle(cases)

@contextlib.contextmanager
def cpu_policy():
    saved = []
    holder = None

    def write(path, value):
        subprocess.run(['sudo', '-n', 'tee', str(path)], input=value + '\n', stdout=subprocess.DEVNULL, text=True, check=True)
    try:
        if a.low_latency:
            code = "import struct,sys; handle=open('/dev/cpu_dma_latency','wb',buffering=0); handle.write(struct.pack('i',0)); print('ready',flush=True); sys.stdin.buffer.read()"
            holder = subprocess.Popen(['sudo', '-n', 'python3', '-c', code], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            if holder.stdout.readline().strip() != 'ready':
                raise RuntimeError('could not establish the CPU idle latency constraint')
        report['cpu_idle_latency_us'] = 0 if holder else None
        if a.performance_governor:
            for cpu in range(8):
                policy = Path(f'/sys/devices/system/cpu/cpu{cpu}/cpufreq')
                governor = policy / 'scaling_governor'
                preference = policy / 'energy_performance_preference'
                saved.append((governor, governor.read_text().strip(), preference, preference.read_text().strip()))
                write(governor, 'performance')
        report['cpu_policy'] = [dict(path=str(g), governor_before=gv, preference_before=pv, governor_during=g.read_text().strip(), preference_during=p.read_text().strip()) for g, gv, p, pv in saved]
        yield
    finally:
        if holder:
            holder.stdin.close()
            holder.wait(timeout=10)
            holder.stdout.close()
        for governor, value, preference, original in reversed(saved):
            write(governor, value)
            write(preference, original)
        report['cpu_policy_restored'] = all((g.read_text().strip() == gv and p.read_text().strip() == pv for g, gv, p, pv in saved))
with open('/tmp/core-scheduler-perf.lock', 'a') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    report['started_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with cpu_policy():
        for repeat, workers, connections, size, (label, backend, path) in cases:
            output = a.output / label
            output.mkdir(exist_ok=True)
            args = types.SimpleNamespace(server=Path(path), client=a.client, output=output, privileged_perf=False)
            s = sample(args, 'control', backend, workers, connections, size, repeat)
            s['variant'] = label
            report['samples'].append(s)
            (a.output / 'aiko.json').write_text(json.dumps(report, indent=2) + '\n')
            print(f"{len(report['samples'])}/{len(cases)} {label} {s['identifier']}: {s['requests_per_second']:,.0f}", flush=True)
report['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
report['load_after'] = os.getloadavg()
(a.output / 'aiko.json').write_text(json.dumps(report, indent=2) + '\n')
