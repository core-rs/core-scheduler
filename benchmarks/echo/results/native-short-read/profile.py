#!/usr/bin/env python3
"""Collect matched server CPU/syscall counters and sampled profiles; not capacity results."""
import argparse, csv, hashlib, importlib.util, json, random, shlex, subprocess, tempfile
from pathlib import Path
from types import SimpleNamespace
ROOT = Path(__file__).resolve().parents[4]
spec = importlib.util.spec_from_file_location('echo_run', ROOT / 'benchmarks/echo/run.py')
run = importlib.util.module_from_spec(spec); spec.loader.exec_module(run)
p = argparse.ArgumentParser(description=__doc__)
for name in ('before','after','client','output'): p.add_argument('--'+name, type=Path, required=True)
a=p.parse_args(); a.output.mkdir(parents=True,exist_ok=True)
settings=SimpleNamespace(server_cpus=[0],client_cpus=[4,5,6,7],client_binary=a.client.resolve(),client_workers=4,warmup=0,seconds=3)
variants={'baseline':(a.before.resolve(),'iouring-monitor'),'short-read':(a.after.resolve(),'iouring-monitor'),'tokio':(a.before.resolve(),'tokio')}
events=['cycles:u','instructions:u','cycles:k','instructions:k','context-switches','cpu-migrations','syscalls:sys_enter_recvfrom','syscalls:sys_enter_sendto','syscalls:sys_enter_io_uring_enter','syscalls:sys_enter_epoll_wait','syscalls:sys_enter_epoll_pwait','syscalls:sys_enter_epoll_pwait2']
report={'settings':vars(settings)|{'client_binary':str(settings.client_binary)},'events':events,'samples':[],'profiles':[]}
with tempfile.TemporaryDirectory(prefix='core-scheduler-profile-') as directory:
 directory=Path(directory)
 jobs=[(n,i) for i in range(3) for n in variants];random.Random(20261024).shuffle(jobs)
 for name,i in jobs:
  binary,backend=variants[name]; raw=a.output/f'{name}-{i}.stat.csv'; wrapper=directory/'server'
  wrapper.write_text('#!/bin/sh\nexec '+shlex.join(['sudo','-n','perf','stat','-x,','-e',','.join(events),'-o',str(raw),'--',str(binary)])+' "$@"\n'); wrapper.chmod(0o700)
  s=run.sample(wrapper,backend,1,32,64,settings)
  vals={row[2]:float(row[0]) for row in csv.reader(raw.read_text().splitlines()) if len(row)>=3 and row[2] in events}
  report['samples'].append(dict(s,variant=name,repeat=i,counters=vals,per_request={k:v/s['requests'] for k,v in vals.items()}))
  (a.output/'stats.json').write_text(json.dumps(report,indent=2)+'\n'); print(name,i,report['samples'][-1]['per_request'],flush=True)
 settings.seconds=6
 for name in ['short-read','tokio','baseline']:
  binary,backend=variants[name]; data=a.output/f'{name}.data'; wrapper=directory/'server'
  wrapper.write_text('#!/bin/sh\nexec '+shlex.join(['sudo','-n','perf','record','-F','499','-e','cycles','-g','--call-graph','dwarf,16384','-o',str(data),'--',str(binary)])+' "$@"\n');wrapper.chmod(0o700)
  s=run.sample(wrapper,backend,1,32,64,settings); report['profiles'].append(dict(s,variant=name,binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest()))
  for suffix,flags in [('self',['--no-children']),('children',['--children'])]:
   with (a.output/f'{name}.{suffix}.txt').open('w') as f: subprocess.run(['sudo','-n','perf','report','--stdio','--header','--percent-limit','0.2','-i',str(data),*flags],stdout=f,check=True)
  (a.output/'stats.json').write_text(json.dumps(report,indent=2)+'\n');print('profiled',name,flush=True)
