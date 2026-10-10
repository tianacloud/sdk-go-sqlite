"""Continuous TPC-C, slow additions, and explicit common measurement windows."""
import collections, importlib.util, subprocess, sys, time
from workload import Client
from common import *
spec=importlib.util.spec_from_file_location('density_stage',ROOT/'bench/online-tpcc/density-stage.py')
stage=importlib.util.module_from_spec(spec);spec.loader.exec_module(stage)
phase=sys.argv[1]
cohort=json.loads((EVIDENCE/'density-400-r4-cohort.json').read_text())['active_instances']
placement=json.loads((EVIDENCE/'density-400-r4-1791600659-placement.json').read_text())['items']
ids={json.loads((PRIVATE/f'instance-{n:03d}.json').read_text())['id']:n for n in cohort}
lanes=collections.defaultdict(list)
for row in placement:
 if row['instance_id'] in ids:lanes[row['root']['node_id']].append(ids[row['instance_id']])
for lane in lanes.values():lane.sort()
candidates=[]
while any(lanes.values()):
 for node in sorted(lanes):
  if lanes[node]:candidates.append(lanes[node].pop(0))
next_fixture=406
procs={};windows=[];failures=[];end=ROOT/'.work'/('END-'+phase)
if end.exists():raise RuntimeError('phase already ended; use a new phase name')
def check():
 stage.stopped()
 dead=[n for n,p in procs.items() if p.poll() is not None]
 if dead:raise RuntimeError('TPC processes exited before end: '+str(dead))
def wait(seconds):
 until=time.time()+seconds
 while time.time()<until:check();time.sleep(min(2,max(0,until-time.time())))
def save():
 record(phase+'-progress',{'time':time.time(),'phase':phase,'active_instances':list(procs),'activation_failures':failures,'windows':windows,'target':400,'add_step':8,'sql_qps':12})
try:
 for target in [320,*range(328,401,8)]:
  event('gradual_stage_ramp_start',phase=phase,target=target,active=len(procs))
  while len(procs)<target:
   check()
   if not candidates:
    if next_fixture>801:raise RuntimeError('owned replacement pool exhausted')
    check();event('gradual_replacement_fixture_start',phase=phase,instance=next_fixture,active=len(procs))
    subprocess.run([sys.executable,str(ROOT/'bench/online-tpcc/workload.py'),'load',str(next_fixture),'0','200'],cwd=ROOT,check=True)
    candidates.append(next_fixture);next_fixture+=1
   n=candidates.pop(0);client=Client(n,phase+'-activation');ok=False
   try:client.execute('SELECT 1');ok=True
   except Exception as exc:
    failures.append({'instance':n,'time':time.time(),'error':str(exc)})
    event('gradual_activation_failed',phase=phase,instance=n,error=str(exc))
   finally:client.finish(ok)
   if not ok:save();continue
   procs[n]=subprocess.Popen([sys.executable,str(ROOT/'bench/online-tpcc/workload.py'),'run',str(n),phase,'3600','12'],cwd=ROOT)
   wait(.5)
  save();rows=stage.snapshot(phase+'-density'+str(target)+'-before')
  wanted={json.loads((PRIVATE/f'instance-{n:03d}.json').read_text())['id'] for n in procs}
  counts=collections.Counter(r['root']['node_id'] for r in rows if r['instance_id'] in wanted)
  warmup=120 if target==320 else 30
  duration=300 if target==320 else 60
  event('gradual_stage_settle',phase=phase,target=target,node_counts=dict(counts),settle_seconds=warmup)
  wait(warmup);start=time.time()
  event('gradual_measurement_start',phase=phase,target=target,start=start,seconds=duration,instances=list(procs))
  wait(duration);finish=time.time()
  windows.append({'target':target,'start':start,'end':finish,'instances':list(procs),'node_counts':dict(counts),'complete':True})
  save();event('gradual_measurement_complete',phase=phase,target=target,start=start,end=finish)
  print('completed density',target,dict(counts),flush=True)
except Exception as exc:
 event('gradual_run_interrupted',phase=phase,error=str(exc),active_instances=list(procs));save();raise
finally:
 end.write_text('Finish transactions and retain successful sessions; STOP still overrides retention.\n')
 for p in procs.values():
  try:p.wait(timeout=50)
  except subprocess.TimeoutExpired:event('gradual_worker_exit_pending',phase=phase,pid=p.pid)
 event('gradual_run_ended',phase=phase,exit_codes={n:p.poll() for n,p in procs.items()},complete_windows=len(windows))
