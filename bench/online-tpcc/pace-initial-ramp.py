"""Pace the already-running local coordinator; SQL workers continue unchanged."""
import os,signal,time,sys,collections,importlib.util
from common import *
phase='gradual-density-r1'
pid=json.loads((PRIVATE/(phase+'-process.json')).read_text())['pid']
spec=importlib.util.spec_from_file_location('density_stage',ROOT/'bench/online-tpcc/density-stage.py');stage=importlib.util.module_from_spec(spec);spec.loader.exec_module(stage)
log=(EVIDENCE/'events.jsonl').open();active=[];windows=[]
def consume():
 for line in log:
  try:x=json.loads(line)
  except ValueError:continue
  if x.get('phase')==phase and x['kind']=='workload_start' and x['instance'] not in active:active.append(x['instance'])
def check():
 if (ROOT/'.work/STOP').exists():raise RuntimeError('resource STOP')
 os.kill(pid,0)
def wait(until):
 while time.time()<until:check();consume();time.sleep(min(1,max(0,until-time.time())))
def signal_parent(sig):
 assert b'gradual-density.py' in pathlib.Path(f'/proc/{pid}/cmdline').read_bytes()
 os.kill(pid,sig)
consume()
try:
 while len(active)<320:
  check();signal_parent(signal.SIGSTOP);hold=time.time();consume()
  event('gradual_coordinator_hold',phase=phase,instances=list(active),hold_start=hold,coordinator_pid=pid,note='Only local orchestration is paused; any overlapping activation latency may include harness pause and must be excluded from cold latency statistics. SQL worker latencies are unaffected.')
  rows=stage.snapshot(phase+'-paced'+str(len(active))+'-'+str(int(hold)))
  ids={json.loads((PRIVATE/f'instance-{n:03d}.json').read_text())['id'] for n in active}
  counts=collections.Counter(r['root']['node_id'] for r in rows if r['instance_id'] in ids)
  wait(hold+30);start=time.time();event('gradual_paced_measurement_start',phase=phase,target=len(active),start=start,seconds=60)
  wait(start+60);end=time.time()
  windows.append({'target':len(active),'start':start,'end':end,'instances':list(active),'node_counts':dict(counts),'complete':True,'protocol':'initial ramp hold30 then common60; existing TPC workers uninterrupted'})
  record(phase+'-supplemental-windows',{'phase':phase,'windows':windows})
  event('gradual_paced_measurement_complete',phase=phase,target=len(active),start=start,end=end)
  target=min(320,len(active)+8);signal_parent(signal.SIGCONT);event('gradual_coordinator_resumed',phase=phase,next_target=target)
  print('completed paced density',len(active),'next',target,flush=True)
  while len(active)<target:check();consume();time.sleep(.05)
 # At320 the original coordinator already waits120s and measures300s.
 event('gradual_initial_pacing_complete',phase=phase,active=len(active))
finally:
 try:signal_parent(signal.SIGCONT)
 except ProcessLookupError:pass
