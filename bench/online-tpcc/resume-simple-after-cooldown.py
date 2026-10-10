"""Resume the authorized slow ramp only after fresh recovery evidence."""
import sys,time,subprocess,importlib.util
from common import *
sys.path.insert(0,str(pathlib.Path.home()/'.codex/skills/tiana-debug/scripts'))
from tiana_debug import Backend
spec=importlib.util.spec_from_file_location('density_stage',ROOT/'bench/online-tpcc/density-stage.py');stage=importlib.util.module_from_spec(spec);spec.loader.exec_module(stage)
b=Backend(PROFILE)
stop=ROOT/'.work/STOP';phase='simple-density-r1'
expected_stop=1791605247.0940435
while True:
 ready=EVIDENCE/'gradual-r2-cooldown-ready.json'
 if not ready.exists():time.sleep(15);continue
 # Require the original stop; never clear a different or newly raised stop.
 assert stop.exists() and json.loads(stop.read_text())['time']==expected_stop
 rows=stage.snapshot('before-simple-recovery-'+str(int(time.time())))
 busy=[r for r in rows if r['root']['runtime_state']=='DRAINING']
 active=[r for r in rows if r['root']['runtime_state']=='READY']
 if busy or len(active)>10:
  event('simple_resume_wait_lifecycle',draining=len(busy),ready=len(active));time.sleep(30);continue
 values={};ok=True
 for name,q in {'cpu':'100*(1-avg by(node_id)(irate(node_cpu_seconds_total{mode="idle"}[1m])))','cpu_rate':'100*(1-avg by(node_id)(rate(node_cpu_seconds_total{mode="idle"}[1m])))','memory':'100*(1-node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes)','age':'time()-timestamp(node_memory_MemAvailable_bytes)'}.items():
  r=b.get('prometheus','/api/v1/query',{'query':q});values[name]=r
  samples=r['data']['result'];limit=60 if name=='age' else 70
  if len(samples)<7 or any(not float(x['value'][1])<limit for x in samples):ok=False
 if not ok:time.sleep(15);continue
 observer=pathlib.Path('/proc/284369/cmdline')
 assert observer.exists() and b'bench/online-tpcc/observe.py' in observer.read_bytes()
 record('simple-resume-evidence',{'time':time.time(),'recovery':json.loads(ready.read_text()),'fresh_metrics':values,'draining':len(busy),'remaining_ready':len(active),'original_stop':json.loads(stop.read_text())})
 stop.unlink();event('resource_stop_cleared_after_verified_recovery',previous_stop=expected_stop,phase=phase,policy='CPU/memory95 STOP;85 watermark; simple SQL12QPS read/write1:1; retain instances')
 f=open(PRIVATE/(phase+'.log'),'ab',buffering=0)
 p=subprocess.Popen([sys.executable,str(ROOT/'bench/online-tpcc/simple-density.py'),phase],cwd=ROOT,stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
 save_private(phase+'-process.json',{'pid':p.pid,'start':time.time()})
 state=json.loads((PRIVATE/'runner-state.json').read_text());state.update(time=time.time(),current_phase=phase,coordinator_pid=p.pid,resource_stop_active=False);save_private('runner-state.json',state)
 print('simple SQL ramp started',p.pid,flush=True);break
