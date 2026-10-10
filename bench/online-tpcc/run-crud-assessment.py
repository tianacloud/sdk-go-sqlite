"""Supervise the authorized CRUD retest and restore admission on completion."""
import subprocess,sys,time,os,json
from common import *
phase=sys.argv[1];stop=ROOT/'.work/STOP'
processes=json.loads((PRIVATE/'crud-r1-processes.json').read_text())
p=None
try:
 # The prior round is terminal; save its stop, then clear only after fresh healthy samples.
 latest={}
 for line in (EVIDENCE/'metrics-live.jsonl').read_text().splitlines():
  r=json.loads(line)
  if r['name'] in ['cpu','cpu_instant','memory','sample_age']:latest[r['name']]=r
 assert len(latest)==4
 for name,r in latest.items():
  assert time.time()-r['time']<30
  values=r['response']['data']['result'];limit=60 if name=='sample_age' else 70
  assert len(values)>=7 and all(float(v['value'][1])<limit for v in values)
 for info in processes.values():os.kill(info['pid'],0)
 assert not (ROOT/'.work'/('END-'+phase)).exists()
 record(phase+'-previous-stop',json.loads(stop.read_text()))
 stop.unlink();event('resource_stop_cleared_after_verified_recovery',phase=phase,policy='CRUD read50/write50;95resource stop;85watermark')
 (ROOT/'.work/RETAIN-SESSIONS').touch()
 f=(PRIVATE/(phase+'.log')).open('ab',buffering=0)
 p=subprocess.Popen([sys.executable,str(ROOT/'bench/online-tpcc/crud-density.py'),phase],cwd=ROOT,stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT)
 record('coordinator-process',{'phase':phase,'pid':p.pid,'start':time.time()})
 record(phase+'-coordinator-process',{'phase':phase,'pid':p.pid,'start':time.time()})
 save_private('runner-state.json',{'time':time.time(),'current_phase':phase,'coordinator_pid':p.pid,'supervisor_pid':os.getpid(),'goal':'active CRUD retest, read50/write50; previous goal operation-ratio wording superseded','evidence_directory':str(EVIDENCE),'admission_current_percent':100,'admission_restore_required':True,'resource_stop_percent':95,'watermark_percent':85,'retain_instances':True,'observer_processes':processes,'next':['Monitor per-node progress and full windows; do not launch another coordinator','Supervisor restores Control80 after phase exit; continue recovery observation and update report']})
 while p.poll() is None:
  for name,info in processes.items():
   try:os.kill(info['pid'],0)
   except ProcessLookupError:
    if not stop.exists():stop.write_text(json.dumps({'time':time.time(),'reason':'observer/bridge stopped: '+name}))
  time.sleep(5)
 record('coordinator-terminal',{'phase':phase,'time':time.time(),'exit_code':p.returncode})
 record(phase+'-coordinator-terminal',{'phase':phase,'time':time.time(),'exit_code':p.returncode})
finally:
 if not stop.exists():stop.write_text(json.dumps({'time':time.time(),'reason':'CRUD evaluation ended; withdraw retained test sessions'}))
 env=dict(os.environ,TPCC_ADMISSION_OPERATION_KEY='online-'+phase+'-restore80')
 result=subprocess.run([sys.executable,str(ROOT/'bench/online-tpcc/restore-admission.py')],cwd=ROOT,env=env)
 record('supervisor-restoration-result',{'phase':phase,'time':time.time(),'exit_code':result.returncode})
 record(phase+'-supervisor-restoration-result',{'phase':phase,'time':time.time(),'exit_code':result.returncode})
