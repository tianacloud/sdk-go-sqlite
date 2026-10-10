import subprocess,sys,time,os
from common import *
stop=ROOT/'.work/STOP';latest={}
for line in (EVIDENCE/'metrics-live.jsonl').read_text().splitlines():
 r=json.loads(line)
 if r['name'] in ['cpu','cpu_instant','memory','sample_age']:latest[r['name']]=r
assert len(latest)==4
for name,r in latest.items():
 assert time.time()-r['time']<30
 samples=r['response']['data']['result'];limit=60 if name=='sample_age' else 70
 assert len(samples)>=7 and all(float(x['value'][1])<limit for x in samples)
record('pre-creation-stop',json.loads(stop.read_text()));stop.unlink()
handed_off=False
try:
 result=subprocess.run([sys.executable,str(ROOT/'bench/online-tpcc/create-crud-capacity.py')],cwd=ROOT)
 if result.returncode or stop.exists():raise RuntimeError('Pool creation interrupted; preserve resource STOP and restore admission')
 stop.write_text(json.dumps({'time':time.time(),'reason':'New test pool ready; supervisor must verify fresh low resources before CRUD'}))
 f=(PRIVATE/'crud-r2-supervisor.log').open('ab',buffering=0)
 p=subprocess.Popen([sys.executable,str(ROOT/'bench/online-tpcc/run-crud-assessment.py'),'crud-density-r2'],cwd=ROOT,stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
 record('crud-r2-supervisor-process',{'pid':p.pid,'time':time.time()});handed_off=True
finally:
 if not handed_off:
  if not stop.exists():stop.write_text(json.dumps({'time':time.time(),'reason':'CRUD pool preparation ended unexpectedly'}))
  subprocess.run([sys.executable,str(ROOT/'bench/online-tpcc/restore-admission.py')],cwd=ROOT,env=dict(os.environ,TPCC_ADMISSION_OPERATION_KEY='crud-pool-failed-restore80'))
