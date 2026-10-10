"""Wait for verified withdrawal recovery, supplement the owned pool, then runr3."""
import sys,subprocess,math,os
from common import *
sys.path.insert(0,str(pathlib.Path.home()/'.codex/skills/tiana-debug/scripts'))
from tiana_debug import Backend
b=Backend(PROFILE);stop=ROOT/'.work/STOP';expected_stop=1791616245.7551756
queries={'cpu':'100*(1-avg by(node_id)(rate(node_cpu_seconds_total{mode="idle"}[1m])))','cpu_instant':'100*(1-avg by(node_id)(irate(node_cpu_seconds_total{mode="idle"}[1m])))','memory':'100*(1-node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes)','age':'time()-timestamp(node_memory_MemAvailable_bytes)'}
while True:
 assert stop.exists() and json.loads(stop.read_text())['time']==expected_stop,'A different STOP must be investigated'
 end=time.time();responses={};ok=True
 for name,q in queries.items():
  try:
   response=b.get('prometheus','/api/v1/query_range',{'query':q,'start':end-60,'end':end,'step':5});responses[name]=response;rows=response['data']['result'];limit=60 if name=='age' else 70
   if len(rows)<7 or any(len(row['values'])<12 or any(not math.isfinite(float(v)) or float(v)>=limit for _,v in row['values']) for row in rows):ok=False
  except Exception as exc:responses[name]={'error':str(exc)};ok=False
 nodes=call('GET','/api/v1/nodes',gaia=True);branches=[v for n in nodes for v in n['Control'].get('branches',[])];draining=sum(v['count'] for v in branches if v['runtime_state']=='DRAINING');ready=sum(v['count'] for v in branches if v['runtime_state']=='READY')
 record('r3-recovery-latest',{'time':time.time(),'resource_window_passed':ok,'draining':draining,'ready':ready,'metrics':responses,'nodes':nodes})
 if ok and draining==0 and ready<=1:break
 time.sleep(15)
record('r3-recovery-verified',{'time':time.time(),'resource_window_seconds':60,'all7_nodes_below70':True,'draining':draining,'ready':ready,'previous_stop':json.loads(stop.read_text())})
stop.unlink();event('resource_stop_cleared_after_verified_recovery',phase='crud-density-r3-preparation',previous_stop=expected_stop)
handed_off=False
try:
 for group in range(138,143):
  if stop.exists():raise RuntimeError('STOP during supplementary pool creation')
  subprocess.run([sys.executable,str(ROOT/'bench/online-tpcc/create-account.py'),str(group)],cwd=ROOT,check=True)
  for n in range((group-1)*8+1,group*8+1):
   subprocess.run([sys.executable,str(ROOT/'bench/online-tpcc/create.py'),str(n)],cwd=ROOT,check=True)
  record('r3-pool-creation-progress',{'time':time.time(),'completed_group':group,'last_instance':group*8})
 if stop.exists():raise RuntimeError('STOP after pool creation')
 stop.write_text(json.dumps({'time':time.time(),'reason':'r3 pool ready; supervisor must verify fresh resources before restart'}))
 subprocess.run([sys.executable,str(ROOT/'bench/online-tpcc/set-crud-admission.py')],cwd=ROOT,env=dict(os.environ,TPCC_ADMISSION_OPERATION_KEY='online-crud-r3-disable100'),check=True)
 f=(PRIVATE/'crud-r3-supervisor.log').open('ab',buffering=0)
 p=subprocess.Popen([sys.executable,str(ROOT/'bench/online-tpcc/run-crud-assessment.py'),'crud-density-r3'],cwd=ROOT,stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
 record('crud-r3-supervisor-process',{'pid':p.pid,'time':time.time()});handed_off=True
finally:
 if not handed_off:
  if not stop.exists():stop.write_text(json.dumps({'time':time.time(),'reason':'r3 preparation interrupted'}))
  subprocess.run([sys.executable,str(ROOT/'bench/online-tpcc/restore-admission.py')],cwd=ROOT,env=dict(os.environ,TPCC_ADMISSION_OPERATION_KEY='online-crud-r3-preparation-restore80'))
