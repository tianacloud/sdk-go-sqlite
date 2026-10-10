import time,json,sys,concurrent.futures
from common import *
from importlib.machinery import SourceFileLoader
stage=SourceFileLoader('density_stage',str(ROOT/'bench/online-tpcc/density-stage.py')).load_module()
sys.path.insert(0,str(pathlib.Path.home()/'.codex/skills/tiana-debug/scripts'))
from tiana_debug import Backend
b=Backend(PROFILE);above=set();pool=concurrent.futures.ThreadPoolExecutor(max_workers=1)
queries={'cpu':'100*(1-avg by(instance,node_id)(irate(node_cpu_seconds_total{mode="idle"}[1m])))','memory':'100*(1-node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes)'}
def capture(label,trigger):
 record(label+'-trigger',trigger)
 complete=sorted(set(range(2,130))|{int(p.stem.split('-')[-1]) for p in PRIVATE.glob('fixture-*.json')})
 retained=sorted(int(p.stem.split('-')[-1]) for p in PRIVATE.glob('retained-session-*.json'))
 record(label+'-client-state',{'time':time.time(),'completed_fixture_instance_numbers':complete,'retained_session_instance_numbers':retained,'note':'Retained sessions include idle loaded instances; not equivalent to active TPC-C. Correlate workload_start/workload_complete events and SQL timestamps.'})
 try:stage.snapshot(label)
 except Exception as e:event('watermark_snapshot_error',label=label,error=str(e))
while True:
 for name,q in queries.items():
  try:
   r=b.get('prometheus','/api/v1/query',{'query':q})
   for x in r.get('data',{}).get('result',[]):
    k=(name,x['metric'].get('node_id',x['metric'].get('instance')));v=float(x['value'][1])
    if v>=85 and k not in above:
     trigger={'time':time.time(),'metric':name,'threshold':85,'sample':x,'stop_threshold':95};event('resource_85_watermark',**trigger);pool.submit(capture,'watermark85-'+name+'-'+str(k[1])+'-'+str(int(time.time())),trigger);above.add(k)
    elif v<83:above.discard(k)
  except Exception as e:event('watermark_observation_error',error=str(e))
 time.sleep(5)
