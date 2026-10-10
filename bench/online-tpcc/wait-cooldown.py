"""Record recovery; leave STOP in place for review before resuming workload."""
import sys,time,math
from common import *
sys.path.insert(0,str(pathlib.Path.home()/'.codex/skills/tiana-debug/scripts'))
from tiana_debug import Backend
b=Backend(PROFILE);stable_since=None
queries={'cpu':'100*(1-avg by(node_id)(irate(node_cpu_seconds_total{mode="idle"}[1m])))','cpu_rate':'100*(1-avg by(node_id)(rate(node_cpu_seconds_total{mode="idle"}[1m])))','memory':'100*(1-node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes)','age':'time()-timestamp(node_memory_MemAvailable_bytes)'}
while True:
 values={};ok=True
 for name,q in queries.items():
  try:
   r=b.get('prometheus','/api/v1/query',{'query':q});values[name]=r
   rows=r['data']['result'];threshold=60 if name=='age' else 70
   if len(rows)<7 or any(not math.isfinite(float(x['value'][1])) or float(x['value'][1])>=threshold for x in rows):ok=False
  except Exception as exc:values[name]={'error':str(exc)};ok=False
 now=time.time()
 stable_since=(stable_since or now) if ok else None
 with (EVIDENCE/'density400-cooldown-live.jsonl').open('a') as f:f.write(json.dumps({'time':now,'stable_since':stable_since,'ready':bool(stable_since and now-stable_since>=60),'values':values})+'\n')
 print('cooldown',int(now),'stable_seconds',int(now-stable_since) if stable_since else 0,flush=True)
 if stable_since and now-stable_since>=60:
  record('density400-cooldown-ready',{'time':now,'stable_since':stable_since,'values':values,'stop_cleared':False});break
 time.sleep(15)
