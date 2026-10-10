"""Bounded batches of normal connection closure before the next density phase."""
import time,json,sys
from workload import *
nums=[int(x) for x in sys.argv[1].split(',')];batch_size=16
for offset in range(0,len(nums),batch_size):
 if STOP.exists():raise RuntimeError('resource STOP')
 batch=nums[offset:offset+batch_size];configs={n:json.loads((PRIVATE/f'instance-{n:03d}.json').read_text()) for n in batch}
 event('import_memory_recycle_batch_start',instances=batch)
 for n in batch:close_retained(n)
 deadline=time.time()+240
 while True:
  if STOP.exists():raise RuntimeError('resource STOP')
  rows=[]
  for tenant in sorted({c['tenant_id'] for c in configs.values()}):
   r=call('GET',f'/api/v1/list-instances?tenant_id={tenant}&page=1&page_size=100',gaia=True);rows.extend(r['items'])
  wanted={c['id'] for c in configs.values()};states=[x for x in rows if x['instance_id'] in wanted]
  record('recycle-instances-'+str(batch[0])+'-'+str(int(time.time())),{'time':time.time(),'instances':batch,'states':states})
  if len(states)==len(batch) and all(x['root']['runtime_state']=='SLEEPING' for x in states):break
  if time.time()>deadline:raise RuntimeError('batch did not reach SLEEPING; inspect saved state')
  time.sleep(5)
 event('import_memory_recycle_batch_complete',instances=batch)
 print('recycled',offset+len(batch),'/',len(nums),flush=True)
event('import_memory_recycle_complete',instances=nums)
