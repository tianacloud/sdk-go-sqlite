import subprocess,sys,time,json,concurrent.futures,shutil,os
from common import *
SCRIPT=ROOT/'bench/online-tpcc'; STOP=ROOT/'.work/STOP'
tenant='ten-562592a8fd4d5ba70b676d1cbc139a1dfbf4'
def stopped():
 if STOP.exists():raise RuntimeError('STOP: '+STOP.read_text())
def command(args):
 stopped();r=subprocess.run([sys.executable,*map(str,args)],cwd=ROOT)
 if r.returncode:raise RuntimeError('child failed '+str(args)+' exit '+str(r.returncode))
def snapshot(label):
 registry=PRIVATE/'test-accounts.json'
 accounts=json.loads(registry.read_text()) if registry.exists() else [{'tenant_id':tenant}]
 items=[]
 for account in accounts:
  scope=account['tenant_id']
  response=call('GET',f'/api/v1/list-instances?tenant_id={scope}&page=1&page_size=100',gaia=True)
  items.extend(response['items'])
 record(label+'-placement',{'items':items,'tenant_ids':[a['tenant_id'] for a in accounts]})
 record(label+'-nodes',call('GET','/api/v1/nodes',gaia=True))
 return items
if __name__=='__main__':
 phase=sys.argv[1];new=[int(x) for x in sys.argv[2].split(',') if x];active=[int(x) for x in sys.argv[3].split(',')]
 if len(sys.argv)>4 and sys.argv[4]=='after-baseline':
  while True:
   stopped();events=[json.loads(s) for s in (EVIDENCE/'events.jsonl').read_text().splitlines()]
   done={e.get('phase') for e in events if e['kind']=='workload_complete'}
   if {'single-node33-r1','baseline-instance002'}<=done:break
   time.sleep(5)
 event('density_stage_prepare',phase=phase,new_instances=new,active_instances=active)
 for n in new:
  if (PRIVATE/f'instance-{n:03d}.json').exists():continue
  if n>8 and (n-1)%8==0:command([SCRIPT/'create-account.py',str((n-1)//8+1)])
  command([SCRIPT/'create.py',n])
 with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
  futures=[pool.submit(command,[SCRIPT/'workload.py','load',n,0,200]) for n in new if not (PRIVATE/f'fixture-{n:03d}.json').exists()]
  for future in concurrent.futures.as_completed(futures):
   try:future.result()
   except Exception:
    event('fixture_loader_failed',phase=phase)
    raise
 selected={json.loads((PRIVATE/f'instance-{n:03d}.json').read_text())['id'] for n in active}
 while (ROOT/'.work'/('PREACTIVATE-WAIT-'+phase)).exists():
  stopped();time.sleep(2)
 wait_start=time.time()
 while True:
  placement=snapshot(phase+'-before-'+str(int(time.time())))
  draining=[x['instance_id'] for x in placement if x['instance_id'] in selected and x['root']['runtime_state']=='DRAINING']
  if not draining:break
  event('phase_transition_wait',phase=phase,draining_instances=draining,elapsed=time.time()-wait_start)
  if time.time()-wait_start>180:raise RuntimeError('App stop transition did not settle within180s')
  stopped();time.sleep(5)
 if (ROOT/'.work/PREACTIVATE-DENSITY').exists():
  from workload import Client
  def preactivate(n):
   stopped();client=Client(n,phase+'-activation');success=False
   try:
    client.execute('SELECT 1');success=True
    return {'instance':n,'success':True}
   except Exception as exc:
    event('density_preactivation_failed',phase=phase,instance=n,error=str(exc))
    return {'instance':n,'success':False,'error':str(exc)}
   finally:client.finish(success)
  event('density_preactivation_start',phase=phase,instances=active,concurrency=8)
  with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
   activation_results=list(pool.map(preactivate,active))
  record(phase+'-preactivation-results',activation_results)
  failures=[x for x in activation_results if not x['success']]
  event('density_preactivation_complete',phase=phase,successful=len(active)-len(failures),failed=len(failures))
  if failures:raise RuntimeError('Preactivation failed for '+str(len(failures))+' instances; full outcomes preserved; no TPC window started')
 event('density_stage_start',phase=phase,instances=active)
 procs=[subprocess.Popen([sys.executable,str(SCRIPT/'workload.py'),'run',str(n),phase,'420','12'],cwd=ROOT) for n in active]
 started=time.time();last=0
 while any(p.poll() is None for p in procs):
  if time.time()-last>=30:
   snapshot(phase+'-'+str(int(time.time())));last=time.time()
  time.sleep(5)
 snapshot(phase+'-after');event('density_stage_end',phase=phase,exit_codes=[p.returncode for p in procs],elapsed=time.time()-started)
 print('stage ended',phase,[p.returncode for p in procs],flush=True)
