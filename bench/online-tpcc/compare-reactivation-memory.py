import sys,time,json,subprocess,concurrent.futures
from workload import *
sys.path.insert(0,str(pathlib.Path.home()/'.codex/skills/tiana-debug/scripts'))
from tiana_debug import Backend
b=Backend(PROFILE);nums=[40,50,150,160]
def check():
 if STOP.exists():raise RuntimeError('resource STOP during comparison')
def rss(label):
 q='container_memory_rss{pod=~"tiana-agent-.*",container!="",container!="POD"}'
 record('reactivation-memory-'+label,{'time':time.time(),'response':b.get('prometheus','/api/v1/query',{'query':q})})
while True:
 check();events=[json.loads(s) for s in (EVIDENCE/'events.jsonl').read_text().splitlines()]
 if any(e['kind']=='density_stage_end' and e.get('phase')=='density-327-r1' for e in events):break
 time.sleep(3)
rss('before');event('reactivation_memory_comparison_start',instances=nums)
for n in nums:close_retained(n)
configs={n:json.loads((PRIVATE/f'instance-{n:03d}.json').read_text()) for n in nums}
deadline=time.time()+240
while True:
 check();states=[]
 for n,c in configs.items():
  r=call('GET',f"/api/v1/list-instances?tenant_id={c['tenant_id']}&page=1&page_size=100",gaia=True)
  row=next(x for x in r['items'] if x['instance_id']==c['id']);states.append({'number':n,'root':row['root']})
 record('reactivation-memory-states-'+str(int(time.time())),states)
 if all(x['root']['runtime_state'] not in ['READY','DRAINING','STARTING'] for x in states):break
 if time.time()>deadline:raise RuntimeError('App stop did not settle; inspect states before reactivation')
 time.sleep(5)
rss('stopped')
procs=[subprocess.Popen([sys.executable,str(ROOT/'bench/online-tpcc/workload.py'),'run',str(n),'memory-reactivation-r1','120','12']) for n in nums]
results=[p.wait() for p in procs];rss('after120');event('reactivation_memory_comparison_end',instances=nums,exit_codes=results)
if any(results):raise RuntimeError('comparison workload failed')
(ROOT/'.work/PREP-PAUSE').unlink(missing_ok=True)
print('memory comparison complete; preparation resumed',flush=True)
