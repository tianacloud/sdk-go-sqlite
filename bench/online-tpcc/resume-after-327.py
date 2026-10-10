import sys,time,json,subprocess,concurrent.futures
from common import *
while True:
 if (ROOT/'.work/STOP').exists():raise RuntimeError('resource STOP')
 events=[json.loads(s) for s in (EVIDENCE/'events.jsonl').read_text().splitlines()]
 if any(e['kind']=='density_stage_end' and e.get('phase')=='density-327-r1' for e in events):break
 time.sleep(5)
while True:
 if (ROOT/'.work/STOP').exists():raise RuntimeError('resource STOP')
 nodes=call('GET','/api/v1/nodes',gaia=True)
 if len(nodes)==2 and all(all(x.get('Control',{}).get('resource_admission',{}).get(k,{}).get('enabled') is False for k in ['cpu','memory']) for x in nodes):break
 time.sleep(5)
record('unlimited-admission-confirmed-before-resume',nodes)
missing=[n for n in range(130,402) if not (PRIVATE/f'fixture-{n:03d}.json').exists()]
for n in missing:
 p=EVIDENCE/f'sql-{n:03d}-load-new.jsonl'
 if p.exists():
  rows=[json.loads(s) for s in p.read_text().splitlines()]
  if any(r['sql_kind']!='SELECT' for r in rows):raise RuntimeError(f'partial mutation requires inspection: {n}')
event('fixture_retry_after_admission_change',instances=missing,reason='Previous attempts failed during initial read-only connection probe; no mutations replayed')
def load(n):
 return n,subprocess.run([sys.executable,str(ROOT/'bench/online-tpcc/workload.py'),'load',str(n),'0','200']).returncode
with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
 results=list(pool.map(load,missing))
record('fixture400-retry-results',results)
print('fixture retry results',results,flush=True)
