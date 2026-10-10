import json,csv,math,collections
from common import *
analyses={}
for p in sorted(EVIDENCE.glob('phase-analysis-*.json')):
 for a in json.loads(p.read_text()):
  if a['complete']:analyses[(a['phase'],a['instance'])]=a
phases=collections.defaultdict(list)
for (phase,n),a in analyses.items():phases[phase].append(a)
rows=[]
for phase,items in phases.items():
 placement=EVIDENCE/(phase+'-after-placement.json')
 if phase in ['single-node33-r1','baseline-instance002']:placement=EVIDENCE/'baseline-placement-late.json'
 if not placement.exists():continue
 locations={x['instance_id']:x['root']['node_id'] for x in json.loads(placement.read_text())['items']}
 nodes=collections.defaultdict(list)
 for a in items:
  p=EVIDENCE/f"sql-{a['instance']:03d}-{phase}.jsonl"
  with p.open() as f:iid=json.loads(next(f))['instance_id']
  nodes[locations.get(iid,'UNKNOWN')].append(a)
 for node,instances in nodes.items():
  lat=[]
  for a in instances:
   with (EVIDENCE/f"sql-{a['instance']:03d}-{phase}.jsonl").open() as f:
    for line in f:
     r=json.loads(line)
     if a['measurement_start']<=r['time']<a['measurement_end'] and not r.get('cold_connection'):lat.append(r['elapsed_ms'])
  lat.sort()
  rows.append({'phase':phase,'node':node,'test_instances':len(instances),'instance_numbers':','.join(str(a['instance']) for a in instances),'min_instance_all_sql_qps':min(a['all_sql_qps'] for a in instances),'sum_all_sql_qps':sum(a['all_sql_qps'] for a in instances),'min_instance_business_qps':min(a['business_sql_qps'] for a in instances),'sum_business_qps':sum(a['business_sql_qps'] for a in instances),'sql_p90_ms':lat[math.ceil(len(lat)*.9)-1],'sql_p99_ms':lat[math.ceil(len(lat)*.99)-1],'sql_max_ms':max(lat),'max_instance_30s_p90_ms':max(a['max_complete_30s_p90_ms'] for a in instances),'sql_errors':sum(a['errors'] for a in instances),'cold_connection_max_ms':max(v for a in instances for v in a['cold_connections']),'measurement_start':min(a['measurement_start'] for a in instances),'measurement_end':max(a['measurement_end'] for a in instances)})
record('capacity-table',rows)
if rows:
 with (EVIDENCE/'capacity-table.csv').open('w') as f:
  w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
print('capacity table rows',len(rows))
