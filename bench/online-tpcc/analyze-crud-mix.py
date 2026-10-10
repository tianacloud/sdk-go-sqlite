"""Verify actual CRUD operation proportions and affected rows in completed windows."""
import bisect,collections,sys
from common import *
phase=sys.argv[1]
progress=json.loads((EVIDENCE/(phase+'-progress.json')).read_text())
windows=progress['windows'];results=[]
for window in windows:
 results.append({'start':window['start'],'end':window['end'],'node_counts':window['node_counts'],'counts':collections.Counter(),'mutation_affected_rows':collections.Counter(),'per_instance':{}})
for n in sorted({n for w in windows for n in w['instances']}):
 eligible=[(i,w) for i,w in enumerate(windows) if n in w['instances']]
 starts=[w['start'] for _,w in eligible]
 with (EVIDENCE/f'sql-{n:03d}-{phase}.jsonl').open() as f:
  for line in f:
   r=json.loads(line)
   if not r.get('txn','').startswith('CRUD_'):continue
   position=bisect.bisect_right(starts,r['time'])-1
   if position<0:continue
   i,w=eligible[position]
   if r['time']>=w['end']:continue
   out=results[i];kind=r['sql_kind'];out['counts'][kind]+=1
   per=out['per_instance'].setdefault(n,collections.Counter());per[kind]+=1
   if kind!='SELECT':out['mutation_affected_rows'][str(r.get('affected'))]+=1
for out in results:
 total=sum(out['counts'].values());out['read_percent']=100*out['counts']['SELECT']/total if total else None
 out['write_percent']=100-out['read_percent'] if total else None
 out['all_recorded_mutations_affected_one_row']=bool(out['mutation_affected_rows']) and set(out['mutation_affected_rows'])=={'1'}
record(phase+'-operation-mix',{'phase':phase,'target_read_write_ratio':'50:50','target_operation_ratio':'3:1:1:1','windows':results})
print('CRUD operation mix audited',len(results),'windows')
