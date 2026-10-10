"""Summarize the resource stop separately from completed measurement windows."""
import collections,math,sys
from common import *
phase=sys.argv[1]
stop=json.loads((ROOT/'.work/STOP').read_text())
progress=json.loads((EVIDENCE/(phase+'-progress.json')).read_text())
nodes_by_instance={}
for line in (EVIDENCE/'events.jsonl').read_text().splitlines():
 event_row=json.loads(line)
 if event_row.get('phase')==phase and event_row['kind']=='simple_prepared_placement':
  nodes_by_instance[event_row['instance']]=event_row['node']
end=stop['time'];start=end-30;rows=[]
for n in progress['active_instances']:
 path=EVIDENCE/f'sql-{n:03d}-{phase}.jsonl';size=path.stat().st_size;length=min(size,512*1024)
 while True:
  offset=max(0,size-length)
  with path.open('rb') as stream:
   stream.seek(offset)
   if offset:stream.readline()
   lines=stream.read(size-stream.tell()).splitlines()
  values=[json.loads(line) for line in lines]
  if not offset or (values and values[0]['time']<=start):break
  length=min(size,length*2)
 selected=[v for v in values if start<=v['time']<end and not v.get('cold_connection')]
 latencies=sorted(v['elapsed_ms'] for v in selected)
 rows.append({'instance':n,'node':nodes_by_instance[n],'hot_sql_requests_preceding30s':len(selected),'hot_sql_qps_preceding30s':len(selected)/30,'errors_preceding30s':sum(bool(v.get('error')) for v in selected),'p90_ms_preceding30s':latencies[math.ceil(len(latencies)*.9)-1] if latencies else None,'source_covers_window_start':bool(values and values[0]['time']<=start),'last_request_start':values[-1]['time'],'last_completion':max(v['time']+v['elapsed_ms']/1000 for v in values),'requests_started_after_stop':sum(v['time']>end for v in values)})
summary={}
for node in sorted({r['node'] for r in rows}):
 selected=[r for r in rows if r['node']==node]
 summary[node]={'started_workers':len(selected),'workers_with_hot_sql_preceding30s':sum(r['hot_sql_requests_preceding30s']>0 for r in selected),'minimum_hot_sql_qps_preceding30s':min(r['hot_sql_qps_preceding30s'] for r in selected),'maximum_instance_p90_ms_preceding30s':max(r['p90_ms_preceding30s'] for r in selected if r['p90_ms_preceding30s'] is not None),'errors_preceding30s':sum(r['errors_preceding30s'] for r in selected),'last_logged_sql_completion':max(r['last_completion'] for r in selected),'logged_requests_started_after_stop':sum(r['requests_started_after_stop'] for r in selected)}
record('crud-stop-summary',{'phase':phase,'stop':stop,'preceding_window_start':start,'preceding_window_end':end,'node_summaries':summary,'workers':rows,'highest_completed_scheduled_window':progress['windows'][-1] if progress['windows'] else None,'note':'Preceding30s is a stop-aligned observational window and may overlap ramp or warmup; it is not a completed scheduled measurement. Node placement comes from the recorded Gaia preparation placement for each worker. Log completion times cover recorded responses, not unlogged transport failures; inspect interruption events separately.'})
print(json.dumps(summary,indent=2))

# Preserve the original observer responses used around STOP, not historical requery values.
samples={}
with (EVIDENCE/'metrics-live.jsonl').open() as source:
 for line in source:
  if not line.endswith('\n'):break
  if not any('"name": "'+name+'"' in line[:150] for name in ['cpu','cpu_instant','memory']):continue
  response=json.loads(line)
  if response['time']<=stop['time']:
   prior=samples.get(response['name'])
   if prior is None or response['time']>prior['time']:samples[response['name']]=response
record('crud-stop-original-observer-samples',{'phase':phase,'stop':stop,'samples':samples})
