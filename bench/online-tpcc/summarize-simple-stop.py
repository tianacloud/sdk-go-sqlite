"""Summarize the resource stop separately from completed measurement windows."""
import collections,math
from common import *
phase='simple-density-r1'
stop=json.loads((ROOT/'.work/STOP').read_text())
progress=json.loads((EVIDENCE/(phase+'-progress.json')).read_text())
ids={x['instance']:x['instance_id'] for x in json.loads((EVIDENCE/'test-instance-workload-history.json').read_text())['instances']}
placement=json.loads((EVIDENCE/(phase+'-density'+str(len(progress['active_instances']))+'-before-placement.json')).read_text())['items']
nodes={r['instance_id']:r['root']['node_id'] for r in placement}
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
 rows.append({'instance':n,'node':nodes[ids[n]],'hot_sql_requests_preceding30s':len(selected),'hot_sql_qps_preceding30s':len(selected)/30,'errors_preceding30s':sum(bool(v.get('error')) for v in selected),'p90_ms_preceding30s':latencies[math.ceil(len(latencies)*.9)-1] if latencies else None,'source_covers_window_start':bool(values and values[0]['time']<=start),'last_request_start':values[-1]['time'],'last_completion':max(v['time']+v['elapsed_ms']/1000 for v in values),'requests_started_after_stop':sum(v['time']>end for v in values)})
summary={}
for node in sorted({r['node'] for r in rows}):
 selected=[r for r in rows if r['node']==node]
 summary[node]={'started_workers':len(selected),'workers_with_hot_sql_preceding30s':sum(r['hot_sql_requests_preceding30s']>0 for r in selected),'minimum_hot_sql_qps_preceding30s':min(r['hot_sql_qps_preceding30s'] for r in selected),'maximum_instance_p90_ms_preceding30s':max(r['p90_ms_preceding30s'] for r in selected if r['p90_ms_preceding30s'] is not None),'errors_preceding30s':sum(r['errors_preceding30s'] for r in selected),'last_logged_sql_completion':max(r['last_completion'] for r in selected),'logged_requests_started_after_stop':sum(r['requests_started_after_stop'] for r in selected)}
record('simple-stop-summary',{'phase':phase,'stop':stop,'preceding_window_start':start,'preceding_window_end':end,'node_summaries':summary,'workers':rows,'highest_completed_scheduled_window':progress['windows'][-1],'note':'Preceding30s crosses warmup/measurement boundary for376 stage; it is not a completed scheduled60s window. Log completion times cover recorded responses, not unlogged transport failures; inspect interruption events separately.'})
print(json.dumps(summary,indent=2))
