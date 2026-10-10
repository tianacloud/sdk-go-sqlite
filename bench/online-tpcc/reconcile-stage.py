import sys,json,collections
from common import *
phase=sys.argv[1];phases=phase.split(',');nums=[int(n) for n in sys.argv[2].split(',')];include_preparation='--include-simple-preparation' in sys.argv[3:]
clients={};begin=None;end=None
for n in nums:
 rows=[]
 for source_phase in (['simple-sql-prepare'] if include_preparation else [])+phases:
  path=EVIDENCE/f'sql-{n:03d}-{source_phase}.jsonl'
  if path.exists():rows.extend(json.loads(line) for line in path.read_text().splitlines())
 rows.sort(key=lambda row:row['time'])
 if not rows:raise RuntimeError('No logged SQL for requested instance '+str(n))
 iid=rows[0]['instance_id'];start=rows[0]['time'];finish=max(r['time']+r['elapsed_ms']/1000 for r in rows)
 clients[iid]={'instance':n,'first_request_time':start,'last_completion_time':finish,'client_sql_requests':len(rows),'client_error_count':sum(bool(r.get('error')) for r in rows)}
 begin=min(begin or start,start);end=max(end or finish,finish)
pre={};post={};identities={};last_query=0;last_values={};decreases=[]
with (EVIDENCE/'sql-metrics-live.jsonl').open() as f:
 for line in f:
  x=json.loads(line);t=x['time']
  if t>end+60:continue
  last_query=max(last_query,t)
  for r in x.get('response',{}).get('data',{}).get('result',[]):
   m=r['metric'];iid=m.get('instance_id')
   if iid not in clients or m['__name__']!='tiana_app_sql_statements_total':continue
   key=(iid,m.get('source_id'),m.get('operation'),m.get('result'))
   identities[key]=m;value=float(r['value'][1])
   if t<clients[iid]['first_request_time']:pre[key]=max(pre.get(key,0),value)
   else:
    if key in last_values and value<last_values[key]:decreases.append({'time':t,'labels':m,'before':last_values[key],'after':value})
    last_values[key]=value;post[key]=max(post.get(key,0),value)
server=collections.Counter();details=[]
for key,value in post.items():
 delta=max(0,value-pre.get(key,0));server[key[0]]+=delta
 if delta:details.append({'labels':identities[key],'before_max':pre.get(key,0),'after_max':value,'delta':delta})
for iid,v in clients.items():
 v['server_statement_delta']=server[iid];v['server_minus_client']=server[iid]-v['client_sql_requests']
out={'phase':phase,'included_phases':phases,'included_simple_preparation':include_preparation,'observed_post_start_counter_decreases':decreases,'latest_query_used':last_query,'requested_observation_end':end+60,'complete_post_window':last_query>=end+45,'instances':clients,'series':details,'method':'Per producer source_id/operation/result cumulative maxima before and after phase; final observation up to60s after last client completion. No new workload may run on these instances during that post window. Source counter decreases invalidate a simple cumulative-max reconciliation and are reported separately. Shared Prometheus replica identity remains UNKNOWN; nonzero differences require investigation, not assumed packet loss.'}
record('telemetry-reconciliation-'+phase,out);print(json.dumps({k:v for k,v in out.items() if k!='series'},ensure_ascii=False,indent=2))
