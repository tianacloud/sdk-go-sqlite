"""Extend the quiet post-test observation to120s, reusing immutable client counts."""
import collections
from common import *
source='telemetry-reconciliation-simple-density-r1.json'
prior=json.loads((EVIDENCE/source).read_text());clients=prior['instances'];end=max(v['last_completion_time'] for v in clients.values())
pre={};post={};identities={};last_values={};decreases=[];latest=0
for line in (EVIDENCE/'sql-metrics-live.jsonl').open():
 x=json.loads(line);t=x['time']
 if t>end+120:break
 if x.get('response',{}).get('status')!='success':continue
 latest=max(latest,t)
 for r in x['response'].get('data',{}).get('result',[]):
  m=r['metric'];iid=m.get('instance_id')
  if iid not in clients or m.get('__name__')!='tiana_app_sql_statements_total':continue
  key=(iid,m.get('source_id'),m.get('operation'),m.get('result'));v=float(r['value'][1]);identities[key]=m
  if t<clients[iid]['first_request_time']:pre[key]=max(pre.get(key,0),v)
  else:
   if key in last_values and v<last_values[key]:decreases.append({'time':t,'labels':m,'before':last_values[key],'after':v})
   last_values[key]=v;post[key]=max(post.get(key,0),v)
server=collections.Counter();details=[]
for key,value in post.items():
 delta=max(0,value-pre.get(key,0));server[key[0]]+=delta
 if delta:details.append({'labels':identities[key],'before_max':pre.get(key,0),'after_max':value,'delta':delta})
for iid,v in clients.items():v.update(server_statement_delta=server[iid],server_minus_client=server[iid]-v['client_sql_requests'])
record('telemetry-reconciliation-simple-density-r1-120s',{'time':time.time(),'phase':'simple-density-r1','client_count_source':source,'included_simple_preparation':True,'latest_query_used':latest,'last_client_completion':end,'requested_observation_end':end+120,'minimum_required_post_seconds':60,'complete_post_window':latest>=end+60,'instances':clients,'series':details,'observed_post_start_counter_decreases':decreases,'method':'Use unchanged ended-phase client counts including preparation. Recompute every producer cumulative maximum with a successful metric query at least60s after final SQL, within120s. Original60s range contained no query late enough for its45s check; preserve that earlier result. No workload restarted. Replica identity UNKNOWN; differences and source decreases require investigation.'})
print('post seconds',latest-end,'complete',latest>=end+60,'instances',len(clients),'matched',sum(v['server_minus_client']==0 for v in clients.values()),'decreases',len(decreases))
