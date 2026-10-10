"""Correlate identity-poor Agent timeout logs with preceding FS log pod identities."""
import re,collections
from common import *
stop=json.loads((ROOT/'.work/STOP').read_text())['time'];owners={r['instance_id']:r for r in json.loads((EVIDENCE/'owned-instance-workload-history.json').read_text())['instances']};latest={}
for line in (EVIDENCE/'fs-stats-current-logs-live.jsonl').open():
 query=json.loads(line)
 for stream in query['response'].get('data',{}).get('result',[]):
  labels=stream['stream'];uid=labels.get('k8s_pod_uid')
  for stamp,message in stream['values']:
   if int(stamp)>stop*1e9:continue
   match=re.search(r'instance_id="([^"]+)"',message)
   if not uid or not match or match.group(1) not in owners:continue
   if uid not in latest or int(stamp)>int(latest[uid]['time_ns']):latest[uid]={'time_ns':stamp,'instance_id':match.group(1),'pod':labels.get('k8s_pod_name'),'node':labels.get('k8s_node_name')}
x=json.loads((EVIDENCE/'crud-stop-agent-timeout-expanded-logs.json').read_text());events=[];seen=set()
for stream in x['response']['data']['result']:
 labels=stream['stream'];uid=labels.get('k8s_pod_uid')
 for stamp,message in stream['values']:
  if 'APP_STOP_TIMEOUT' not in message:continue
  key=(uid,stamp,message)
  if key in seen:continue
  seen.add(key);mapping=latest.get(uid);owner=owners.get(mapping['instance_id']) if mapping else None
  events.append({'time_ns':stamp,'pod_uid':uid,'pod':labels.get('k8s_pod_name'),'node':labels.get('k8s_node_name'),'preceding_fs_identity':mapping,'instance':owner['instance'] if owner else None,'completed_tpcc_load_history':bool(owner and owner.get('tpcc_fixture_complete')),'log_has_instance_id':'instance_id' in message,'log_has_cluster':'cluster' in message})
out={'time':time.time(),'raw_source':'crud-stop-agent-timeout-expanded-logs.json','possibly_truncated':x['possibly_truncated'],'distinct_timeout_records':len(events),'matched_to_owned_instance':sum(r['instance'] is not None for r in events),'matched_with_tpcc_load_history':sum(r['completed_tpcc_load_history'] for r in events),'per_node_records':dict(collections.Counter(r['node'] for r in events)),'events':events,'interpretation':'Associate by Kubernetes pod UID and last FS statistics identity before resource stop. This is historical association, not proof that TPC-C data causes the timeout. Timeout log body lacks cluster/instance identity when flags are false; filtering by those body fields misses these records.'};record('crud-stop-timeout-correlation',out);print({k:v for k,v in out.items() if k!='events'})
