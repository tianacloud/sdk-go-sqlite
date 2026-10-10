import sys,json,collections,statistics
from common import *
start,end=map(float,sys.argv[1:3]);phase=sys.argv[3]
values=collections.defaultdict(list);alerts=[];missing=[]
with (EVIDENCE/'metrics-live.jsonl').open() as f:
 for line in f:
  x=json.loads(line)
  if not start<=x['time']<=end:continue
  name=x['name']
  if name in ['cpu','cpu_instant','memory']:
   for row in x.get('response',{}).get('data',{}).get('result',[]):
    label=row['metric'];values[(name,label.get('node_id') or label.get('instance'))].append(float(row['value'][1]))
  if name=='alerts':
   for row in x.get('response',{}).get('data',{}).get('result',[]):
    if row['metric'].get('alertstate')=='firing':alerts.append({'time':x['time'],**row})
summary={'phase':phase,'start':start,'end':end,'node_resources':[{'metric':k[0],'node':k[1],'samples':len(v),'mean':statistics.mean(v),'max':max(v),'min':min(v)} for k,v in sorted(values.items())],'firing_alert_samples':alerts,'replica_identity':'UNKNOWN'}
record('resources-'+phase,summary);print(json.dumps(summary,ensure_ascii=False,indent=2))
