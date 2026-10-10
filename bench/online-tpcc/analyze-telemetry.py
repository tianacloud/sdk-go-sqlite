import json,sys,math,collections
from common import *
start,end=map(float,sys.argv[1:3]);phase=sys.argv[3];series={};query_count=0
with (EVIDENCE/'metrics-live.jsonl').open() as f:
 for line in f:
  if '"name": "otel"' not in line[:150]:continue
  x=json.loads(line)
  if x['name']!='otel' or not start<=x['time']<=end:continue
  query_count+=1
  for r in x.get('response',{}).get('data',{}).get('result',[]):
   m=r['metric'];name=m['__name__'];k=json.dumps(m,sort_keys=True);value=float(r['value'][1])
   if not math.isfinite(value):continue
   if k not in series:series[k]={'labels':m,'min':value,'max':value,'first':value,'last':value,'samples':0,'observed_decreases':0}
   s=series[k];s['observed_decreases']+=int(value<s['last']);s['last']=value;s['min']=min(s['min'],value);s['max']=max(s['max'],value);s['samples']+=1
summary=collections.defaultdict(lambda:{'series':0,'max_observed_value':0,'sum_of_max_minus_min':0,'decreases':0});nonzero=[]
for s in series.values():
 name=s['labels']['__name__'];v=summary[name];v['series']+=1;v['max_observed_value']=max(v['max_observed_value'],s['max']);v['sum_of_max_minus_min']+=s['max']-s['min'];v['decreases']+=s['observed_decreases']
 if any(w in name for w in ['failed','refused']) and s['max']>0:nonzero.append(s)
out={'phase':phase,'start':start,'end':end,'query_count':query_count,'signals':dict(summary),'nonzero_failure_or_refusal_series':nonzero,'series':list(series.values()),'interpretation':'Observed per-series extrema, not exact boundary deltas. Counter decreases may reflect reset or UNKNOWN Prometheus replica variation. Absent metric names are missing evidence, not zero failures.'}
record('telemetry-health-'+phase,out);print(json.dumps({k:v for k,v in out.items() if k!='series'},ensure_ascii=False,indent=2))
