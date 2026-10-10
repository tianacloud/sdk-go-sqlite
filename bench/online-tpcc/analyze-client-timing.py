"""Separate recorded SDK-call time from local client overhead in a fixed interval."""
import math,statistics,sys
from common import *
start,end=map(float,sys.argv[1:3]);label=sys.argv[3];phase='simple-density-r1'
progress=json.loads((EVIDENCE/(phase+'-progress.json')).read_text());rows=[];pooled={'sdk_ms':[],'local_bridge_extra_ms':[],'unaccounted_between_requests_ms':[]}
def summarize(values):
 values=sorted(values)
 return {'samples':len(values),'mean':statistics.mean(values),'p90':values[max(0,math.ceil(len(values)*.9)-1)],'max':values[-1]} if values else None
for n in progress['active_instances']:
 path=EVIDENCE/f'sql-{n:03d}-{phase}.jsonl';size=path.stat().st_size;length=min(size,512*1024)
 while True:
  offset=max(0,size-length)
  with path.open('rb') as stream:
   stream.seek(offset)
   if offset:stream.readline()
   values=[json.loads(line) for line in stream.read(size-stream.tell()).splitlines()]
  if not offset or (values and values[0]['time']<=start):break
  length=min(size,length*2)
 selected=[v for v in values if start<=v['time']<end and not v.get('cold_connection')]
 if not selected:continue
 sdk=[v['sdk_ms'] for v in selected if v.get('sdk_ms') is not None]
 extra=[v['elapsed_ms']-v['sdk_ms'] for v in selected if v.get('sdk_ms') is not None]
 gaps=[(b['time']-a['time'])*1000-a['elapsed_ms']-b.get('rate_wait_ms',0) for a,b in zip(selected,selected[1:])]
 data={'sdk_ms':sdk,'local_bridge_extra_ms':extra,'unaccounted_between_requests_ms':gaps}
 for k,v in data.items():pooled[k].extend(v)
 rows.append({'instance':n,'qps':len(selected)/(end-start),**{k:summarize(v) for k,v in data.items()}})
record('client-timing-'+label,{'time':time.time(),'start':start,'end':end,'instances':rows,'pooled':{k:summarize(v) for k,v in pooled.items()},'note':'SDK duration includes SDK, network and server time; not server-only execution. Extra time includes local HTTP bridge/serialization/scheduling. Gap subtracts requested rate sleep and recorded elapsed time, so includes logging, sorting, oversleep and scheduling; not a direct profiler attribution.'})
print(label,'instances',len(rows),{k:summarize(v) for k,v in pooled.items()})
