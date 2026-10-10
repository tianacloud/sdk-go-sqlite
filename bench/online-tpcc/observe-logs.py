import sys,json,time
from common import *
sys.path.insert(0,str(pathlib.Path.home()/'.codex/skills/tiana-debug/scripts'));from tiana_debug import Backend,clean_fields
b=Backend(PROFILE)
q='{environment="online"} |~ `level=(error|warn)|"level":"(error|warn)"|outcome="failed"|"outcome":"failed"|"level":"(ERROR|WARN)"`'
start=time.time()-600
while True:
 end=time.time()-10
 if end>start:
  try:
   params={'query':q,'start':int(start*1e9),'end':int(end*1e9),'limit':2000,'direction':'forward'};r=b.get('loki','/loki/api/v1/query_range',params)
   count=sum(len(s['values']) for s in r.get('data',{}).get('result',[]))
   with (EVIDENCE/'failure-logs-live.jsonl').open('a') as f:f.write(json.dumps(clean_fields({'queried_at':time.time(),'params':params,'count':count,'possibly_truncated':count>=2000,'response':r}))+'\n')
   if count>=2000:event('log_window_may_be_truncated',start=start,end=end)
  except Exception as e:event('log_observation_error',error=str(e))
  start=end
 time.sleep(30)
