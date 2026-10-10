import sys,time,json
from common import *
sys.path.insert(0,str(pathlib.Path.home()/'.codex/skills/tiana-debug/scripts'));from tiana_debug import Backend
b=Backend(PROFILE)
q='{__name__=~"tiana_(app_.*|gateway_.*duration.*|control_.*duration.*|mgr_.*duration.*|fs_.*)"}'
q+=' or sum by (__name__,node_id,stage,result,le) ({__name__=~"tiana_agent_startup_stage_duration_seconds.*"})'
record('sql-observation-query',{'query':q,'interval_seconds':15,'replica_identity':'UNKNOWN'})
while True:
 try:
  t=time.time();r=b.get('prometheus','/api/v1/query',{'query':q})
  with (EVIDENCE/'sql-metrics-live.jsonl').open('a') as f:f.write(json.dumps({'time':t,'query':q,'replica_identity':'UNKNOWN','response':r})+'\n')
 except Exception as e:event('sql_observation_error',error=str(e))
 time.sleep(15)
