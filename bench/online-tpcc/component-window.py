import sys,json,time
from common import *
sys.path.insert(0,str(pathlib.Path.home()/'.codex/skills/tiana-debug/scripts'));from tiana_debug import Backend
phase=sys.argv[1];start,end=map(float,sys.argv[2:4]);b=Backend(PROFILE)
selector='pod=~"(tiana-)?(control|runtime|gateway|mgr|directory|gaia).*",container!="",container!="POD"'
queries={'cpu_cores':'sum by (namespace,pod,container) (rate(container_cpu_usage_seconds_total{'+selector+'}[1m]))','cpu_throttled_seconds_per_second':'sum by (namespace,pod,container) (rate(container_cpu_cfs_throttled_seconds_total{'+selector+'}[1m]))','memory_working_set_bytes':'sum by (namespace,pod,container) (container_memory_working_set_bytes{'+selector+'})','restart_count':'kube_pod_container_status_restarts_total{pod=~"(tiana-)?(control|runtime|gateway|mgr|directory|gaia).*"}'}
summary={}
for name,q in queries.items():
 params={'query':q,'start':start,'end':end,'step':15};r=b.get('prometheus','/api/v1/query_range',params);record('components-'+phase+'-'+name,{'params':params,'response':r,'replica_identity':'UNKNOWN'})
 rows=[]
 for x in r.get('data',{}).get('result',[]):
  values=[float(v) for _,v in x['values'] if v not in ['NaN','+Inf','-Inf']]
  if values:rows.append({'labels':x['metric'],'samples':len(values),'min':min(values),'max':max(values),'mean':sum(values)/len(values)})
 summary[name]=rows
record('components-'+phase+'-summary',summary);print('component signals',phase,{k:len(v) for k,v in summary.items()})
