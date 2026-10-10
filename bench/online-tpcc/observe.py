import sys,json,time,math,threading
from common import *
sys.path.insert(0,str(pathlib.Path.home()/'.codex/skills/tiana-debug/scripts'))
from tiana_debug import Backend
STOP=ROOT/'.work/STOP'
b=Backend(PROFILE)
queries={
 'cpu':'100 * (1 - avg by (instance, node_id) (rate(node_cpu_seconds_total{mode="idle"}[1m])))',
 'cpu_instant':'100 * (1 - avg by (instance, node_id) (irate(node_cpu_seconds_total{mode="idle"}[1m])))',
 'memory':'100 * (1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)',
 'sample_age':'time() - timestamp(node_memory_MemAvailable_bytes)',
 'up':'up',
 'resources':'{__name__=~"node_load1|node_memory_MemTotal_bytes|node_memory_MemAvailable_bytes|node_disk_io_time_seconds_total|node_disk_read_bytes_total|node_disk_written_bytes_total|node_network_receive_bytes_total|node_network_transmit_bytes_total|node_cpu_seconds_total",device!="lo"}',
 'processes':'sum by (__name__,namespace,pod,container,node_id,service_name,component) ({__name__=~"container_cpu_usage_seconds_total|container_memory_working_set_bytes|container_cpu_cfs_throttled_seconds_total|kube_pod_container_status_restarts_total|kube_pod_container_status_waiting_reason",container!="",container!="POD"})',
 'tiana':'{__name__=~"tiana_(control|mgr|gateway|runtime|directory|gaia).*",__name__!~".*(bucket|config.*|revision.*)"}',
 'otel':'{__name__=~"otelcol_(exporter_queue.*|exporter_send_failed.*|exporter_sent.*|exporter_enqueue_failed.*|receiver_accepted.*|receiver_refused.*)"}',
 'alerts':'ALERTS',
}
record('observation-queries',{'queries':queries,'fast_interval_seconds':5,'detail_interval_seconds':15,'replica_identity':'UNKNOWN'})
def query(name,q):
 try:
  start=time.time();r=b.get('prometheus','/api/v1/query',{'query':q})
  with (EVIDENCE/'metrics-live.jsonl').open('a') as f:f.write(json.dumps({'time':start,'name':name,'query':q,'replica_identity':'UNKNOWN','response':r})+'\n')
  if r.get('status')!='success':raise RuntimeError('query unsuccessful')
  return r['data']['result']
 except Exception as e:
  event('observation_error',metric=name,error=str(e));return None
def stop(reason):
 if not STOP.exists():STOP.write_text(json.dumps({'time':time.time(),'reason':reason}));event('stop',reason=reason);print('STOP',reason,flush=True)
def detail():
 while True:
  for name in ['up','resources','processes','tiana','otel','alerts']:query(name,queries[name])
  time.sleep(15)
threading.Thread(target=detail,daemon=True).start()
while True:
 start=time.monotonic()
 for name in ['cpu','cpu_instant','memory','sample_age']:
  values=query(name,queries[name])
  if not values or len(values)<7:stop('missing node monitoring: '+name);continue
  for v in values:
   value=float(v['value'][1]);limit=60 if name=='sample_age' else 95
   if not math.isfinite(value) or value>=limit:stop(name+' threshold: '+json.dumps(v))
 time.sleep(max(0,5-(time.monotonic()-start)))
