"""Restore the explicitly agreed production admission thresholds after this test."""
import sys,time,json,urllib.request,uuid
from common import *
path='/api/v1/platform/control-clusters/'+CLUSTER
current=call('GET',path,gaia=True)
record('control-before-crud-disable',current)
for key in ['activation_cpu_usage_limit_percent','activation_memory_usage_limit_percent']:current[key]=100
headers={'Content-Type':'application/json','Host':PROFILE['gaia_host'],'Authorization':'Bearer '+os.environ['GAIA_TOKEN'],'X-Gaia-Token-Access':'1','X-Tiana-Cluster':CLUSTER,'X-Tiana-Operator':'online-tpcc-20261010','Idempotency-Key':os.environ.get('TPCC_ADMISSION_OPERATION_KEY','online-tpcc-20261010-restore-admission80')}
def request(method,path,body):
 rid=str(uuid.uuid4());headers['X-Request-ID']=rid
 req=urllib.request.Request(PROFILE['gaia_origin']+path,data=json.dumps(body).encode(),headers=headers,method=method)
 with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req,timeout=40) as response:v=json.load(response)
 event('admission_restoration_api',method=method,path=path,request_id=rid)
 return v
saved=request('PUT',path,current);record('control-crud-disable-saved',saved)
assert all(saved.get(k)==100 for k in ['activation_cpu_usage_limit_percent','activation_memory_usage_limit_percent'])
op=request('POST',path+'/deploy',{});record('control-crud-disable-operation',op)
operation_path='/api/v1/platform/deployment-operations/'+op['operation_id']
while True:
 op=call('GET',operation_path,gaia=True);record('control-crud-disable-operation-latest',op)
 if op['state']=='SUCCEEDED':break
 if op['state'] in ['FAILED','CANCELED','CANCELLED','PAUSED']:raise RuntimeError('restore operation requires follow-up: '+op['state'])
 time.sleep(5)
while True:
 nodes=call('GET','/api/v1/nodes',gaia=True);record('control-crud-disable-node-verification',nodes)
 if len(nodes)==2 and all(all(x.get('Control',{}).get('resource_admission',{}).get(k,{}).get('limit_percent')==100 and not x['Control']['resource_admission'][k]['enabled'] for k in ['cpu','memory']) for x in nodes):break
 time.sleep(5)
record('admission-restoration-required',{'cluster':CLUSTER,'restore_cpu_percent':80,'restore_memory_percent':80,'restore_status':'REQUIRED_AFTER_CRUD','time':time.time(),'operation_id':op['operation_id']})
print('admission disabled100 and verified on both nodes',flush=True)
