import sys,time,json,uuid
from common import *
n=int(sys.argv[1]);stop=ROOT/'.work/STOP'
if stop.exists():raise RuntimeError('stop condition active')
account=json.loads((PRIVATE/'account.json').read_text());record(f'instance-{n:03d}-owner',{'tenant_id':account['tenant_id'],'account_id':account['id'],'group':account.get('group',1)});key=str(uuid.uuid4());record(f'instance-{n:03d}-request',{'request_id':key})
r=call('POST','/api/v1/instances',{'request_id':key,'display_name':f'online-tpcc-20261010-{n:03d}','root_branch_name':'main','engine':'sqlite','timeline':False,'config':{}});record(f'instance-{n:03d}-create',r)
iid=r['instance_id'];op=r['operation_id']
for _ in range(90):
 s=call('GET',f'/api/v1/instances/{iid}/operations/{op}');record(f'instance-{n:03d}-operation',s)
 if s.get('state')=='success':break
 if s.get('state')=='failed':raise RuntimeError('creation failed')
 time.sleep(1)
else:raise RuntimeError('creation did not complete')
v=call('GET',f'/api/v1/instances/{iid}');record(f'instance-{n:03d}-view',v);ep=v['endpoint_id']
t=call('POST',f'/api/v1/instances/{iid}/endpoints/{ep}/tokens',{'request_id':str(uuid.uuid4()),'name':'TPC-C test','expires_at':int(time.time())+86400});save_private(f'instance-{n:03d}.json',{'id':iid,'endpoint':v['connection']['hostname'],'token':t['token'],'tenant_id':account['tenant_id'],'account_id':account['id'],'account_group':account.get('group',1)});record(f'instance-{n:03d}-token-issue',t); print('created',n,iid,flush=True)
