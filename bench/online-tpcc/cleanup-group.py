"""Delete only this benchmark's recorded instances for one dedicated test account."""
import sys,json,time,uuid,os
from common import *
group=int(sys.argv[1]);accounts=json.loads((PRIVATE/'test-accounts.json').read_text());account=next(a for a in accounts if a['group']==group)
os.environ['TPCC_COOKIE_FILE']=f'cleanup-cookies-group-{group}'
pw=call('GET',f"/api/v1/platform/test-accounts/{account['id']}/password",gaia=True)
call('POST','/api/v1/auth/email/login',{'email':account['email'],'password':pw['password']})
items=[]
for p in sorted(PRIVATE.glob('instance-*.json')):
 v=json.loads(p.read_text())
 if v.get('account_group')==group:
  assert v['tenant_id']==account['tenant_id'] and v['account_id']==account['id']
  items.append((int(p.stem.split('-')[-1]),v['id']))
for n,iid in items:
 done=EVIDENCE/f'cleanup-{n:03d}-operation.json'
 if done.exists() and json.loads(done.read_text()).get('state')=='success':continue
 accepted=EVIDENCE/f'cleanup-{n:03d}-accepted.json'
 if accepted.exists():r=json.loads(accepted.read_text())
 else:
  request_file=EVIDENCE/f'cleanup-{n:03d}-request.json'
  if request_file.exists():request=json.loads(request_file.read_text())
  else:
   request={'request_id':str(uuid.uuid4())};record(f'cleanup-{n:03d}-request',request)
  r=call('DELETE',f'/api/v1/instances/{iid}',request);record(f'cleanup-{n:03d}-accepted',r)
 deadline=time.time()+300
 while True:
  v=call('GET',f"/api/v1/instances/{iid}/operations/{r['operation_id']}");record(f'cleanup-{n:03d}-operation',v)
  if v.get('state')=='success':break
  if v.get('state')=='failed':raise RuntimeError(f'instance {n} deletion failed; inspect saved operation')
  if time.time()>deadline:raise RuntimeError(f'instance {n} deletion still pending; operation retained for follow-up')
  time.sleep(2)
 event('test_instance_deleted',instance=n,instance_id=iid,group=group)
 time.sleep(1)
print('group cleanup complete',group,len(items),flush=True)
