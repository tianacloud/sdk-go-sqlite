import sys,shutil
from common import *
group=int(sys.argv[1]); registry=PRIVATE/'test-accounts.json'
accounts=json.loads(registry.read_text())
if any(x['group']==group for x in accounts):raise RuntimeError('account group already exists')
if (ROOT/'.work/STOP').exists():raise RuntimeError('stop active')
old=json.loads((PRIVATE/'account.json').read_text());old_group=next(x['group'] for x in accounts if x['id']==old['id'])
shutil.copy2(PRIVATE/'cookies',PRIVATE/f'cookies-group-{old_group}')
a=call('POST','/api/v1/platform/test-accounts',{'request_id':str(uuid.uuid4())},True)
a['group']=group;accounts.append(a);save_private('test-accounts.json',accounts);save_private(f'account-group-{group}.json',a);record(f'account-group-{group}',a)
pw=call('GET',f"/api/v1/platform/test-accounts/{a['id']}/password",gaia=True)
(PRIVATE/'cookies').unlink()
login=call('POST','/api/v1/auth/email/login',{'email':a['email'],'password':pw['password']});record(f'login-group-{group}',login)
save_private('account.json',a);shutil.copy2(PRIVATE/'cookies',PRIVATE/f'cookies-group-{group}')
print('test account group created',group,a['tenant_id'],flush=True)
