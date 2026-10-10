import concurrent.futures,os,shutil,subprocess,sys
from common import *
SCRIPT=ROOT/'bench/online-tpcc'
nums=[int(x) for x in sys.argv[1].split(',')]
def check():
 if (ROOT/'.work/STOP').exists():raise RuntimeError('resource STOP active')
def create_lane(cookie,items):
 for n in items:
  check()
  subprocess.run([sys.executable,str(SCRIPT/'create.py'),str(n)],cwd=ROOT,env=dict(os.environ,TPCC_COOKIE_FILE=cookie),check=True)
for group in sorted({(n-1)//8+1 for n in nums}):
 check();members=[n for n in nums if (n-1)//8+1==group and not (PRIVATE/f'instance-{n:03d}.json').exists()]
 if not members:continue
 accounts=json.loads((PRIVATE/'test-accounts.json').read_text())
 if not any(a['group']==group for a in accounts):subprocess.run([sys.executable,str(SCRIPT/'create-account.py'),str(group)],cwd=ROOT,check=True)
 account=json.loads((PRIVATE/'account.json').read_text())
 if account['group']!=group:raise RuntimeError('current account does not match requested creation group')
 pw=call('GET',f"/api/v1/platform/test-accounts/{account['id']}/password",gaia=True)
 lane_count=min(3,len(members));cookies=[f'cookies-prepare-{group}-{i}' for i in range(lane_count)]
 shutil.copy2(PRIVATE/'cookies',PRIVATE/cookies[0])
 for cookie in cookies[1:]:
  check();os.environ['TPCC_COOKIE_FILE']=cookie
  try:
   login=call('POST','/api/v1/auth/email/login',{'email':account['email'],'password':pw['password']})
   record(f'prepare-session-group-{group}-{cookie}',login)
  finally:os.environ.pop('TPCC_COOKIE_FILE',None)
 try:
  with concurrent.futures.ThreadPoolExecutor(max_workers=lane_count) as pool:
   for f in concurrent.futures.as_completed([pool.submit(create_lane,cookies[i],members[i::lane_count]) for i in range(lane_count)]):f.result()
 finally:
  shutil.copy2(PRIVATE/cookies[0],PRIVATE/'cookies')
  shutil.copy2(PRIVATE/cookies[0],PRIVATE/f'cookies-group-{group}')
  for cookie in cookies:(PRIVATE/cookie).unlink(missing_ok=True)
 event('instance_group_prepared',group=group,instances=members,creation_lanes=lane_count,authentication='separate sessions, sequential writes per session')
