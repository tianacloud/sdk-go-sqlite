import os,json,pathlib,time,uuid,tempfile,http.cookiejar,urllib.request,urllib.error
ROOT=pathlib.Path(__file__).resolve().parents[2]
EVIDENCE=pathlib.Path(os.environ.get('TPCC_EVIDENCE_DIR', str(ROOT.parent/'specifications/context/deployments/online/2026-10-10-tpcc/evidence')))
EVIDENCE.mkdir(parents=True,exist_ok=True)
PRIVATE=ROOT/'.work/private'; PRIVATE.mkdir(parents=True,exist_ok=True); PRIVATE.chmod(0o700)
PROFILE=json.loads((pathlib.Path.home()/'.config/tiana-debug/online.json').read_text())
CLUSTER='online-shanghai-control-01'
CONSOLE='https://console.tianacloud.com'
def clean(v):
 if isinstance(v,dict):
  return {k:('[REDACTED]' if (not str(k).startswith('sqlite-') and any(s in str(k).lower() for s in ['token','password','secret','credential','private_key','tls_key','dsn','cookie'])) else clean(x)) for k,x in v.items()}
 if isinstance(v,list):return [clean(x) for x in v]
 return v
def record(name,v):
 p=EVIDENCE/(name+'.json');p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(clean(v),ensure_ascii=False,indent=2)+'\n')
def event(kind,**values):
 with (EVIDENCE/'events.jsonl').open('a') as f:f.write(json.dumps(clean({'time':time.time(),'kind':kind,**values}),ensure_ascii=False)+'\n')
def save_private(name,data):
 p=PRIVATE/name
 with tempfile.NamedTemporaryFile(mode='w',dir=PRIVATE,prefix='.write-',delete=False) as f:
  json.dump(data,f);temporary=f.name
 os.replace(temporary,p)
def call(method,path,body=None,gaia=False):
 jar=http.cookiejar.MozillaCookieJar(str(PRIVATE/os.environ.get('TPCC_COOKIE_FILE','cookies')))
 if not gaia and pathlib.Path(jar.filename).exists():jar.load(ignore_discard=True,ignore_expires=True)
 opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPCookieProcessor(jar))
 headers={'Content-Type':'application/json','Accept':'application/json','X-Request-ID':str(uuid.uuid4())}
 origin=CONSOLE
 if gaia:
  origin=PROFILE['gaia_origin'];headers.update({'Host':PROFILE['gaia_host'],'Authorization':'Bearer '+os.environ['GAIA_TOKEN'],'X-Gaia-Token-Access':'1','X-Tiana-Cluster':CLUSTER})
 else:
  headers['Origin']=CONSOLE
  for c in jar:
   if c.name=='tiana_mgr_csrf':headers['X-CSRF-Token']=c.value
 started=time.time()
 req=urllib.request.Request(origin+path,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method)
 try:
  with opener.open(req,timeout=40) as r: data=json.load(r);status=r.status
 except urllib.error.HTTPError as e:
  raw=e.read().decode(); event('api_error',method=method,path=path,status=e.code,request_id=headers['X-Request-ID']); raise RuntimeError(f'{method} {path} HTTP {e.code}: {raw[:300]}') from None
 if not gaia:jar.save(ignore_discard=True,ignore_expires=True);pathlib.Path(jar.filename).chmod(0o600)
 event('api',method=method,path=path,status=status,elapsed_ms=(time.time()-started)*1000,request_id=headers['X-Request-ID'])
 return data
if __name__=='__main__':
 import sys
 if sys.argv[1]=='account':
  rid=str(uuid.uuid4()); record('account-request',{'request_id':rid})
  a=call('POST','/api/v1/platform/test-accounts',{'request_id':rid},True);save_private('account.json',a);record('account',a)
  print('account created',a.get('tenant_id'))
  login=call('POST','/api/v1/auth/email/login',{'email':a['email'],'password':a['password']});record('login',login);print('login',login.get('status'))
