import collections,datetime,gzip,http.client,json,math,random,sys,time,threading
from common import *
from prepare import literal
TPCC=ROOT/'.work/py-tpcc/pytpcc';sys.path.insert(0,str(TPCC))
from drivers.sqlitedriver import SqliteDriver,TXN_QUERIES
from runtime.executor import Executor
from util import scaleparameters,rand,nurand
STOP=ROOT/'.work/STOP'
class Stopped(Exception):pass
def close_retained(n):
 p=PRIVATE/f'retained-session-{n:03d}.json'
 if not p.exists():return
 config=json.loads(p.read_text());conn=http.client.HTTPConnection('127.0.0.1',18761,timeout=40)
 try:
  conn.request('POST','/sql',json.dumps(dict(config,close=True)),{'Content-Type':'application/json'})
  r=conn.getresponse();v=json.loads(r.read());event('retained_session_closed',instance=n,response=v)
  if v.get('error'):event('retained_remote_close_unconfirmed',instance=n,error=v['error'],local_transport_released=True)
  p.unlink(missing_ok=True)
 finally:conn.close()
class Client:
 def __init__(self,n,phase,qps=0,reuse_retained=False):
  self.n=n;self.phase=phase;self.qps=qps;self.last=0.;self.rows=[];self.txn='';self.rollback_expected=False
  self.config=json.loads((PRIVATE/f'instance-{n:03d}.json').read_text());self.instance_id=self.config['id'];self.reuse_retained=reuse_retained
  if reuse_retained:
   retained=PRIVATE/f'retained-session-{n:03d}.json'
   self.config=json.loads(retained.read_text());retained.unlink()
   event('retained_session_claimed',instance=n,phase=phase,reason='continue simple SQL on existing SDK session without a new activation')
  else:self.config['id']+=f'-{phase}-{uuid.uuid4().hex[:8]}'
  self.conn=http.client.HTTPConnection('127.0.0.1',18761,timeout=40);self.samples=collections.deque();self.seq=0;self.window_start=None;self.last_window_log=0
  self.log=(EVIDENCE/f'sql-{n:03d}-{phase}.jsonl').open('a',buffering=1)
 def execute(self,q,args=None,cleanup=False):
  if STOP.exists() and not cleanup:raise Stopped(STOP.read_text())
  if args:
   parts=q.split('?');assert len(parts)==len(args)+1
   q=''.join(p+literal(a) for p,a in zip(parts,args))+parts[-1]
  wait=max(0,self.last+1/self.qps-time.monotonic()) if self.qps else 0
  if wait:time.sleep(wait)
  if STOP.exists() and not cleanup:raise Stopped(STOP.read_text())
  start=time.time();self.last=time.monotonic();self.conn.request('POST','/sql',json.dumps(dict(self.config,sql=q)),{'Content-Type':'application/json'})
  r=self.conn.getresponse();v=json.loads(r.read());elapsed=(time.time()-start)*1000;self.seq+=1
  self.log.write(json.dumps({'time':start,'instance':self.n,'instance_id':self.instance_id,'phase':self.phase,'txn':self.txn,'seq':self.seq,'sql_kind':q.lstrip().split()[0],'sql_bytes':len(q),'elapsed_ms':elapsed,'sdk_ms':v.get('elapsed_ms'),'rate_wait_ms':wait*1000,'error':v.get('error'),'request_id':v.get('request_id'),'affected':(v.get('result') or {}).get('affected_row_count'),'cold_connection':self.seq==1 and not self.reuse_retained,'retained_session_reused':self.reuse_retained})+'\n')
  if self.seq==1:
   event('retained_session_first_request' if self.reuse_retained else 'cold_connection',instance=self.n,phase=self.phase,elapsed_ms=elapsed,request_id=v.get('request_id'))
   self.window_start=time.time()
  else:
   self.samples.append((start,elapsed))
  cutoff=time.time()-30
  while self.samples and self.samples[0][0]<cutoff:self.samples.popleft()
  if self.samples and time.time()-self.window_start>=30 and not cleanup:
   p90=sorted(x[1] for x in self.samples)[math.ceil(len(self.samples)*.9)-1]
   if time.time()-self.last_window_log>=5:
    with (EVIDENCE/f'latency-windows-{self.n:03d}-{self.phase}.jsonl').open('a') as f:f.write(json.dumps({'time':time.time(),'window_seconds':30,'instance':self.n,'phase':self.phase,'samples':len(self.samples),'p90_ms':p90,'p99_ms':sorted(x[1] for x in self.samples)[math.ceil(len(self.samples)*.99)-1],'max_ms':max(x[1] for x in self.samples),'observed_sql_qps':len(self.samples)/30})+'\n')
    self.last_window_log=time.time()
  if v.get('error'):raise RuntimeError(v['error'])
  if self.seq==1 and not self.reuse_retained:close_retained(self.n)
  self.rows=[]
  for row in (v.get('result') or {}).get('rows',[]):
   values=[]
   for x in row:
    t=x['type'];value=x.get('value')
    if t=='integer':value=int(value)
    elif t=='float':value=float(value)
    values.append(value)
   self.rows.append(tuple(values))
  return self
 def fetchone(self):return self.rows.pop(0) if self.rows else None
 def fetchall(self):r=self.rows;self.rows=[];return r
 def commit(self):self.execute('COMMIT')
 def rollback(self):self.rollback_expected=True;self.execute('ROLLBACK',cleanup=True)
 def finish(self,completed):
  if completed and (ROOT/'.work/RETAIN-SESSIONS').exists() and not STOP.exists():
   save_private(f'retained-session-{self.n:03d}.json',self.config)
   event('session_retained_between_phases',instance=self.n,phase=self.phase,reason='retain open SDK connection to avoid synchronized idle-stop bursts during density ramp')
   self.conn.close();self.log.close()
  else:self.close()
 def close(self):
  try:
   self.conn.request('POST','/sql',json.dumps(dict(self.config,close=True)),{'Content-Type':'application/json'});r=self.conn.getresponse();v=json.loads(r.read());event('session_closed',instance=self.n,phase=self.phase,response=v)
  finally:self.conn.close();self.log.close()
def load(n,resume=0,chunks=5):
 if (PRIVATE/f'fixture-{n:03d}.json').exists():
  event('fixture_already_complete',instance=n);return
 pause=ROOT/'.work/PREP-PAUSE'
 if pause.exists():event('fixture_preparation_paused',instance=n,reason=pause.read_text().strip())
 while pause.exists():
  if STOP.exists():raise Stopped(STOP.read_text())
  time.sleep(1)
 completed=False
 c=Client(n,'load-resumed' if resume else 'load-new');event('load_start',instance=n,resume_committed_batches=resume,fixture_chunks_per_batch=chunks,maximum_rows_per_batch=100*chunks)
 try:
  for attempt in range(3):
   try:c.execute('SELECT 1');break
   except Stopped:raise
   except Exception as e:
    event('fixture_initial_read_error',instance=n,attempt=attempt+1,error=str(e),mutations_sent=False)
    c.close()
    if attempt==2:raise
    time.sleep(1);c=Client(n,'load-resumed' if resume else 'load-new')
  c.execute('PRAGMA foreign_keys=OFF')
  c.execute('PRAGMA journal_mode');print('journal_mode',c.fetchall(),flush=True)
  with gzip.open(ROOT/'.work/warehouse.jsonl.gz','rt') as f:
   batch=[];table=None;batch_number=0
   def flush():
    nonlocal batch_number
    if not batch:return
    batch_number+=1
    if batch_number<=resume:batch.clear();return
    c.execute('BEGIN');c.execute('INSERT INTO '+table+' VALUES '+','.join(batch));c.commit();batch.clear()
   for line in f:
    x=json.loads(line)
    if x['table']=='schema':
     if not resume:c.execute(x['sql'])
     continue
    if table!=x['table']:flush();table=x['table']
    batch.append(x['sql'].split(' VALUES ',1)[1])
    if len(batch)>=chunks:flush()
   flush()
  c.execute('PRAGMA foreign_keys=ON')
  c.execute('EXPLAIN QUERY PLAN '+TXN_QUERIES['STOCK_LEVEL']['getStockCount'],[1,1,3001,2981,1,15]);record(f'instance-{n:03d}-stock-query-plan',c.fetchall())
  save_private(f'fixture-{n:03d}.json',{'instance':n,'completed_at':time.time(),'chunks_per_batch':chunks,'sql_count':c.seq,'rows':598056});event('load_complete',instance=n,sql_count=c.seq);print('load_complete',n,c.seq,flush=True);completed=True
 finally:c.finish(completed)
def run(n,phase,seconds,qps):
 hold=ROOT/'.work'/('HOLD-'+phase)
 if hold.exists():event('phase_barrier_wait',instance=n,phase=phase,reason=hold.read_text().strip())
 while hold.exists():
  if STOP.exists():raise Stopped(STOP.read_text())
  time.sleep(.2)
 completed=False
 c=Client(n,phase,qps);d=SqliteDriver(str(TPCC/'tpcc.sql'));d.conn=c;d.cursor=c
 random.seed(20261010+n);fixture=json.loads((ROOT/'.work/fixture-meta.json').read_text());nu=nurand.NURandC(**fixture['nurand_load']);rand.setNURand(nurand.makeForRun(nu))
 ex=Executor(d,scaleparameters.makeDefault(1));end=time.monotonic()+seconds;counts=collections.Counter();event('workload_start',instance=n,phase=phase,seconds=seconds,qps=qps)
 try:
  while time.monotonic()<end and not (ROOT/'.work'/('END-'+phase)).exists():
   txn,p=ex.doOne();c.txn=txn;c.rollback_expected=False;start=time.time();seq=c.seq
   try:
    c.execute('BEGIN');result=d.executeTransaction(txn,p);outcome='expected_rollback' if c.rollback_expected else 'committed';counts[txn]+=1
   except Stopped:raise
   except Exception as e:
    event('transaction_error',instance=n,phase=phase,txn=txn,error=str(e),action='close failed session and generate a new transaction; do not replay failed SQL or transaction')
    with (EVIDENCE/f'transactions-{n:03d}-{phase}.jsonl').open('a') as f:f.write(json.dumps({'time':start,'instance':n,'phase':phase,'txn':txn,'outcome':'failed_or_unknown','duration_ms':(time.time()-start)*1000,'sql_count':c.seq-seq,'error':str(e)})+'\n')
    try:c.close()
    except Exception as close_error:event('failed_session_close_error',instance=n,phase=phase,error=str(close_error))
    time.sleep(1)
    c=Client(n,phase,qps);d.conn=c;d.cursor=c
    continue
   with (EVIDENCE/f'transactions-{n:03d}-{phase}.jsonl').open('a') as f:f.write(json.dumps({'time':start,'instance':n,'phase':phase,'txn':txn,'outcome':outcome,'duration_ms':(time.time()-start)*1000,'sql_count':c.seq-seq})+'\n')
  event('workload_complete',instance=n,phase=phase,counts=dict(counts),sql_count=c.seq);print('workload_complete',n,phase,dict(counts),flush=True);completed=True
 finally:c.finish(completed)
if __name__=='__main__':
 if sys.argv[1]=='load':load(int(sys.argv[2]),int(sys.argv[3]) if len(sys.argv)>3 else 0,int(sys.argv[4]) if len(sys.argv)>4 else 5)
 else:run(int(sys.argv[2]),sys.argv[3],int(sys.argv[4]),float(sys.argv[5]))
