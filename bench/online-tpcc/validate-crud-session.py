"""Exercise the real worker with an expired preparation session and local SQLite."""
import importlib.util,json,pathlib,sqlite3,sys,tempfile
sys.path.insert(0,str(pathlib.Path(__file__).parent))
import workload as wl
from common import EVIDENCE
spec=importlib.util.spec_from_file_location('crud_worker',pathlib.Path(__file__).with_name('crud-sql.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
with tempfile.TemporaryDirectory() as directory:
 root=pathlib.Path(directory);private=root/'.work/private';private.mkdir(parents=True);logs=root/'evidence';logs.mkdir()
 (private/'instance-001.json').write_text(json.dumps({'id':'local-instance','endpoint':'local.invalid','token':'local-test'}))
 (root/'.work/RETAIN-SESSIONS').touch()
 wl.ROOT=root;wl.PRIVATE=private;wl.EVIDENCE=logs;wl.STOP=root/'.work/STOP';m.ROOT=root
 wl.event=lambda *args,**kwargs:None;m.event=wl.event
 wl.save_private=lambda name,value:(private/name).write_text(json.dumps(value))
 db=sqlite3.connect(':memory:',isolation_level=None);expired=set();closed=[];run_count=0
 class Response:
  def __init__(self,value):self.value=value
  def read(self):return json.dumps(self.value).encode()
 class Connection:
  def __init__(self,*args,**kwargs):pass
  def close(self):pass
  def request(self,method,path,body,headers):
   global run_count
   request=json.loads(body);identity=request['id']
   if request.get('close'):
    closed.append(identity);self.response={'error':'BATON_INVALID'} if identity in expired else {};return
   if identity in expired:self.response={'error':'BATON_INVALID'};return
   cursor=db.execute(request['sql']);values=cursor.fetchall() if cursor.description else []
   rows=[[{'type':'integer' if isinstance(v,int) else 'text','value':str(v)} for v in row] for row in values]
   self.response={'elapsed_ms':0.1,'request_id':'local-test','result':{'rows':rows,'affected_row_count':max(0,cursor.rowcount)}}
   if '-local-crud-' in identity:
    run_count+=1
    if run_count==7:(root/'.work/END-local-crud').touch()
  def getresponse(self):return Response(self.response)
 wl.http.client.HTTPConnection=Connection
 m.prepare(1)
 old=json.loads((private/'retained-session-001.json').read_text())['id'];expired.add(old)
 m.run(1,'local-crud',10,0)
 rows=[json.loads(line) for line in (logs/'sql-001-local-crud.jsonl').read_text().splitlines()]
 assert len(rows)==7 and all(not row['error'] for row in rows)
 assert old in closed and all(not row['retained_session_reused'] for row in rows)
 assert db.execute(f'SELECT count(*) FROM {m.TABLE}').fetchone()[0]==1001
 assert all(row['affected']==1 for row in rows if row['sql_kind'] in ['INSERT','UPDATE','DELETE'])
 result={'passed':True,'scenario':'Preparation session expires before measurement starts','measurement_requests':len(rows),'measurement_errors':0,'old_expired_session_closed':True,'new_measurement_session_used':True,'rows_after_one_crud_cycle':1001,'all_mutations_affected_one_row':True}
 (EVIDENCE/'crud-fresh-session-local-validation.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
