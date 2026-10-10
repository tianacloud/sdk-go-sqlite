import sys,json,gzip,random,datetime,re,sqlite3
from common import *
TPCC=ROOT/'.work/py-tpcc/pytpcc';sys.path.insert(0,str(TPCC))
from runtime.loader import Loader
from drivers.abstractdriver import AbstractDriver
from util import scaleparameters,rand,nurand

def literal(x):
 if x is None:return 'NULL'
 if isinstance(x,(int,float)):return str(x)
 return "'"+str(x).replace("'","''")+"'"
class Writer(AbstractDriver):
 def __init__(self,out):
  super().__init__('export',str(TPCC/'tpcc.sql'));self.out=out;self.counts={}
 def loadTuples(self,table,rows):
  self.counts[table]=self.counts.get(table,0)+len(rows)
  for i in range(0,len(rows),100):
   batch=rows[i:i+100];q='INSERT INTO '+table+' VALUES '+','.join('('+','.join(map(literal,row))+')' for row in batch)
   self.out.write(json.dumps({'table':table,'rows':len(batch),'sql':q})+'\n')
if __name__=='__main__':
 random.seed(20261010);nu=nurand.makeForLoad();rand.setNURand(nu)
 (ROOT/'.work/fixture-meta.json').write_text(json.dumps({'nurand_load':vars(nu)})+'\n')
 with gzip.open(ROOT/'.work/warehouse.jsonl.gz','wt') as f:
  ddl=(TPCC/'tpcc.sql').read_text();ddl=re.sub(r'--[^\n]*','',ddl)
  for q in ddl.split(';'):
   if q.strip():f.write(json.dumps({'table':'schema','rows':0,'sql':q.strip()})+'\n')
  w=Writer(f);Loader(w,scaleparameters.makeDefault(1),[1],True).execute()
 record('fixture',{'warehouses':1,'seed':20261010,'rows':w.counts,'nurand_load':vars(nu),'source':'https://github.com/apavlo/py-tpcc','source_revision':__import__('subprocess').check_output(['git','rev-parse','HEAD'],cwd=TPCC.parent,text=True).strip(),'batch_rows':100})
 print(w.counts)
