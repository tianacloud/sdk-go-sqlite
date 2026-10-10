"""Supplement only this task's test pool; leave existing instances and quotas intact."""
import subprocess,sys,time
from common import *
stop=ROOT/'.work/STOP'
for group in range(106,138):
 if stop.exists():raise RuntimeError('STOP before pool creation')
 subprocess.run([sys.executable,str(ROOT/'bench/online-tpcc/create-account.py'),str(group)],cwd=ROOT,check=True)
 for n in range((group-1)*8+1,group*8+1):
  subprocess.run([sys.executable,str(ROOT/'bench/online-tpcc/create.py'),str(n)],cwd=ROOT,check=True)
 record('crud-pool-creation-progress',{'time':time.time(),'completed_group':group,'created_last_instance':group*8,'target_last_instance':1096})
record('crud-pool-creation-complete',{'time':time.time(),'new_instances':256,'owned_instances':1096})
