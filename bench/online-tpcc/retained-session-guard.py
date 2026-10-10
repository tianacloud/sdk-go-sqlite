import time,concurrent.futures
from workload import *
while True:
 if STOP.exists():
  nums=[int(p.stem.rsplit('-',1)[1]) for p in PRIVATE.glob('retained-session-*.json')]
  with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
   for n,f in [(n,pool.submit(close_retained,n)) for n in nums]:
    try:f.result()
    except Exception as e:event('retained_close_error',instance=n,error=str(e))
 time.sleep(1)
