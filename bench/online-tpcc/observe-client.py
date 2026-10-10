import os,time,json,pathlib
from common import *
while True:
 processes=[]
 for p in pathlib.Path('/proc').iterdir():
  if not p.name.isdigit():continue
  try:
   raw=(p/'cmdline').read_bytes()
   if b'online-tpcc/' not in raw and b'tpcc-bridge' not in raw:continue
   stat=(p/'stat').read_text();fields=stat[stat.rfind(')')+2:].split()
   processes.append({'pid':int(p.name),'user_ticks':int(fields[11]),'system_ticks':int(fields[12]),'rss_pages':int(fields[21]),'threads':int(fields[17])})
  except (OSError,ValueError,IndexError):pass
 with (EVIDENCE/'client-resources.jsonl').open('a') as f:f.write(json.dumps({'time':time.time(),'clock_ticks':os.sysconf('SC_CLK_TCK'),'page_size':os.sysconf('SC_PAGE_SIZE'),'logical_cpu_count':os.cpu_count(),'loadavg':os.getloadavg(),'meminfo':{x.split(':')[0]:x.split(':')[1].strip() for x in pathlib.Path('/proc/meminfo').read_text().splitlines() if x.startswith(('MemTotal:','MemAvailable:'))},'cpu_ticks':pathlib.Path('/proc/stat').read_text().splitlines()[0],'processes':processes,'network_counters':pathlib.Path('/proc/net/dev').read_text()})+'\n')
 time.sleep(5)
