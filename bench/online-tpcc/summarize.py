import sys,json,math,collections,pathlib,csv
from common import *
def quantile(values,p):
 return sorted(values)[max(0,math.ceil(len(values)*p)-1)] if values else None
summary=[];windows=[]
for path in sorted(EVIDENCE.glob('sql-[0-9]*.jsonl')):
 rows=[]
 for line in path.read_text().splitlines():
  try:rows.append(json.loads(line))
  except json.JSONDecodeError:continue
 if not rows:continue
 values=[r['elapsed_ms'] for r in rows];end=rows[-1]['time']+rows[-1]['elapsed_ms']/1000;duration=end-rows[0]['time']
 item={'file':path.name,'instance':rows[0]['instance'],'phase':rows[0]['phase'],'start':rows[0]['time'],'end':end,'seconds':duration,'sql_count':len(rows),'business_sql_count':sum(r['sql_kind'] not in ['BEGIN','COMMIT','ROLLBACK'] for r in rows),'sql_qps':len(rows)/duration,'business_sql_qps':sum(r['sql_kind'] not in ['BEGIN','COMMIT','ROLLBACK'] for r in rows)/duration,'p50_ms':quantile(values,.5),'p90_ms':quantile(values,.9),'p95_ms':quantile(values,.95),'p99_ms':quantile(values,.99),'max_ms':max(values),'errors':sum(bool(r['error']) for r in rows)};summary.append(item)
 buckets=collections.defaultdict(list)
 for r in rows:buckets[int(r['time']//10)*10].append(r)
 for t,rs in sorted(buckets.items()):windows.append({'window_start':t,'instance':item['instance'],'phase':item['phase'],'sql_count':len(rs),'sql_qps':len(rs)/10,'p90_ms':quantile([r['elapsed_ms'] for r in rs],.9),'errors':sum(bool(r['error']) for r in rs)})
record('sql-summary',summary)
for name,rs in [('sql-summary',summary),('sql-10s-windows',windows)]:
 if rs:
  with (EVIDENCE/(name+'.csv')).open('w') as f:w=csv.DictWriter(f,fieldnames=list(rs[0]));w.writeheader();w.writerows(rs)
print(json.dumps([{k:x[k] for k in ['instance','phase','sql_count','business_sql_qps','p90_ms','errors']} for x in summary],indent=2))
