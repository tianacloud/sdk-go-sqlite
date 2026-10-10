import sys,json,math,collections
from common import *
def quantile(v,p):return sorted(v)[max(0,math.ceil(len(v)*p)-1)] if v else None
out=[]
for arg in sys.argv[1:]:
 n,phase=arg.split(':',1);n=int(n)
 rows=[json.loads(s) for s in (EVIDENCE/f'sql-{n:03d}-{phase}.jsonl').read_text().splitlines()]
 begin=rows[0]['time'];start=begin+120;end=begin+420
 hot=[r for r in rows if start<=r['time']<end and not r.get('cold_connection')]
 business=[r for r in hot if r['sql_kind'] not in ['BEGIN','COMMIT','ROLLBACK']]
 txp=EVIDENCE/f'transactions-{n:03d}-{phase}.jsonl'
 tx=[json.loads(s) for s in txp.read_text().splitlines()] if txp.exists() else []
 tx=[r for r in tx if start<=r['time'] and r['time']+r['duration_ms']/1000<=end]
 windows=[json.loads(s) for s in (EVIDENCE/f'latency-windows-{n:03d}-{phase}.jsonl').read_text().splitlines()]
 out.append({'instance':n,'phase':phase,'measurement_start':start,'measurement_end':end,'complete':rows[-1]['time']+rows[-1]['elapsed_ms']/1000>=end-.5,'warmup_seconds':120,'measurement_seconds':300,'sql_count':len(hot),'all_sql_qps':len(hot)/300,'business_sql_qps':len(business)/300,'errors':sum(bool(r['error']) for r in hot),'sql_p90_ms':quantile([r['elapsed_ms'] for r in hot],.9),'sql_p99_ms':quantile([r['elapsed_ms'] for r in hot],.99),'sql_max_ms':max([r['elapsed_ms'] for r in hot],default=None),'max_complete_30s_p90_ms':max([r['p90_ms'] for r in windows if start+30<=r['time']<=end],default=None),'cold_connections':[r['elapsed_ms'] for r in rows if r.get('cold_connection')],'transactions':dict(collections.Counter(r['txn'] for r in tx)),'committed_transactions':sum(r['outcome']=='committed' for r in tx),'new_order_tpm':sum(r['txn']=='NEW_ORDER' and r['outcome']=='committed' for r in tx)/5})
record('phase-analysis-'+str(int(time.time())),out);print(json.dumps(out,ensure_ascii=False,indent=2))
