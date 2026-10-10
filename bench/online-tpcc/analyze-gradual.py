"""Analyze common wall-clock windows for a continuously running density ramp."""
import collections, math, sys, bisect
from common import *
phase=sys.argv[1]
progress=json.loads((EVIDENCE/(phase+'-progress.json')).read_text())
windows=[dict(w,placement_file=phase+'-density'+str(w['target'])+'-before-placement.json') for w in progress['windows']]
supplemental=EVIDENCE/(phase+'-supplemental-windows.json')
if supplemental.exists():
 for w in json.loads(supplemental.read_text())['windows']:
  matches=list(EVIDENCE.glob(phase+'-paced'+str(w['target'])+'-*-placement.json'))
  assert len(matches)==1, matches
  windows.append(dict(w,placement_file=matches[0].name))
windows.sort(key=lambda w:w['start'])
def quantile(values,p):
 return sorted(values)[max(0,math.ceil(len(values)*p)-1)] if values else None
out=[dict(w,per_instance=[]) for w in windows]
placements={w['placement_file']:{r['instance_id']:r['root']['node_id'] for r in json.loads((EVIDENCE/w['placement_file']).read_text())['items']} for w in windows}
all_instances=sorted({n for w in windows for n in w['instances']})
for n in all_instances:
 instance_id=json.loads((PRIVATE/f'instance-{n:03d}.json').read_text())['id']
 rows=[json.loads(l) for l in (EVIDENCE/f'sql-{n:03d}-{phase}.jsonl').read_text().splitlines()]
 latency_file=EVIDENCE/f'latency-windows-{n:03d}-{phase}.jsonl'
 latency=[json.loads(l) for l in latency_file.read_text().splitlines()] if latency_file.exists() else []
 tx_file=EVIDENCE/f'transactions-{n:03d}-{phase}.jsonl'
 tx=[json.loads(l) for l in tx_file.read_text().splitlines()] if tx_file.exists() else []
 row_times=[r['time'] for r in rows]
 latency_times=[r['time'] for r in latency]
 tx_times=[r['time'] for r in tx]
 for w in out:
  if n not in w['instances']:continue
  start,end=w['start'],w['end'];duration=end-start
  hot=[r for r in rows[bisect.bisect_left(row_times,start):bisect.bisect_left(row_times,end)] if not r.get('cold_connection')]
  business=[r for r in hot if r['sql_kind'] not in ('BEGIN','COMMIT','ROLLBACK')]
  lat=[r['elapsed_ms'] for r in hot]
  complete_windows=latency[bisect.bisect_left(latency_times,start+30):bisect.bisect_right(latency_times,end)]
  transactions=[r for r in tx[bisect.bisect_left(tx_times,start):bisect.bisect_right(tx_times,end)] if r['time']+r['duration_ms']/1000<=end]
  w['per_instance'].append({'instance':n,'node':placements[w['placement_file']][instance_id],'complete_coverage':bool(rows and rows[0]['time']<=start and rows[-1]['time']+rows[-1]['elapsed_ms']/1000>=end-.5),'sql_count':len(hot),'all_sql_qps':len(hot)/duration,'business_sql_qps':len(business)/duration,'errors':sum(bool(r['error']) for r in hot),'sql_p90_ms':quantile(lat,.9),'sql_p99_ms':quantile(lat,.99),'sql_max_ms':max(lat,default=None),'complete_30s_windows':len(complete_windows),'max_complete_30s_p90_ms':max([r['p90_ms'] for r in complete_windows],default=None),'committed_transactions':sum(r['outcome']=='committed' for r in transactions),'transactions':dict(collections.Counter(r['txn'] for r in transactions))})
def summarize(results):
 return {'instances':len(results),'complete_instances':sum(r['complete_coverage'] for r in results),'min_all_sql_qps':min((r['all_sql_qps'] for r in results),default=None),'min_business_sql_qps':min((r['business_sql_qps'] for r in results),default=None),'instances_below_10_sql_qps':[r['instance'] for r in results if r['all_sql_qps']<10],'errors':sum(r['errors'] for r in results),'max_complete_30s_p90_ms':max((r['max_complete_30s_p90_ms'] for r in results if r['max_complete_30s_p90_ms'] is not None),default=None)}
pool_change_path=EVIDENCE/'agent-pool400-change.json'
pool_change=json.loads(pool_change_path.read_text()) if pool_change_path.exists() else None
for w in out:
 if pool_change and w['start']<pool_change.get('end',float('inf')) and w['end']>pool_change['start']:
  w['configuration_change_overlap']='Agent pool600to400; workload continues, but this window is not a fixed-configuration capacity result'
 results=w['per_instance']
 w['summary']=summarize(results)
 w['node_summaries']={node:summarize([r for r in results if r['node']==node]) for node in sorted({r['node'] for r in results})}
record(phase+'-analysis',{'phase':phase,'window_source':phase+'-progress.json','latency_method':'nearest-rank; per-instance; cold first requests excluded; only complete30s intervals fully inside common measurement window','windows':out})
print(json.dumps([{'start':w['start'],'end':w['end'],'nodes':w['node_summaries']} for w in out],indent=2))
