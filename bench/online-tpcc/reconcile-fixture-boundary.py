import sys,json,collections
from common import *
phase=sys.argv[1];nums=set(int(n) for n in sys.argv[2].split(','));p=EVIDENCE/('telemetry-reconciliation-'+phase+'.json');v=json.loads(p.read_text());selected={iid:row for iid,row in v['instances'].items() if row['instance'] in nums};sources={iid:{s['labels'].get('source_id') for s in v['series'] if s['labels']['instance_id']==iid} for iid in selected};maxima={}
with (EVIDENCE/'sql-metrics-live.jsonl').open() as f:
 for line in f:
  x=json.loads(line)
  if x['time']>v['requested_observation_end']:continue
  for r in x.get('response',{}).get('data',{}).get('result',[]):
   m=r['metric'];iid=m.get('instance_id');source=m.get('source_id')
   if iid not in selected or source not in sources[iid] or m['__name__']!='tiana_app_sql_statements_total':continue
   k=(iid,source,m.get('operation'),m.get('result'));maxima[k]=max(maxima.get(k,0),float(r['value'][1]))
results=[]
for iid,row in selected.items():
 n=row['instance'];load=sum(1 for _ in (EVIDENCE/f'sql-{n:03d}-load-new.jsonl').open());total=sum(value for k,value in maxima.items() if k[0]==iid);expected=load+row['client_sql_requests'];results.append({'instance':n,'source_ids':sorted(sources[iid]),'client_fixture_sql':load,'client_phase_sql':row['client_sql_requests'],'server_cumulative_statements':total,'server_minus_combined_client':total-expected,'exact_match':total==expected})
v['fixture_boundary_reconciliation']=results;v['fixture_boundary_method']='Compare all operation/result labels, including counters unchanged during the phase, against the contiguous fixture+first workload client requests. An exact match resolves a pre-phase scrape boundary discrepancy for these freshly created instances.';record('telemetry-reconciliation-'+phase,v);print(json.dumps(results,indent=2))
