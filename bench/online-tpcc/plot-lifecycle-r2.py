import collections,json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from common import *
start=1791604938;end=1791606164;origin=1791604938;stop=1791605247.0940435
series=collections.defaultdict(list)
with (EVIDENCE/'metrics-live.jsonl').open() as f:
 for line in f:
  if not any('"name": "'+n+'"' in line for n in ('cpu_instant','memory')):continue
  x=json.loads(line)
  if not start<=x['time']<=end:continue
  for row in x.get('response',{}).get('data',{}).get('result',[]):
   node=row['metric'].get('node_id','')
   if node.startswith('n10'):series[(x['name'],node)].append((x['time'],float(row['value'][1])))
for metric in ('active','stop_busy_rate'):
 data=json.loads((EVIDENCE/('gradual-r2-lifecycle-'+metric+'.json')).read_text())
 for row in data['response']['data']['result']:
  series[(metric,row['metric']['node_id'])]=[(t,float(v)) for t,v in row['values']]
fig,axes=plt.subplots(2,2,figsize=(13,8),constrained_layout=True)
for ax,(metric,label) in zip(axes.flat,[('cpu_instant','Node non-idle CPU, including I/O wait (%)'),('memory','Node memory used (%)'),('active','Runtime active Agent count'),('stop_busy_rate','Runtime stop-capacity refusals / second')]):
 for (name,node),points in sorted(series.items()):
  if name!=metric:continue
  ax.plot([(t-origin)/60 for t,v in points],[v for t,v in points],label='10.81.32.164' if '32-164' in node else '10.81.33.145',linewidth=1.8)
 ax.axvline((stop-origin)/60,color='#a12b32',linestyle='--',label='95% stop')
 ax.set(xlabel='Minutes since TPC-C phase transition',ylabel=label);ax.grid(alpha=.2)
 if metric in ('cpu_instant','memory'):ax.set_ylim(0,102)
 if metric=='cpu_instant':ax.axhline(95,color='#a12b32',alpha=.3);ax.legend(fontsize=8)
fig.suptitle('TPC-C phase transition and withdrawal: per-node lifecycle signals')
fig.savefig(EVIDENCE/'gradual-r2-lifecycle.png',dpi=150);fig.savefig(EVIDENCE/'gradual-r2-lifecycle.svg')
record('gradual-r2-lifecycle-plot-data',{'start':start,'end':end,'origin':origin,'stop':stop,'series':[{'metric':k[0],'node':k[1],'samples':v} for k,v in series.items()],'replica_identity':'UNKNOWN','cpu_memory_source':'direct observer samples','active_stop_source':'Prometheus range query,15s step'})
print('lifecycle plot saved')
