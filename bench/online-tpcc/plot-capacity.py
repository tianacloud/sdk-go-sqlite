import json,collections,pathlib
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from common import *
rows=json.loads((EVIDENCE/'capacity-table.json').read_text());nodes=collections.defaultdict(list)
for row in rows:
 phase={'single-node33-r1':'baseline003','baseline-instance002':'baseline002'}.get(row['phase'],row['phase'])
 resource=EVIDENCE/f'resources-{phase}.json'
 row['cpu_peak_percent']=None;row['memory_peak_percent']=None
 if resource.exists():
  for value in json.loads(resource.read_text())['node_resources']:
   if value['node']==row['node']:row[value['metric']+'_peak_percent']=value['max']
 nodes[row['node']].append(row)
fig,axes=plt.subplots(2,3,figsize=(15,8),constrained_layout=True)
metrics=[('cpu_peak_percent','Peak 1-minute non-idle CPU (%)',95),('memory_peak_percent','Peak memory (%)',95),('max_instance_30s_p90_ms','Highest instance 30s SQL p90 (ms)',None),('min_instance_all_sql_qps','Lowest instance SQL requests/s',10),('sum_all_sql_qps','Node SQL requests/s',None),('cold_connection_max_ms','Slowest first connection request (ms)',None)]
for ax,(metric,label,threshold) in zip(axes.flat,metrics):
 for node,points in sorted(nodes.items()):
  points=sorted(points,key=lambda p:p['test_instances']);valid=[p for p in points if p.get(metric)is not None]
  ax.plot([p['test_instances'] for p in valid],[p[metric] for p in valid],marker='o',label='10.81.32.164' if '32-164'in node else '10.81.33.145')
 ax.set_xlabel('Actual concurrent TPC-C instances on node');ax.set_ylabel(label);ax.grid(alpha=.2)
 if threshold is not None:ax.axhline(threshold,color='#b91c1c',linestyle='--',linewidth=1)
 if metric in ['cpu_peak_percent','memory_peak_percent']:ax.axhline(85,color='#a16207',linestyle=':',linewidth=1)
axes[0,0].legend();fig.suptitle('Online TPC-C: 300s steady windows; first-connection panel includes startup')
axes[1,2].set_title('Client first requests; startup/failed-connect caveats in report',fontsize=8)
fig.savefig(EVIDENCE/'capacity-curves.png',dpi=160);fig.savefig(EVIDENCE/'capacity-curves.svg');record('capacity-plot-data',rows)
print('saved capacity-curves.png and .svg')
