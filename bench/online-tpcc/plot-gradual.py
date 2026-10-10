"""Per-node continuous TPC-C density plots, separated by Agent pool configuration."""
import csv
import collections
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from common import *

phase = 'gradual-density-r1'
sql = json.loads((EVIDENCE/(phase+'-analysis.json')).read_text())['windows']
resources = {w['start']: w for w in json.loads((EVIDENCE/(phase+'-window-resources.json')).read_text())['windows']}
change = json.loads((EVIDENCE/'agent-pool400-change.json').read_text())
rows = []
for window in sql:
    if window.get('configuration_change_overlap'):
        continue
    resource = resources[window['start']]
    peaks = {(r['node'], r['metric']): r['max'] for r in resource['resources']}
    for node, summary in window['node_summaries'].items():
        rows.append({'node': node, 'density': summary['instances'],
            'agent_pool_per_node': 600 if window['end'] < change['start'] else 400,
            'measurement_start': window['start'], 'measurement_end': window['end'],
            'complete_instances': summary['complete_instances'],
            'minimum_instance_sql_qps': summary['min_all_sql_qps'],
            'maximum_instance_30s_p90_ms': summary['max_complete_30s_p90_ms'],
            'sql_errors': summary['errors'],
            'cpu_rate_peak_percent': peaks.get((node, 'cpu')),
            'cpu_irate_peak_percent': peaks.get((node, 'cpu_instant')),
            'memory_peak_percent': peaks.get((node, 'memory'))})
        rows[-1]['cpu_guard_peak_percent'] = max(v for v in [rows[-1]['cpu_rate_peak_percent'], rows[-1]['cpu_irate_peak_percent']] if v is not None)
record('gradual-per-node-capacity-table', rows)
with (EVIDENCE/'gradual-per-node-capacity-table.csv').open('w') as output:
    writer = csv.DictWriter(output, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
metrics = [('cpu_guard_peak_percent', 'Peak non-idle CPU, incl. I/O wait (%)', 95),
           ('memory_peak_percent', 'Peak memory usage (%)', 95),
           ('minimum_instance_sql_qps', 'Lowest instance SQL requests/s', 10),
           ('maximum_instance_30s_p90_ms', 'Highest complete 30s SQL p90 (ms)', None)]
groups = collections.defaultdict(list)
for row in rows:
    groups[(row['node'], row['agent_pool_per_node'])].append(row)
for ax, (metric, label, threshold) in zip(axes.flat, metrics):
    for (node, pool), points in sorted(groups.items()):
        name = 'Node32' if '32-164' in node else 'Node33'
        color = '#1d4ed8' if '32-164' in node else '#c2410c'
        ax.plot([r['density'] for r in points], [r[metric] for r in points],
                color=color, marker='o' if pool == 600 else 's',
                linestyle=':' if pool == 600 else '-', markersize=4,
                label=f'{name}, pool {pool}')
    if threshold is not None:
        ax.axhline(threshold, color='#991b1b', linestyle='--', linewidth=1)
    if metric in ('cpu_guard_peak_percent', 'memory_peak_percent'):
        ax.axhline(85, color='#a16207', linestyle=':', linewidth=1)
    ax.set_xlabel('TPC-C instances on this node')
    ax.set_ylabel(label)
    ax.grid(alpha=.2)
axes[0, 0].legend(fontsize=8)
axes[1, 0].set_ylim(9.5, 12.2)
fig.suptitle('Continuous TPC-C: per-node completed 60s windows\nPool-resize window excluded; startup/exit peaks and durability failures reported separately')
fig.savefig(EVIDENCE/'gradual-per-node-capacity.png', dpi=160)
fig.savefig(EVIDENCE/'gradual-per-node-capacity.svg')
print('saved per-node CSV, PNG and SVG;', len(rows), 'node-window rows')
