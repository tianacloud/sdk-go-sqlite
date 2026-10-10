"""Per-node completed simple SQL density windows."""
import csv
import collections
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from common import *

phase = 'simple-density-r1'
sql = json.loads((EVIDENCE/(phase+'-analysis.json')).read_text())['windows']
resources = {w['start']: w for w in json.loads((EVIDENCE/(phase+'-window-resources.json')).read_text())['windows']}
rows = []
for window in sql:
    if window.get('configuration_change_overlap'):
        continue
    resource = resources[window['start']]
    peaks = {(r['node'], r['metric']): r['max'] for r in resource['resources']}
    for node, summary in window['node_summaries'].items():
        rows.append({'node': node, 'density': summary['instances'],
            'agent_pool_per_node': 400,
            'measurement_seconds': window['end'] - window['start'],
            'measurement_start': window['start'], 'measurement_end': window['end'],
            'complete_instances': summary['complete_instances'],
            'minimum_instance_sql_qps': summary['min_all_sql_qps'],
            'maximum_instance_30s_p90_ms': summary['max_complete_30s_p90_ms'],
            'sql_errors': summary['errors'],
            'cpu_rate_peak_percent': peaks.get((node, 'cpu')),
            'cpu_irate_peak_percent': peaks.get((node, 'cpu_instant')),
            'memory_peak_percent': peaks.get((node, 'memory'))})
        rows[-1]['cpu_guard_peak_percent'] = max(v for v in [rows[-1]['cpu_rate_peak_percent'], rows[-1]['cpu_irate_peak_percent']] if v is not None)
record('simple-per-node-capacity-table', rows)
with (EVIDENCE/'simple-per-node-capacity-table.csv').open('w') as output:
    writer = csv.DictWriter(output, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
metrics = [('cpu_guard_peak_percent', 'Non-idle CPU: window peak / stop sample (%)', 95),
           ('memory_peak_percent', 'Memory: window peak / stop sample (%)', 95),
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
    stop_path = EVIDENCE/'simple-stop-summary.json'
    if stop_path.exists():
        stopped = json.loads(stop_path.read_text())
        original = json.loads((EVIDENCE/'simple-stop-original-observer-samples.json').read_text())['samples']
        for node, summary in sorted(stopped['node_summaries'].items()):
            color = '#1d4ed8' if '32-164' in node else '#c2410c'
            name = 'Node32' if '32-164' in node else 'Node33'
            def sample(metric_name):
                return next(float(v['value'][1]) for v in original[metric_name]['response']['data']['result'] if v['metric'].get('node_id') == node)
            stop_values = {'cpu_guard_peak_percent': max(sample('cpu'), sample('cpu_instant')),
                'memory_peak_percent': sample('memory'),
                'minimum_instance_sql_qps': summary['minimum_hot_sql_qps_preceding30s'],
                'maximum_instance_30s_p90_ms': summary['maximum_instance_p90_ms_preceding30s']}
            ax.scatter([summary['started_workers']], [stop_values[metric]], marker='^', s=70,
                facecolors='none', edgecolors=color, linewidths=1.8, label=name+' stop snapshot', zorder=5)
    if threshold is not None:
        ax.axhline(threshold, color='#991b1b', linestyle='--', linewidth=1)
    if metric in ('cpu_guard_peak_percent', 'memory_peak_percent'):
        ax.axhline(85, color='#a16207', linestyle=':', linewidth=1)
    ax.set_xlabel('Simple SQL instances on this node')
    ax.set_ylabel(label)
    ax.grid(alpha=.2)
axes[0, 0].legend(fontsize=8)
axes[1, 0].set_ylim(8.0, 12.2)
fig.suptitle('SELECT + INSERT: per-node completed windows and resource stop\nSquares: completed windows; triangles: stop resource sample / preceding 30s SQL (overlaps warmup)')
fig.savefig(EVIDENCE/'simple-per-node-capacity.png', dpi=160)
fig.savefig(EVIDENCE/'simple-per-node-capacity.svg')
print('saved per-node CSV, PNG and SVG;', len(rows), 'node-window rows')
