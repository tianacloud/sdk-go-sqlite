"""Summarize resource samples in the same completed SQL measurement windows."""
import collections
import math
import statistics
import sys
from common import *

phase = sys.argv[1]
windows = json.loads((EVIDENCE/(phase+'-progress.json')).read_text())['windows']
supplemental = EVIDENCE/(phase+'-supplemental-windows.json')
if supplemental.exists():
    windows += json.loads(supplemental.read_text())['windows']
windows.sort(key=lambda w: w['start'])
out = [dict(start=w['start'], end=w['end'], node_counts=w['node_counts'],
            series=collections.defaultdict(list), failed_queries=[]) for w in windows]
names = ['cpu', 'cpu_instant', 'memory', 'sample_age']
with (EVIDENCE/'metrics-live.jsonl').open() as stream:
    for line in stream:
        if not any('"name": "'+name+'"' in line[:150] for name in names):
            continue
        x = json.loads(line)
        for window in out:
            if not window['start'] <= x['time'] <= window['end']:
                continue
            if x.get('response', {}).get('status') != 'success':
                window['failed_queries'].append({'time': x['time'], 'metric': x['name']})
            for row in x.get('response', {}).get('data', {}).get('result', []):
                node = row['metric'].get('node_id', row['metric'].get('instance'))
                value = float(row['value'][1])
                if math.isfinite(value):
                    window['series'][(node, x['name'])].append(value)
for window in out:
    series = window.pop('series')
    window['resources'] = [{'node': node, 'metric': metric, 'samples': len(values),
                           'min': min(values), 'max': max(values), 'mean': statistics.mean(values)}
                          for (node, metric), values in sorted(series.items())]
record(phase+'-window-resources', {'phase': phase, 'windows': out, 'replica_identity': 'UNKNOWN',
    'cpu_definition': '100 minus idle rate or irate; includes I/O wait; these are separate guard series'})
print('resource windows', len(out), 'last node densities', out[-1]['node_counts'] if out else {})
