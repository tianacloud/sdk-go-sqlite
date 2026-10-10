"""Export retained FS statistics as per-instance time series and observed extrema."""
import collections
import csv
import re
import sys
from common import *

owners = {row['instance_id']: row['instance'] for row in
          json.loads((EVIDENCE / 'owned-instance-workload-history.json').read_text())['instances']}
phase = sys.argv[1]
phase_start = json.loads((EVIDENCE/(phase+'-coordinator-process.json')).read_text())['start']
phase_end = float(sys.argv[2]) if len(sys.argv)>2 else time.time()
latest_queried_end = None
fields = ['pending_tasks', 'pending_bytes', 'oldest_pending_age_s',
          'upload_success_total', 'upload_failure_total', 'upload_max_ms',
          'sync_success_total', 'sync_failure_total', 'sync_max_ms',
          'last_upload_failure_age_s', 'last_sync_failure_age_s']
seen = set()
rows = []
queries = 0
truncated = 0
with (EVIDENCE / 'fs-stats-current-logs-live.jsonl').open() as source:
    for line in source:
        query = json.loads(line)
        if int(query["params"]["end"])/1e9 < phase_start or int(query["params"]["start"])/1e9 > phase_end:
            continue
        latest_queried_end = max(latest_queried_end or 0, int(query["params"]["end"])/1e9)
        queries += 1
        truncated += int(query['possibly_truncated'])
        for stream in query['response'].get('data', {}).get('result', []):
            node = stream['stream'].get('k8s_node_name')
            for stamp, message in stream['values']:
                if not int(phase_start * 1e9) <= int(stamp) <= int(phase_end * 1e9):
                    continue
                match = re.search(r'instance_id="([^"]+)"', message)
                if not match or match.group(1) not in owners:
                    continue
                key = (stamp, message)
                if key in seen:
                    continue
                seen.add(key)
                values = dict(re.findall(r'\b([a-z_]+)=([0-9]+)', message))
                iid = match.group(1)
                rows.append({'time_ns': stamp, 'instance': owners[iid], 'instance_id': iid,
                    'node': node, **{name: int(values[name]) if name in values else None for name in fields}})
rows.sort(key=lambda row: int(row['time_ns']))
with (EVIDENCE / (phase+'-fs-stats-timeseries.csv')).open('w') as output:
    writer = csv.DictWriter(output, fieldnames=['time_ns', 'instance', 'instance_id', 'node', *fields])
    writer.writeheader()
    writer.writerows(rows)
groups = collections.defaultdict(list)
for row in rows:
    groups[(row['instance_id'], row['node'])].append(row)
summary = []
for (iid, node), points in groups.items():
    counters = [p['upload_failure_total'] for p in points if p['upload_failure_total'] is not None]
    summary.append({'instance': owners[iid], 'instance_id': iid, 'node': node,
        'samples': len(points), 'first_time_ns': points[0]['time_ns'], 'last_time_ns': points[-1]['time_ns'],
        'latest': {name: points[-1][name] for name in fields},
        'observed_maxima': {name: max((p[name] for p in points if p[name] is not None), default=None) for name in fields},
        'upload_counter_decreases': sum(b < a for a, b in zip(counters, counters[1:]))})
summary.sort(key=lambda row: row['instance'])
record(phase+'-fs-stats-instance-summary', {'time': time.time(), 'phase':phase, 'requested_start':phase_start, 'requested_end':phase_end, 'latest_queried_end':latest_queried_end, 'query_windows': queries,
    'possibly_truncated_windows': truncated, 'distinct_log_entries': len(rows), 'instances': summary,
    'interpretation': 'Observed log values only. Missing fields remain null. Per-instance extrema occur at different times; do not sum them as a simultaneous node peak. Counter decreases may indicate process restarts. Historical maxima are not current error rates.'})
print('FS log entries', len(rows), 'instance/node series', len(summary), 'truncated queries', truncated)
print('nonzero upload failure series', [(x['instance'], x['latest']['upload_failure_total'], x['latest']['pending_bytes'])
      for x in summary if (x['observed_maxima']['upload_failure_total'] or 0) > 0])
