"""Keep preparation cold activation and fresh measurement connections separate."""
import collections
import math
import sys
from common import *

phase = sys.argv[1]
placements = {}
with (EVIDENCE/'events.jsonl').open() as source:
    for line in source:
        row = json.loads(line)
        if row.get('kind') == 'simple_prepared_placement' and row.get('phase') == phase:
            placements[row['instance']] = row['node']

groups = collections.defaultdict(list)
for connection_phase in ['crud-sql-prepare-r3', phase]:
    for path in sorted(EVIDENCE.glob('sql-*-'+connection_phase+'.jsonl')):
        with path.open() as source:
            first = source.readline()
        if not first:
            continue
        row = json.loads(first)
        if not row.get('cold_connection'):
            continue
        groups[(connection_phase, placements.get(row['instance'], 'UNKNOWN'))].append(
            {key: row.get(key) for key in ['time', 'instance', 'instance_id',
                'elapsed_ms', 'sdk_ms', 'error', 'request_id', 'cold_connection',
                'retained_session_reused']})

results = []
for (connection_phase, node), rows in sorted(groups.items()):
    values = sorted(row['elapsed_ms'] for row in rows if not row['error'])
    def percentile(p):
        return values[max(0, math.ceil(len(values)*p)-1)] if values else None
    results.append({'connection_phase': connection_phase, 'node': node,
        'connections': len(rows), 'successful_connections': len(values),
        'errors': dict(collections.Counter(str(row['error']) for row in rows if row['error'])),
        'success_p50_ms': percentile(.5), 'success_p90_ms': percentile(.9),
        'success_p95_ms': percentile(.95), 'success_max_ms': max(values, default=None),
        'samples': rows})
record(phase+'-connections', {'phase': phase, 'time': time.time(), 'groups': results,
    'interpretation': 'Client first-request duration, nearest-rank percentiles. Preparation can activate sleeping instances; measurement establishes a fresh connection to an already prepared instance. Neither is the Gateway first-byte cold-start release-gate measurement. Kept separate from hot SQL and full 30-second windows; unknown placement remains explicit.'})
print([(r['connection_phase'], r['node'], r['connections'], r['success_p90_ms']) for r in results])
