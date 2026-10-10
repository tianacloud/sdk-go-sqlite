"""Correlate resource watermarks with per-node SQL worker lifecycle evidence."""
from collections import Counter
from common import *

ids = {int(p.stem.split('-')[-1]): json.loads(p.read_text())['id']
       for p in PRIVATE.glob('instance-*.json')}
kinds = {'workload_start', 'workload_complete', 'session_closed',
         'density_stage_end', 'gradual_run_ended', 'simple_density_ended'}
events = []
for line in (EVIDENCE/'events.jsonl').open():
    x = json.loads(line)
    if x['kind'] in kinds:
        events.append(x)
events.sort(key=lambda x: x['time'])
change_path = EVIDENCE/'agent-pool400-change.json'
change = json.loads(change_path.read_text()) if change_path.exists() else None
out = []
for path in sorted(EVIDENCE.glob('watermark85-*-trigger.json')):
    trigger = json.loads(path.read_text())
    stamp = trigger['time']
    active = set()
    for x in events:
        if x['time'] > stamp:
            break
        key = (x.get('phase'), x.get('instance'))
        if x['kind'] == 'workload_start':
            active.add(key)
        elif x['kind'] in {'workload_complete', 'session_closed'}:
            active.discard(key)
        else:
            active = {k for k in active if k[0] != x.get('phase')}
    placement = EVIDENCE/path.name.replace('-trigger.json', '-placement.json')
    if not placement.exists():
        continue
    rows = json.loads(placement.read_text())['items']
    node = trigger['sample']['metric'].get('node_id', trigger['sample']['metric'].get('instance'))
    mapping = {r['instance_id']: r['root']['node_id'] for r in rows}
    selected = [{'phase': phase, 'instance': n} for phase, n in sorted(active)
                if mapping.get(ids.get(n)) == node]
    context = 'SQL workload active' if selected else 'preparation or lifecycle; no active SQL worker'
    if change and change['start'] <= stamp <= change.get('end', float('inf')):
        context = 'Agent pool resizing with SQL workload; administrative transient'
    out.append({'time': stamp, 'node': node, 'metric': trigger['metric'],
        'value': float(trigger['sample']['value'][1]), 'threshold': 85,
        'active_sql_worker_estimate': len(selected), 'workers': selected,
        'owned_app_states_in_following_snapshot': dict(Counter(
            r['root'].get('runtime_state') for r in rows if r['root'].get('node_id') == node)),
        'trigger_source': path.name, 'placement_source': placement.name,
        'classification': context,
        'caveat': 'Worker estimate from lifecycle events; not proof of a completed steady window. Placement snapshot follows trigger; reconnects may transiently affect the estimate.'})
out.sort(key=lambda x: x['time'])
record('watermark85-per-node-summary', {'time': time.time(), 'samples': out})
print([{k: x[k] for k in ['time', 'node', 'value', 'active_sql_worker_estimate', 'classification']}
       for x in out[-6:]])
