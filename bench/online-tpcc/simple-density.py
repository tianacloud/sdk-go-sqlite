"""Gradual per-node simple SQL density measurement, using retained owned instances."""
import collections
import importlib.util
import subprocess
import sys
import time
from common import *

spec = importlib.util.spec_from_file_location('density_stage', ROOT/'bench/online-tpcc/density-stage.py')
stage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stage)
phase = sys.argv[1]
prior_phase = sys.argv[2] if len(sys.argv) > 2 else None
script = ROOT/'bench/online-tpcc/simple-sql.py'
end_file = ROOT/'.work'/('END-' + phase)
if end_file.exists():
    raise RuntimeError('Phase already ended; use a new phase name')
stage.stopped()
rows = stage.snapshot(phase + '-initial')
excluded = {1, 2, 4, 5, 9, 13}
owned = {}
configs = {}
for path in PRIVATE.glob('instance-*.json'):
    n = int(path.stem.split('-')[-1])
    if n not in excluded:
        configs[n] = json.loads(path.read_text())
        owned[configs[n]['id']] = n
lanes = {node['ID']: [] for node in call('GET', '/api/v1/nodes', gaia=True)}
unassigned = []
for row in rows:
    if row['instance_id'] in owned:
        node = row['root']['node_id'] or row['root'].get('sticky_node_id') or row['root'].get('previous_node_id')
        (lanes[node] if node in lanes else unassigned).append(owned[row['instance_id']])
for lane in lanes.values():
    lane.sort()
unassigned.sort()
prepared = {}
procs = {}
nodes = {}
windows = []
failures = []
resume = None
if prior_phase:
    if not (ROOT/'.work'/('END-' + prior_phase)).exists():
        raise RuntimeError('Prior phase has not ended')
    resume = json.loads((EVIDENCE/(prior_phase + '-progress.json')).read_text())
    missing = [n for n in resume['active_instances'] if not (PRIVATE/f'retained-session-{n:03d}.json').exists()]
    if missing:
        raise RuntimeError('Prior workers have not retained every session: ' + str(missing))
    skipped = set(resume['active_instances']) | {r['instance'] for r in resume['preparation_failures']}
    lanes = {node: [n for n in lane if n not in skipped] for node, lane in lanes.items()}
    unassigned = [n for n in unassigned if n not in skipped]


def check():
    stage.stopped()
    dead = [n for n, proc in procs.items() if proc.poll() is not None]
    if dead:
        raise RuntimeError('SQL workers exited: ' + str(dead))


def wait(seconds):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        check()
        time.sleep(min(1, max(0, until - time.monotonic())))


def save():
    record(phase + '-progress', {'time': time.time(), 'phase': phase,
        'workload': 'simple_sql', 'active_instances': list(procs),
        'node_counts': dict(collections.Counter(nodes.values())),
        'prepared_waiting_node_counts': dict(collections.Counter(node for n, node in prepared.items() if n not in procs)),
        'windows': windows, 'preparation_failures': failures, 'sql_qps': 12})


try:
    targets = [16, 32, 64, 96, 128, 160, 192, 224, 256, *range(264, 321, 8), *range(324, 401, 4)]
    if resume:
        placement = {r['instance_id']: r['root']['node_id'] for r in rows}
        event('simple_resume_start', phase=phase, prior_phase=prior_phase,
              retained_instances=resume['active_instances'],
              note='Separate measurement phase; reuse existing SDK sessions, no claim of uninterrupted SQL.')
        for n in resume['active_instances']:
            check()
            node = placement[configs[n]['id']]
            if node not in lanes:
                raise RuntimeError('Retained instance has no observed data-node placement: ' + str(n))
            procs[n] = subprocess.Popen([sys.executable, str(script), 'run', str(n), phase, '14400', '12', '--reuse-retained'], cwd=ROOT)
            nodes[n] = node
            wait(.5)
        save()
        highest = max(collections.Counter(nodes.values()).values())
        first_target = min(400, ((highest + 3)//4)*4)
        targets = list(range(first_target, 401, 4))
    for target_per_node in targets:
        while any(sum(v == node for v in nodes.values()) < target_per_node for node in lanes):
            for node in sorted(lanes):
                check()
                if sum(v == node for v in nodes.values()) >= target_per_node:
                    continue
                if not lanes[node] and not unassigned:
                    event('simple_owned_pool_exhausted', phase=phase, node=node,
                          density=sum(v == node for v in nodes.values()),
                          note='Owned pool bound is not a measured node capacity limit.')
                    raise RuntimeError('Additional owned instances needed on ' + node)
                n = (lanes[node] if lanes[node] else unassigned).pop(0)
                if n not in prepared:
                    result = subprocess.run([sys.executable, str(script), 'prepare', str(n)], cwd=ROOT)
                    if result.returncode:
                        failures.append({'instance': n, 'node': node, 'time': time.time(), 'exit_code': result.returncode})
                        save()
                        continue
                    config = configs[n]
                    response = call('GET', '/api/v1/list-instances?tenant_id=' + config['tenant_id'] + '&page=1&page_size=100', gaia=True)
                    actual = next(row['root']['node_id'] for row in response['items'] if row['instance_id'] == config['id'])
                    if actual not in lanes:
                        raise RuntimeError('Prepared instance placement not yet observed: ' + str(n))
                    prepared[n] = actual
                    event('simple_prepared_placement', phase=phase, instance=n, node=actual)
                if prepared[n] != node:
                    lanes[prepared[n]].append(n)
                    continue
                check()
                procs[n] = subprocess.Popen([sys.executable, str(script), 'run', str(n), phase, '14400', '12'], cwd=ROOT)
                nodes[n] = node
                wait(.5)
        save()
        total = len(procs)
        placement = stage.snapshot(phase + '-density' + str(total) + '-before')
        current = {r['instance_id']: r['root']['node_id'] for r in placement}
        for iid, n in owned.items():
            if n in procs:
                nodes[n] = current[iid]
        counts = dict(collections.Counter(nodes.values()))
        wait(120 if target_per_node == 16 else 30)
        start = time.time()
        duration = 300 if target_per_node in (16, 160, 400) else 60
        event('simple_measurement_start', phase=phase, node_counts=counts, start=start, seconds=duration)
        wait(duration)
        finish = time.time()
        windows.append({'target': total, 'start': start, 'end': finish,
            'instances': list(procs), 'node_counts': counts, 'complete': True,
            'workload': 'simple_sql'})
        save()
        event('simple_measurement_complete', phase=phase, node_counts=counts, start=start, end=finish)
        print(json.dumps({'completed_node_densities': counts}), flush=True)
    event('simple_configured_pool_bound', phase=phase, node_counts=counts,
          note='Reached current400-Agent-per-node configuration; this alone is not a measured hardware capacity limit.')
except Exception as exc:
    event('simple_density_interrupted', phase=phase, error=str(exc), active_instances=list(procs))
    save()
    raise
finally:
    end_file.write_text('Finish SQL and retain successful sessions; resource STOP overrides retention.\n')
    for proc in procs.values():
        try:
            proc.wait(timeout=50)
        except subprocess.TimeoutExpired:
            event('simple_worker_exit_pending', phase=phase, pid=proc.pid)
    event('simple_density_ended', phase=phase, exit_codes={n: p.poll() for n, p in procs.items()})
