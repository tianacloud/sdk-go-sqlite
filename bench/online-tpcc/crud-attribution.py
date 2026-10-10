"""Record container and host CPU evidence at a completed window midpoint."""
import sys
from common import *
sys.path.insert(0, str(pathlib.Path.home()/'.codex/skills/tiana-debug/scripts'))
from tiana_debug import Backend

phase, density = sys.argv[1], int(sys.argv[2])
progress = json.loads((EVIDENCE/(phase+'-progress.json')).read_text())
window = next(w for w in progress['windows'] if w.get('complete') and
              all(n == density for n in w['node_counts'].values()))
stamp = (window['start']+window['end'])/2
queries = {
    'container_cpu': 'sum by(instance,node,node_id,container)(rate(container_cpu_usage_seconds_total{container!="",container!="POD"}[1m]))',
    'container_memory': 'sum by(instance,node,node_id,container)(container_memory_working_set_bytes{container!="",container!="POD"})',
    'cpu_modes': '100*avg by(instance,node_id,mode)(rate(node_cpu_seconds_total[1m]))',
    'agent_series_count': 'count by(node)(count by(node,pod)(container_memory_working_set_bytes{container="agent"}))',
}
backend = Backend(PROFILE)
responses = {}
prefix = phase+'-node'+str(density)+'-attribution'
for name, query in queries.items():
    params = {'query': query, 'time': stamp}
    response = backend.get('prometheus', '/api/v1/query', params)
    record(prefix+'-'+name, {'params': params, 'response': response,
                           'replica_identity': 'UNKNOWN'})
    assert response.get('status') == 'success', name
    responses[name] = response['data']['result']

observations = []
for node, container_node in [('n10-81-32-164-30557851a13e4057', 'node-10-81-32-164'),
                             ('n10-81-33-145-56704fa60f09487b', 'node-10-81-33-145')]:
    def container_value(signal, container):
        values = [float(r['value'][1]) for r in responses[signal]
                  if r['metric'].get('node') == container_node and
                  r['metric'].get('container') == container]
        return sum(values) if values else None
    observations.append({'node': node, 'density': density, 'timestamp': stamp,
        'agent_app_cpu_cores': container_value('container_cpu', 'agent'),
        'collector_cpu_cores': container_value('container_cpu', 'tiana-collector'),
        'runtime_cpu_cores': container_value('container_cpu', 'runtime'),
        'agent_app_memory_bytes': container_value('container_memory', 'agent'),
        'collector_memory_bytes': container_value('container_memory', 'tiana-collector'),
        'cpu_modes_percent': {r['metric']['mode']: float(r['value'][1])
            for r in responses['cpu_modes'] if r['metric'].get('node_id') == node}})
record(prefix+'-summary', {'observations': observations, 'limitations': [
    'One midpoint sample with 1m CPU rates, not window peaks.',
    'Agent container includes App; no separation of idle Agent from SQL/FS work.',
    'Host non-idle includes I/O wait; container CPU cores are a different measure.',
    'Missing container series remain null. CPU mode residual is not attributed.',
    'Prometheus replica identity UNKNOWN.']})
print(json.dumps(observations))
