"""Apply the user-requested 400 Agent pool through Gaia while SQL continues."""
import os, signal, sys, time, urllib.request
from common import *

phase = 'gradual-density-r1'
pid = 418287
nodes = call('GET', '/api/v1/nodes', gaia=True)
record('agent-pool400-before-nodes', nodes)
started = time.time()
record('agent-pool400-change', {'start': started, 'status': 'RUNNING',
    'original_count_per_node': 600, 'target_count_per_node': 400,
    'user_requested': True, 'foreground_sql_kept_running': True})
headers = {'Content-Type': 'application/json', 'Host': PROFILE['gaia_host'],
    'Authorization': 'Bearer ' + os.environ['GAIA_TOKEN'], 'X-Gaia-Token-Access': '1',
    'X-Tiana-Cluster': CLUSTER, 'X-Tiana-Operator': 'online-tpcc-20261010'}
operations = []
try:
    for node in sorted(nodes, key=lambda n: n['ID']):
        for count in [500, 400]:
            if (ROOT/'.work/STOP').exists():
                raise RuntimeError('resource STOP; no further resize submitted')
            nid = node['ID']
            key = 'online-tpcc-20261010-pool-' + nid + '-' + str(count)
            headers.update({'Idempotency-Key': key, 'X-Request-ID': str(uuid.uuid4())})
            path = '/api/v1/nodes/' + nid + '/agent-pool'
            body = {'agent_image_ref': node['Runtime']['AgentImageRef'], 'desired_agent_count': count}
            event('agent_pool_resize_submit', node=nid, target=count, request_id=headers['X-Request-ID'])
            req = urllib.request.Request(PROFILE['gaia_origin'] + path,
                data=json.dumps(body).encode(), headers=headers, method='POST')
            with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req, timeout=40) as response:
                result = json.load(response)
            record('agent-pool-' + nid + '-' + str(count) + '-submitted', result)
            oid = result['operation_id']
            operations.append({'node': nid, 'target': count, 'operation_id': oid})
            while True:
                op = call('GET', '/api/v1/platform/deployment-operations/' + oid, gaia=True)
                record('agent-pool-' + nid + '-' + str(count) + '-operation', op)
                if op['state'] == 'SUCCEEDED':
                    break
                if op['state'] in ['FAILED', 'CANCELED', 'CANCELLED', 'PAUSED']:
                    raise RuntimeError('Resize operation state ' + op['state'])
                time.sleep(3)
            current = call('GET', '/api/v1/nodes', gaia=True)
            record('agent-pool-' + nid + '-' + str(count) + '-nodes', current)
            got = next(n for n in current if n['ID'] == nid)['Control']['agent_pool']
            assert got['desired_agent_count'] == count and got['configured_agent_count'] == count and got['state'] == 'READY', got
            event('agent_pool_resize_verified', node=nid, target=count, pool=got)
            print('pool verified', nid, count, 'active', got['active_agent_count'], 'parked', got['parked_agent_count'], flush=True)
            time.sleep(15)
    finish = time.time()
    record('agent-pool400-change', {'start': started, 'end': finish, 'status': 'VERIFIED',
        'original_count_per_node': 600, 'target_count_per_node': 400,
        'operations': operations, 'foreground_sql_kept_running': True})
    event('agent_pool400_complete', start=started, end=finish)
finally:
    assert b'gradual-density.py' in pathlib.Path(f'/proc/{pid}/cmdline').read_bytes()
    os.kill(pid, signal.SIGCONT)
    event('agent_pool_resize_coordinator_resumed', phase=phase, coordinator_pid=pid)
