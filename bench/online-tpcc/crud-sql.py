"""50% reads and50% writes point CRUD; bounded rows, random order, single-statement autocommit."""
import random
import sys
import time
from workload import Client
from common import ROOT, event

TABLE = 'benchmark_crud_density_r3'
PAYLOAD = 'x' * 128

def prepare(n):
    client = Client(n, 'crud-sql-prepare-r3', 12)
    completed = False
    try:
        client.execute('SELECT 1')
        client.execute(f'CREATE TABLE {TABLE} (id INTEGER PRIMARY KEY, payload TEXT)')
        values = ','.join(f"({i},'{PAYLOAD}')" for i in range(1, 1002))
        client.execute(f'INSERT INTO {TABLE} (id,payload) VALUES {values}')
        client.execute(f'SELECT payload FROM {TABLE} WHERE id=1001')
        assert client.fetchone() == (PAYLOAD,)
        completed = True
        event('crud_sql_prepared', instance=n, seed_rows=1001, payload_bytes=128)
    finally:
        client.finish(completed)

def cycle(client, rng, previous_id, next_id, counts):
    operations = ['SELECT', 'SELECT', 'SELECT', 'INSERT', 'UPDATE', 'DELETE']
    rng.shuffle(operations)
    for operation in operations:
        client.txn = 'CRUD_' + operation
        if operation == 'SELECT':
            client.execute(f'SELECT payload FROM {TABLE} WHERE id=?', [rng.randint(1, 1000)])
            row = client.fetchone()
            assert row is not None and len(row[0]) == 128
        elif operation == 'INSERT':
            client.execute(f'INSERT INTO {TABLE} (id,payload) VALUES (?,?)', [next_id, PAYLOAD])
        elif operation == 'UPDATE':
            client.execute(f'UPDATE {TABLE} SET payload=? WHERE id=?', [str(next_id).ljust(128, 'y'), rng.randint(1, 1000)])
        else:
            client.execute(f'DELETE FROM {TABLE} WHERE id=?', [previous_id])
        counts[operation] += 1

def run(n, phase, seconds, qps):
    client = Client(n, phase, qps)
    rng = random.Random(20261010 + n)
    end = time.monotonic() + seconds
    completed = False
    counts = dict.fromkeys(['SELECT','INSERT','UPDATE','DELETE'], 0)
    previous_id, next_id = 1001, 1002
    event('workload_start', instance=n, phase=phase, workload='simple_crud', seconds=seconds,
          qps=qps, read_write_ratio='1:1', operation_ratio='3:1:1:1', order='random permutation each six statements',
          payload_bytes=128, select_update_key_range=[1,1000], autocommit=True)
    try:
        client.execute('SELECT 1')
        while time.monotonic() < end and not (ROOT / '.work' / ('END-' + phase)).exists():
            cycle(client, rng, previous_id, next_id, counts)
            previous_id, next_id = next_id, next_id + 1
        completed = True
        event('workload_complete', instance=n, phase=phase, counts=counts, sql_count=client.seq)
    except Exception as exc:
        event('crud_sql_interrupted', instance=n, phase=phase, error=str(exc), counts=counts,
              action='no replay of failed or outcome-unknown SQL')
        raise
    finally:
        client.finish(completed)

if __name__ == '__main__':
    if sys.argv[1] == 'prepare':
        prepare(int(sys.argv[2]))
    else:
        run(int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), float(sys.argv[5]))
