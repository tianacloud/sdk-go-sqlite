"""Point SELECT and single-row autocommit INSERT on retained test instances."""
import random
import sys
import time
from workload import Client, Stopped, STOP
from common import ROOT, event

TABLE = 'benchmark_simple_density'
PAYLOAD = 'x' * 128


def prepare(n):
    client = Client(n, 'simple-sql-prepare', 12)
    completed = False
    try:
        client.execute('SELECT 1')
        client.execute(f'CREATE TABLE IF NOT EXISTS {TABLE} (id INTEGER PRIMARY KEY, payload TEXT)')
        values = ','.join(f"({i},'{PAYLOAD}')" for i in range(1, 1001))
        client.execute(f'INSERT OR IGNORE INTO {TABLE} (id,payload) VALUES {values}')
        client.execute(f'SELECT payload FROM {TABLE} WHERE id=1000')
        assert client.fetchone() == (PAYLOAD,)
        completed = True
        event('simple_sql_prepared', instance=n, seed_rows=1000, payload_bytes=128)
    finally:
        client.finish(completed)


def run(n, phase, seconds, qps, reuse_retained=False):
    client = Client(n, phase, qps, reuse_retained=reuse_retained)
    rng = random.Random(20261010 + n)
    end = time.monotonic() + seconds
    completed = False
    counts = {'SELECT': 0, 'INSERT': 0}
    event('workload_start', instance=n, phase=phase, workload='simple_sql',
          seconds=seconds, qps=qps, select_insert_ratio='1:1', payload_bytes=128,
          select_key_range=[1, 1000], insert_commit='autocommit')
    try:
        client.execute('SELECT 1')
        while time.monotonic() < end and not (ROOT / '.work' / ('END-' + phase)).exists():
            client.txn = 'POINT_SELECT'
            client.execute(f'SELECT payload FROM {TABLE} WHERE id=?', [rng.randint(1, 1000)])
            assert client.fetchone() == (PAYLOAD,)
            counts['SELECT'] += 1
            client.txn = 'AUTOCOMMIT_INSERT'
            client.execute(f'INSERT INTO {TABLE} (payload) VALUES (?)', [PAYLOAD])
            counts['INSERT'] += 1
        completed = True
        event('workload_complete', instance=n, phase=phase, counts=counts, sql_count=client.seq)
    except Exception as exc:
        event('simple_sql_interrupted', instance=n, phase=phase, error=str(exc), counts=counts,
              action='no replay of failed or outcome-unknown SQL')
        raise
    finally:
        client.finish(completed)


if __name__ == '__main__':
    if sys.argv[1] == 'prepare':
        prepare(int(sys.argv[2]))
    else:
        run(int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), float(sys.argv[5]), '--reuse-retained' in sys.argv[6:])
