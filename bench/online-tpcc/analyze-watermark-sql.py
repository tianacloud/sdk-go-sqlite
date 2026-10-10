"""Verify SQL activity in the30 seconds preceding a selected watermark."""
import sys
from common import *
source = sys.argv[1]
summary = json.loads((EVIDENCE/'watermark85-per-node-summary.json').read_text())
watermark = next(r for r in summary['samples'] if r['trigger_source'] == source)
end = watermark['time']; start = end - 30
rows = []
for worker in watermark['workers']:
    path = EVIDENCE/f"sql-{worker['instance']:03d}-{worker['phase']}.jsonl"
    size = path.stat().st_size
    length = min(size, 512 * 1024)
    while True:
        offset = max(0, size - length)
        with path.open('rb') as stream:
            stream.seek(offset)
            if offset: stream.readline()
            lines = stream.read(size - stream.tell()).splitlines()
        parsed = []
        for line in lines:
            try: parsed.append(json.loads(line))
            except json.JSONDecodeError: continue
        if not offset or (parsed and parsed[0]['time'] <= start): break
        length = min(size, length * 2)
    selected = [r for r in parsed if start <= r['time'] < end and not r.get('cold_connection')]
    rows.append(dict(worker, hot_sql_count=len(selected),
        hot_sql_qps=len(selected)/30,
        sql_errors=sum(bool(r.get('error')) for r in selected),
        source_covers_start=bool(parsed and parsed[0]['time'] <= start),
        first_sql_time=selected[0]['time'] if selected else None,
        last_sql_time=selected[-1]['time'] if selected else None))
record(source.removesuffix('-trigger.json')+'-sql-activity', {
    'time':time.time(),'trigger_source':source,'start':start,'end':end,
    'node':watermark['node'],'resource_value':watermark['value'],
    'worker_estimate':len(rows),'workers_with_hot_sql':sum(r['hot_sql_count']>0 for r in rows),
    'minimum_hot_sql_qps':min((r['hot_sql_qps'] for r in rows),default=None),
    'sql_errors':sum(r['sql_errors'] for r in rows),'workers':rows,
    'caveat':'Preceding30s SQL activity, not a common scheduled steady-state measurement; recently started workers may have shorter coverage.'})
print('node',watermark['node'],'workers',len(rows),'with SQL',sum(r['hot_sql_count']>0 for r in rows),
      'min QPS',min((r['hot_sql_qps'] for r in rows),default=None),'errors',sum(r['sql_errors'] for r in rows))
