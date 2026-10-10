"""Retain FS periodic statistics, including a backfill for the simple SQL phase."""
import re
import sys
from common import *
sys.path.insert(0, str(pathlib.Path.home() / '.codex/skills/tiana-debug/scripts'))
from tiana_debug import Backend, clean_fields

backend = Backend(PROFILE)
start = 1791606189.0
query = '{environment="online"} | k8s_container_name="agent" |= "online-shanghai-control-01" |= "event=storage.stats"'

def redact(value):
    if isinstance(value, str):
        return re.sub(r'https?://[^\s"<>]+', '[REDACTED_URL]', value)
    if isinstance(value, list):
        return [redact(v) for v in value]
    if isinstance(value, dict):
        return {k: redact(v) for k, v in value.items()}
    return value

while True:
    until = min(time.time() - 10, start + 60)
    if until <= start:
        time.sleep(5)
        continue
    params = {'query': query, 'start': int(start * 1e9), 'end': int(until * 1e9),
              'limit': 2000, 'direction': 'forward'}
    try:
        response = redact(clean_fields(backend.get('loki', '/loki/api/v1/query_range', params)))
        count = sum(len(s['values']) for s in response.get('data', {}).get('result', []))
        with (EVIDENCE / 'fs-stats-logs-live.jsonl').open('a') as output:
            output.write(json.dumps({'queried_at': time.time(), 'params': params,
                'count': count, 'possibly_truncated': count >= 2000, 'response': response}) + '\n')
        if count >= 2000:
            event('fs_stats_log_window_may_be_truncated', start=start, end=until)
        start = until
        if start >= time.time() - 15:
            print('FS stats caught up', int(start), 'last_count', count, flush=True)
            time.sleep(30)
    except Exception as error:
        event('fs_stats_log_observation_error', start=start, end=until, error=str(error))
        time.sleep(5)
