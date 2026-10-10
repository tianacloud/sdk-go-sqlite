"""Count actual outer JSON/logfmt severity, excluding severity inside query strings."""
import collections
import shlex
import sys
from common import *

phase = sys.argv[1]
start = json.loads((EVIDENCE/(phase+'-coordinator-process.json')).read_text())['start']
counts = collections.Counter()
queries = truncated = parse_failures = 0
latest = None
with (EVIDENCE/'failure-logs-live.jsonl').open() as source:
    for line in source:
        query = json.loads(line)
        if int(query['params']['end'])/1e9 < start:
            continue
        queries += 1
        truncated += bool(query.get('possibly_truncated'))
        latest = int(query['params']['end'])/1e9
        for stream in query['response'].get('data', {}).get('result', []):
            for timestamp, message in stream['values']:
                if int(timestamp)/1e9 < start:
                    continue
                try:
                    fields = json.loads(message)
                except ValueError:
                    try:
                        fields = dict(token.split('=', 1) for token in shlex.split(message)
                                      if '=' in token)
                    except ValueError:
                        parse_failures += 1
                        continue
                level = str(fields.get('level', fields.get('severity', ''))).lower()
                if level not in ('error', 'warn', 'warning'):
                    continue
                component = stream['stream'].get('k8s_container_name',
                    fields.get('component', stream['stream'].get('service_name', 'UNKNOWN')))
                msg = fields.get('msg', fields.get('message', fields.get('event', '')))
                counts[(component, level, str(msg))] += 1

out = {'phase': phase, 'requested_start': start, 'latest_queried_end': latest,
    'query_windows': queries, 'possibly_truncated_windows': truncated,
    'parse_failures': parse_failures,
    'parsed_error_warning_counts': [{'component': k[0], 'level': k[1],
        'message': k[2], 'count': v} for k, v in counts.most_common()],
    'interpretation': 'Actual outer JSON/logfmt severity. Broad log observation only; failed/truncated queries and unparsed logs remain gaps. Not every observed event is caused by this benchmark.'}
record(phase+'-log-audit', out)
print(json.dumps({k: v for k, v in out.items() if k != 'parsed_error_warning_counts'}))
print(json.dumps(out['parsed_error_warning_counts'][:12]))
