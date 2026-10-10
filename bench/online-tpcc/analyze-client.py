"""Summarize load-generator resource samples over a completed measurement window."""
import sys
from common import *

start, end = map(float, sys.argv[1:3])
phase = sys.argv[3]
samples = []
with (EVIDENCE / 'client-resources.jsonl').open() as source:
    for line in source:
        row = json.loads(line)
        if start <= row['time'] <= end:
            samples.append(row)
intervals = []
for before, after in zip(samples, samples[1:]):
    elapsed = after['time'] - before['time']
    old = {p['pid']: p for p in before['processes']}
    cpu_ticks = sum(p['user_ticks'] + p['system_ticks'] - old[p['pid']]['user_ticks'] - old[p['pid']]['system_ticks']
                    for p in after['processes'] if p['pid'] in old)
    host_before = list(map(int, before['cpu_ticks'].split()[1:9]))
    host_after = list(map(int, after['cpu_ticks'].split()[1:9]))
    deltas = [b - a for a, b in zip(host_before, host_after)]
    total = sum(deltas)
    intervals.append({'start': before['time'], 'end': after['time'],
        'observed_test_process_cpu_cores': cpu_ticks / after['clock_ticks'] / elapsed,
        'host_busy_cpu_percent_excluding_iowait': 100 * (total - deltas[3] - deltas[4]) / total if total else None})
summary = {'phase': phase, 'start': start, 'end': end, 'samples': len(samples),
    'logical_cpu_count': samples[-1]['logical_cpu_count'] if samples else None,
    'max_observed_test_process_cpu_cores': max((x['observed_test_process_cpu_cores'] for x in intervals), default=None),
    'max_host_busy_cpu_percent_excluding_iowait': max((x['host_busy_cpu_percent_excluding_iowait'] for x in intervals if x['host_busy_cpu_percent_excluding_iowait'] is not None), default=None),
    'max_test_process_rss_sum_bytes': max((sum(p['rss_pages'] for p in x['processes']) * x['page_size'] for x in samples), default=None),
    'min_host_available_memory_kib': min((int(x['meminfo']['MemAvailable'].split()[0]) for x in samples), default=None),
    'max_1min_load': max((x['loadavg'][0] for x in samples), default=None),
    'intervals': intervals,
    'limitations': 'Process CPU deltas include only PIDs present in both adjacent samples; short-lived processes may be missed. RSS sum can double-count shared memory. Host CPU includes unrelated workstation activity. Raw samples are retained in client-resources.jsonl.'}
record('client-resources-' + phase, summary)
print(json.dumps({k: v for k, v in summary.items() if k != 'intervals'}, ensure_ascii=False))
