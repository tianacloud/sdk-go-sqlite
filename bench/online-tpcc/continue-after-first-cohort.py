"""Continue only after the current complete cohort exits successfully without STOP."""
import subprocess
import sys
import time
from common import *

pid = 418287
save_private('tpcc-continuation-waiter.json', {'pid': os.getpid(), 'prior_coordinator': pid, 'time': time.time()})
while True:
    if (ROOT/'.work/STOP').exists():
        record('tpcc-continuation-decision', {'time': time.time(), 'launched': False, 'reason': 'resource STOP; retain boundary evidence and wait for recovery'})
        sys.exit(0)
    command = pathlib.Path(f'/proc/{pid}/cmdline')
    if not command.exists() or b'gradual-density.py' not in command.read_bytes():
        break
    time.sleep(2)
progress = json.loads((EVIDENCE/'gradual-density-r1-progress.json').read_text())
if not progress['windows'] or progress['windows'][-1]['target'] != 400 or not progress['windows'][-1]['complete']:
    raise RuntimeError('Prior cohort ended without its final completed measurement; inspect before continuing')
if (ROOT/'.work/STOP').exists():
    raise RuntimeError('resource STOP')
phase = 'gradual-density-r2'
record('tpcc-continuation-decision', {'time': time.time(), 'launched': True, 'phase': phase,
    'previous_node_counts': progress['windows'][-1]['node_counts'], 'goal_instances_per_node': 400})
event('tpcc_continue_beyond_first_cohort', phase=phase, previous_node_counts=progress['windows'][-1]['node_counts'])
with (PRIVATE/(phase+'.log')).open('a') as output:
    proc = subprocess.Popen([sys.executable, str(ROOT/'bench/online-tpcc/extend-density.py'), phase],
                            cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)
    save_private(phase+'-process.json', {'pid': proc.pid, 'time': time.time(), 'phase': phase})
    result = proc.wait()
record('tpcc-continuation-exit', {'time': time.time(), 'phase': phase, 'exit_code': result})
sys.exit(result)
