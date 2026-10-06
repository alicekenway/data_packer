import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import textwrap
import time

import pytest

from test_local import CLI, run


@pytest.fixture
def scheduler(tmp_path):
    """Run generated job scripts for real; only the external scheduler is simulated."""
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    script = '''
import json, os, pathlib, subprocess, sys, time
name = pathlib.Path(sys.argv[0]).name
state = pathlib.Path(os.environ['FAKE_SLURM'])
if name == 'scontrol':
    print('MaxArraySize = 2')
    sys.exit(0)
if name == 'squeue':
    if os.environ.get('FAKE_QUEUE_FAIL'):
        print('queue query unavailable', file=sys.stderr)
        sys.exit(1)
    if os.environ.get('FAKE_JOB_FILTER_INVALID') and any(arg.startswith('--jobs') for arg in sys.argv):
        print('Invalid job id specified', file=sys.stderr)
        sys.exit(1)
    if os.environ.get('FAKE_ACTIVE'):
        print('1001\\n1002')
    sys.exit(0)
if name == 'scancel':
    with (state / 'cancelled').open('a') as f:
        f.write(' '.join(sys.argv[1:]) + '\\n')
    sys.exit(0)
if name != 'sbatch':
    sys.exit(2)
records = json.loads((state / 'calls.json').read_text()) if (state / 'calls.json').exists() else []
args = sys.argv[1:]
records.append(args)
(state / 'calls.json').write_text(json.dumps(records))
job_id = 1000 + len(records)
if os.environ.get('FAKE_REJECT') == str(len(records)):
    print('submission rejected', file=sys.stderr)
    sys.exit(1)
array = next((arg.split('=', 1)[1] for arg in args if arg.startswith('--array=')), None)
print(str(job_id), flush=True)
if '--wait' in args and os.environ.get('FAKE_WAIT_GATE'):
    while not pathlib.Path(os.environ['FAKE_WAIT_GATE']).exists():
        time.sleep(0.02)
indices = range(int(array.split('%')[0].split('-')[-1]) + 1) if array else [None]
script = args[-1]
exit_code = 0
for index in indices:
    env = dict(os.environ, SLURM_JOB_ID=str(job_id))
    if index is not None:
        env['SLURM_ARRAY_TASK_ID'] = str(index)
    # Dropped worker simulates a node failure, without a completion receipt.
    if os.environ.get('FAKE_DROP_WORKER') and array:
        continue
    if os.environ.get('FAKE_DROP_FINALIZER') and not array:
        exit_code = 1
        continue
    output = subprocess.run(['/bin/bash', script], capture_output=True, text=True, env=env)
    exit_code = max(exit_code, output.returncode)
    with (state / 'job-output.log').open('a') as f:
        f.write(output.stdout + output.stderr)
if '--wait' in args:
    sys.exit(int(os.environ.get('FAKE_WAIT_EXIT', exit_code)))
'''
    for name in ('sbatch', 'squeue', 'scontrol', 'scancel'):
        path = bin_dir / name
        path.write_text('#!' + sys.executable + '\n' + textwrap.dedent(script))
        path.chmod(0o755)
    env = dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ['PATH'], FAKE_SLURM=str(tmp_path))
    return tmp_path, env


def make_source(path, count=5):
    path.mkdir()
    for i in range(count):
        (path / str(i)).write_bytes(bytes([i]) * 100)


def test_slurm_wait_splits_arrays_and_runs_the_real_workers(tmp_path, scheduler):
    state, env = scheduler
    source, packed = tmp_path / 'source', tmp_path / 'packed'
    make_source(source)
    result = run('pack', '--input', source, '--output', packed, '--parts', 5,
                 '--mode', 'slurm', '--concurrency', 3, '--partition', 'cpu.only',
                 '--mem', '2G', '--time', '01:00:00', '--cpus-per-task', 2,
                 '--account', 'science', '--qos', 'normal',
                 '--sbatch-args=--constraint=test', '--wait', '--poll-interval', 1, env=env)
    calls = json.loads((state / 'calls.json').read_text())
    arrays = [call for call in calls if any(arg.startswith('--array=') for arg in call)]
    assert len(arrays) == 3
    for call in arrays:
        assert '--partition=cpu.only' in call
        assert '--mem=2G' in call
        assert '--cpus-per-task=2' in call
        assert '--constraint=test' in call
        array = next(arg for arg in call if arg.startswith('--array='))
        assert int(array.split('%')[0].split('-')[-1]) <= 1
    dependency = next(arg for arg in calls[-1] if arg.startswith('--dependency='))
    assert dependency == '--dependency=afterany:1001:1002:1003'
    assert '--wait' in calls[-1]
    assert all('--wait' not in call for call in arrays)
    assert '1001' in result.stdout
    assert json.loads((packed / 'manifest.json').read_text())['state'] == 'complete'
    destination = tmp_path / 'restored'
    run('unpack', '--input', packed, '--output', destination, '--mode', 'slurm',
        '--wait', '--poll-interval', 1, env=env)
    assert [(destination / str(i)).read_bytes() for i in range(5)] == [bytes([i]) * 100 for i in range(5)]


def test_without_wait_returns_submission_information(tmp_path, scheduler):
    state, env = scheduler
    source, packed = tmp_path / 'source', tmp_path / 'packed'
    make_source(source, 1)
    result = run('pack', '--input', source, '--output', packed, '--mode', 'slurm', env=env)
    submission = json.loads((packed / '.work/submission.json').read_text())
    assert submission['array_jobs'] == ['1001']
    assert submission['finalizer_job'] == '1002'
    assert '1002' in result.stdout
    assert all('--wait' not in call for call in json.loads((state / 'calls.json').read_text()))


def test_wait_uses_sbatch_without_polling_queue(tmp_path, scheduler):
    state, env = scheduler
    env['FAKE_QUEUE_FAIL'] = '1'
    source, packed = tmp_path / 'source', tmp_path / 'packed'
    make_source(source, 1)
    run('pack', '--input', source, '--output', packed, '--mode', 'slurm', '--wait', env=env)


def test_wait_preserves_native_failure_exit_code_and_accepted_job_id(tmp_path, scheduler):
    state, env = scheduler
    env['FAKE_WAIT_EXIT'] = '7'
    source, packed = tmp_path / 'source', tmp_path / 'packed'
    make_source(source, 1)
    result = run('pack', '--input', source, '--output', packed, '--mode', 'slurm', '--wait', env=env, ok=False)
    assert result.returncode == 7
    submission = json.loads((packed / '.work/submission.json').read_text())
    assert submission['finalizer_job'] == '1002'
    assert submission['state'] != 'submission_failed'
    assert not (state / 'cancelled').exists()


def test_waited_worker_failure_is_not_mistaken_for_submission_rejection(tmp_path, scheduler):
    state, env = scheduler
    env['FAKE_DROP_WORKER'] = '1'
    source, packed = tmp_path / 'source', tmp_path / 'packed'
    make_source(source, 1)
    run('pack', '--input', source, '--output', packed, '--mode', 'slurm', '--wait', env=env, ok=False)
    submission = json.loads((packed / '.work/submission.json').read_text())
    assert submission['finalizer_job'] == '1002'
    assert submission['state'] != 'submission_failed'
    assert not (state / 'cancelled').exists()


def test_waited_finalizer_failure_recovers_summary_but_returns_failure(tmp_path, scheduler):
    state, env = scheduler
    env['FAKE_DROP_FINALIZER'] = '1'
    env['FAKE_WAIT_EXIT'] = '7'
    source, packed = tmp_path / 'source', tmp_path / 'packed'
    make_source(source, 1)
    result = run('pack', '--input', source, '--output', packed, '--mode', 'slurm', '--wait', env=env, ok=False)
    assert result.returncode == 7
    assert (packed / '.work/summary.json').exists()
    assert not (state / 'cancelled').exists()


def test_canceled_finalizer_does_not_finalize_still_active_arrays(tmp_path, scheduler):
    state, env = scheduler
    env['FAKE_DROP_FINALIZER'] = '1'
    env['FAKE_ACTIVE'] = '1'
    source, packed = tmp_path / 'source', tmp_path / 'packed'
    make_source(source, 1)
    run('pack', '--input', source, '--output', packed, '--mode', 'slurm', '--wait', env=env, ok=False)
    assert not (packed / '.work/summary.json').exists()
    assert json.loads((packed / 'manifest.json').read_text())['state'] == 'running'
    assert not (state / 'cancelled').exists()


@pytest.mark.parametrize('interrupt', [False, True])
def test_native_wait_records_job_id_before_blocking(tmp_path, scheduler, interrupt):
    state, env = scheduler
    gate = tmp_path / 'release-wait'
    env['FAKE_WAIT_GATE'] = str(gate)
    source, packed = tmp_path / 'source', tmp_path / 'packed'
    make_source(source, 1)
    with (tmp_path / 'client.log').open('w') as log:
        client = subprocess.Popen([sys.executable, str(CLI), 'pack', '--input', str(source),
                                   '--output', str(packed), '--mode', 'slurm', '--wait'],
                                  env=env, stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 5
            submission = None
            while time.monotonic() < deadline and client.poll() is None:
                path = packed / '.work/submission.json'
                if path.exists():
                    submission = json.loads(path.read_text())
                    if submission.get('state') == 'waiting':
                        break
                time.sleep(0.02)
            assert submission and submission['state'] == 'waiting'
            assert submission['finalizer_job'] == '1002'
            assert client.poll() is None
            if interrupt:
                client.send_signal(signal.SIGINT)
            else:
                gate.touch()
            assert client.wait(timeout=5) == (130 if interrupt else 0)
            assert not (state / 'cancelled').exists()
        finally:
            gate.touch()
            if client.poll() is None:
                client.kill()
                client.wait()


def test_wait_detects_worker_without_receipt(tmp_path, scheduler):
    state, env = scheduler
    env['FAKE_DROP_WORKER'] = '1'
    source, packed = tmp_path / 'source', tmp_path / 'packed'
    make_source(source, 1)
    run('pack', '--input', source, '--output', packed, '--mode', 'slurm', '--wait', env=env, ok=False)
    assert json.loads((packed / 'manifest.json').read_text())['state'] == 'failed'
    result = run('status', '--input', packed, env=env)
    assert json.loads(result.stdout)['archives']['failed'] == 1


def test_submission_failure_cancels_already_submitted_jobs(tmp_path, scheduler):
    state, env = scheduler
    env['FAKE_REJECT'] = '2'
    source, packed = tmp_path / 'source', tmp_path / 'packed'
    make_source(source, 3)
    run('pack', '--input', source, '--output', packed, '--parts', 3, '--mode', 'slurm', env=env, ok=False)
    assert (state / 'cancelled').read_text().strip() == '1001'


def test_destination_ownership_tracks_async_jobs_across_state_directories(tmp_path, scheduler):
    state, env = scheduler
    source, packed = tmp_path / 'source', tmp_path / 'packed'
    make_source(source, 1)
    run('pack', '--input', source, '--output', packed)
    destination = tmp_path / 'restored'
    run('unpack', '--input', packed, '--output', destination, '--mode', 'slurm',
        '--state-dir', tmp_path / 'state1', env=env)
    env['FAKE_ACTIVE'] = '1'
    run('unpack', '--input', packed, '--output', destination, '--mode', 'slurm',
        '--state-dir', tmp_path / 'state2', '--overwrite', env=env, ok=False)
    assert len(json.loads((state / 'calls.json').read_text())) == 2


def test_wait_and_resume_allow_completed_ids_purged_from_queue(tmp_path, scheduler):
    state, env = scheduler
    env['FAKE_JOB_FILTER_INVALID'] = '1'
    source, packed = tmp_path / 'source', tmp_path / 'packed'
    make_source(source, 1)
    run('pack', '--input', source, '--output', packed, '--mode', 'slurm', '--wait', env=env)
    run('pack', '--input', source, '--output', packed, '--resume', env=env)


@pytest.mark.parametrize('extra', ['--array=0-9', '--output=x', '-p other', '--partition=other', '--wrap=whoami', '--wait', 'other-script.sh'])
def test_rejects_conflicting_or_managed_sbatch_options(tmp_path, scheduler, extra):
    state, env = scheduler
    source, packed = tmp_path / 'source', tmp_path / 'packed'
    make_source(source, 1)
    run('pack', '--input', source, '--output', packed, '--mode', 'slurm',
        '--sbatch-args=' + extra, env=env, ok=False)
    assert not (state / 'calls.json').exists()
    assert not list(packed.glob('*.tar'))
