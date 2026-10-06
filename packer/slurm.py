"""Slurm submission, array splitting, and completion monitoring."""
import errno
import os
from pathlib import Path
import pty
import re
import shlex
import shutil
import subprocess
import sys
import tempfile

from .archive import finalize
from .common import PackerError, atomic_json, ensure_directory, read_json


RESERVED = {'array', 'wrap', 'output', 'error', 'dependency', 'wait', 'parsable',
            'job-name', 'chdir', 'partition', 'mem', 'mem-per-cpu', 'mem-per-gpu',
            'time', 'cpus-per-task', 'account', 'qos', 'clusters', 'cluster',
            'export', 'export-file', 'help', 'usage', 'version', 'test-only'}


def extra_options(value):
    tokens = shlex.split(value)
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if not token.startswith('--') or token == '--':
            raise PackerError('--sbatch-args accepts long options only; use explicit resource flags for -p/-o/etc.')
        name = token[2:].split('=', 1)[0]
        # Disallow abbreviations too: sbatch may accept --out as --output.
        if any(option.startswith(name) for option in RESERVED):
            raise PackerError('managed or conflicting sbatch option: {}'.format(token))
        i += 1
        if '=' not in token and i < len(tokens) and not tokens[i].startswith('-'):
            i += 1
    return tokens


def validate_options(args):
    extra_options(args.sbatch_args)
    for name in ('sbatch', 'squeue', 'scancel'):
        if not shutil.which(name):
            raise PackerError('{} is required for Slurm mode'.format(name))
    if not args.max_array_size and not shutil.which('scontrol'):
        raise PackerError('scontrol is unavailable; provide --max-array-size')


def resource_options(args):
    options = ['--mem=' + args.mem, '--time=' + args.time,
               '--cpus-per-task=' + str(args.cpus_per_task)]
    for name in ('partition', 'account', 'qos'):
        value = getattr(args, name)
        if value:
            options.append('--{}={}'.format(name, value))
    return options + extra_options(args.sbatch_args)


def array_limit(args):
    if args.max_array_size:
        return args.max_array_size
    result = subprocess.run(['scontrol', 'show', 'config'], capture_output=True, text=True)
    match = re.search(r'\bMaxArraySize\s*=\s*(\d+)', result.stdout)
    if result.returncode or not match or int(match.group(1)) < 1:
        raise PackerError('cannot read MaxArraySize; provide --max-array-size explicitly')
    return int(match.group(1))


def active_jobs(ids):
    if not ids:
        return ''
    # Finished IDs can disappear from Slurm's job cache. Query the user's queue
    # instead of asking squeue to look up potentially purged individual IDs.
    result = subprocess.run(['squeue', '--noheader', '--user=' + str(os.getuid()), '--format=%i'],
                            capture_output=True, text=True)
    if result.returncode:
        raise PackerError('cannot query Slurm job completion: ' + result.stderr.strip())
    wanted = set(ids)
    matches = []
    for line in result.stdout.splitlines():
        match = re.match(r'\s*(\d+)', line)
        if match and match.group(1) in wanted:
            matches.append(line.strip())
    return '\n'.join(matches)


def check_inactive(work):
    submission = read_json(work / 'submission.json')
    ids = list(submission.get('array_jobs', []))
    if submission.get('finalizer_job'):
        ids.append(submission['finalizer_job'])
    if active_jobs(ids):
        raise PackerError('previous Slurm jobs are still active; wait for them or cancel them before retrying')


def script(path, command):
    path.write_text('#!/bin/bash\nset -euo pipefail\nexec ' + shlex.join(command) + '\n', encoding='utf-8')
    path.chmod(0o700)


def submit(options, path):
    command = ['sbatch', '--parsable'] + options + [str(path)]
    print('Submit: ' + shlex.join(command), flush=True)
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        raise PackerError('sbatch failed: ' + result.stderr.strip())
    output = result.stdout.strip()
    if not re.fullmatch(r'\d+(?:;[A-Za-z0-9_.-]+)?', output):
        raise PackerError('unexpected sbatch --parsable response: {!r}'.format(output))
    return output.split(';', 1)[0]


def submit_wait(options, path, on_submitted):
    """Use native sbatch waiting and record the accepted ID before it blocks.

    sbatch's C stdio may buffer the job ID when stdout is a pipe. A terminal
    makes its newline flush immediately, retaining ownership if waiting is
    interrupted. Only the sbatch client is terminated on interruption.
    """
    command = ['sbatch', '--parsable', '--wait'] + options + [str(path)]
    print('Submit: ' + shlex.join(command), flush=True)
    master, slave = pty.openpty()
    proc = None
    try:
        with tempfile.TemporaryFile() as errors:
            proc = subprocess.Popen(command, stdout=slave, stderr=errors,
                                    stdin=subprocess.DEVNULL, start_new_session=True)
            os.close(slave)
            slave = None
            response = b''
            while b'\n' not in response:
                try:
                    chunk = os.read(master, 4096)
                except OSError as exc:
                    if exc.errno != errno.EIO:
                        raise
                    break
                if not chunk:
                    break
                response += chunk
            output = response.split(b'\n', 1)[0].decode('utf-8', errors='replace').strip()
            if not re.fullmatch(r'\d+(?:;[A-Za-z0-9_.-]+)?', output):
                code = proc.wait()
                errors.seek(0)
                details = errors.read().decode('utf-8', errors='replace').strip()
                if code:
                    raise PackerError('sbatch submission failed: ' + details)
                raise PackerError('unexpected sbatch --parsable response: {!r}'.format(output))
            job_id = output.split(';', 1)[0]
            on_submitted(job_id)
            code = proc.wait()
            errors.seek(0)
            details = errors.read().decode('utf-8', errors='replace').strip()
            if details:
                print(details, file=sys.stderr, flush=True)
            return code
    finally:
        os.close(master)
        if slave is not None:
            os.close(slave)
        if proc is not None and proc.poll() is None:
            proc.terminate()
            proc.wait()


def waited_result(job_path, code, submission):
    summary_path = job_path.parent / 'summary.json'
    failure_code = code if code > 0 else 1
    if not summary_path.exists():
        # A dependency-waiting finalizer can be canceled before its arrays end.
        # Native waiting for that canceled job does not authorize local cleanup.
        try:
            if active_jobs(submission['array_jobs']):
                print('Finalizer ended without a summary; array jobs are still active. '
                      'Local finalization was not performed.', file=sys.stderr, flush=True)
                return failure_code
        except PackerError as exc:
            print('Cannot safely recover missing summary: ' + str(exc), file=sys.stderr, flush=True)
            return failure_code
        print('Finalizer did not produce a summary; checking receipts locally.', flush=True)
        try:
            finalize(job_path)
        except Exception as exc:
            print('Summary recovery failed: ' + str(exc), file=sys.stderr, flush=True)
        # Even if workers succeeded, a failed finalizer must be reported.
        print('Finalizer job did not finish successfully.', file=sys.stderr, flush=True)
        return failure_code
    try:
        summary = read_json(summary_path)
    except (OSError, ValueError) as exc:
        print('Could not read final summary: ' + str(exc), file=sys.stderr, flush=True)
        return failure_code
    print('{}: {}'.format(summary['operation'], summary['state']), flush=True)
    if code:
        print('sbatch --wait returned exit code {}'.format(code), file=sys.stderr, flush=True)
        return code if code > 0 else 1
    return 0 if summary['state'] == 'complete' else 1


def launch(job_path, shards, args):
    work = job_path.parent
    ensure_directory(work / 'logs')
    ensure_directory(work / 'scripts')
    limit = array_limit(args)
    options = resource_options(args)
    cli = Path(__file__).resolve().parents[1] / 'data_packer.py'
    command = [sys.executable, str(cli)]
    submission = dict(array_jobs=[], finalizer_job=None, state='submitting')
    atomic_json(work / 'submission.json', submission)
    try:
        previous = None
        for index, start in enumerate(range(0, len(shards), limit)):
            batch = shards[start:start + limit]
            mapping = work / 'scripts' / ('tasks-{}.json'.format(index))
            atomic_json(mapping, batch)
            path = work / 'scripts' / ('array-{}.sh'.format(index))
            script(path, command + ['_worker', '--job', str(job_path), '--shards-file', str(mapping)])
            array_options = options + ['--array=0-{}%{}'.format(len(batch) - 1, args.concurrency),
                                       '--job-name=data-packer',
                                       '--output=' + str(work / 'logs' / 'slurm-%A_%a.out'),
                                       '--error=' + str(work / 'logs' / 'slurm-%A_%a.err')]
            # Chain split arrays so the global concurrency limit is respected.
            if previous:
                array_options.append('--dependency=afterany:' + previous)
            previous = submit(array_options, path)
            submission['array_jobs'].append(previous)
            atomic_json(work / 'submission.json', submission)
        path = work / 'scripts' / 'finalize.sh'
        script(path, command + ['_finalize', '--job', str(job_path)])
        final_options = options + [
            '--dependency=afterany:' + ':'.join(submission['array_jobs']),
            '--job-name=data-packer-finalize',
            '--output=' + str(work / 'logs' / 'finalize-%j.out'),
            '--error=' + str(work / 'logs' / 'finalize-%j.err')]
        if args.wait:
            def accepted(job_id):
                submission.update(finalizer_job=job_id, state='waiting')
                atomic_json(work / 'submission.json', submission)
                print('Array jobs: {}; waiting with sbatch --wait for finalizer {}'.format(
                    ', '.join(submission['array_jobs']), job_id), flush=True)

            code = submit_wait(final_options, path, accepted)
            submission.update(state='finished', wait_exit_code=code)
        else:
            submission['finalizer_job'] = submit(final_options, path)
            submission['state'] = 'submitted'
        atomic_json(work / 'submission.json', submission)
    except Exception as exc:
        submission.update(state='submission_failed', error=str(exc))
        atomic_json(work / 'submission.json', submission)
        if submission['array_jobs']:
            cancelled = subprocess.run(['scancel'] + submission['array_jobs'], capture_output=True, text=True)
            if cancelled.returncode:
                print('Could not cancel submitted jobs: ' + cancelled.stderr.strip(), file=sys.stderr)
            else:
                print('Cancelled already submitted jobs: ' + ', '.join(submission['array_jobs']), file=sys.stderr)
        raise
    print('Array jobs: {}; finalizer: {}'.format(', '.join(submission['array_jobs']),
                                                submission['finalizer_job']), flush=True)
    if args.wait:
        return waited_result(job_path, code, submission)
    print('Submitted. Inspect progress with: {} {} status --input {}'.format(
        shlex.quote(sys.executable), shlex.quote(str(cli)), shlex.quote(str(work))), flush=True)
    return 0
