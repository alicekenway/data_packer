"""Command-line coordination: plan once, then execute archive workers."""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import shutil
import sys
import uuid

from .archive import (finalize, load_manifest, pending_pack_shards, prepare_destination,
                      run_local, worker)
from .common import (PackerError, atomic_json, byte_size, ensure_directory, lock,
                     positive, read_json, separate_roots)
from .inventory import create_plan


def restore_state(path, destination):
    """Control directories belong to the tool only after an explicit marker."""
    if path.exists() and any(path.iterdir()):
        marker = path / 'state.json'
        if not marker.is_file() or marker.is_symlink():
            raise PackerError('state directory is not empty and is not owned by data-packer: {}'.format(path))
        value = read_json(marker)
        if (value.get('application') != 'data-packer' or value.get('version') != 1 or
                value.get('destination') != str(destination)):
            raise PackerError('state directory belongs to another destination or application: {}'.format(path))
    ensure_directory(path)
    atomic_json(path / 'state.json', dict(application='data-packer', version=1, destination=str(destination)))


def check_destination_owner(directory):
    marker = directory / 'destination-owner.json'
    if marker.exists():
        previous = Path(read_json(marker)['work'])
        if (previous / 'submission.json').exists():
            from .slurm import check_inactive
            check_inactive(previous)


def parser():
    result = argparse.ArgumentParser(description='Create balanced tar archives and restore directory trees.')
    commands = result.add_subparsers(dest='command', required=True, metavar='{pack,unpack,status}')
    for command in ('pack', 'unpack'):
        sub = commands.add_parser(command, help='create parallel tar archives' if command == 'pack'
                                  else 'restore an archived directory tree')
        sub.add_argument('--input', required=True, type=Path)
        sub.add_argument('--output', required=True, type=Path)
        sub.add_argument('--mode', choices=('local', 'slurm'), default='local')
        sub.add_argument('--workers', type=positive, default=4)
        sub.add_argument('--concurrency', type=positive, default=8, help='maximum concurrent Slurm array tasks')
        sub.add_argument('--wait', action='store_true', help='use sbatch --wait on the final job that depends on all arrays')
        sub.add_argument('--partition', default=None)
        sub.add_argument('--mem', default='4G')
        sub.add_argument('--time', default='24:00:00')
        sub.add_argument('--cpus-per-task', type=positive, default=1)
        sub.add_argument('--account')
        sub.add_argument('--qos')
        sub.add_argument('--sbatch-args', default='', help='additional sbatch arguments, quoted as one string')
        # Compatibility with the initial queue-polling implementation.
        sub.add_argument('--poll-interval', type=positive, default=10, help=argparse.SUPPRESS)
        sub.add_argument('--max-array-size', type=positive, help='override Slurm MaxArraySize discovery')
        if command == 'pack':
            sizing = sub.add_mutually_exclusive_group()
            sizing.add_argument('--target-size', type=byte_size, default=10 * 1024 ** 3)
            sizing.add_argument('--parts', type=positive)
            sub.add_argument('--compression', choices=('none', 'gzip'), default='none')
            sub.add_argument('--balance', choices=('sample', 'exact'), default='sample')
            sub.add_argument('--resume', action='store_true')
        else:
            sub.add_argument('--overwrite', action='store_true')
            sub.add_argument('--state-dir', type=Path, help='restore logs/state, default: OUTPUT.data-packer-state')
    status = commands.add_parser('status', help='inspect packing or an unpack state directory')
    status.add_argument('--input', required=True, type=Path)
    internal = commands.add_parser('_worker')
    internal.add_argument('--job', required=True, type=Path)
    selection = internal.add_mutually_exclusive_group(required=True)
    selection.add_argument('--shard', type=int)
    selection.add_argument('--shards-file', type=Path)
    internal = commands.add_parser('_finalize')
    internal.add_argument('--job', required=True, type=Path)
    return result


def prepare_pack(args):
    source, packed = args.input.resolve(), args.output.resolve()
    if not source.is_dir():
        raise PackerError('input directory does not exist: {}'.format(source))
    separate_roots(source, packed)
    if not shutil.which('tar'):
        raise PackerError('GNU tar is required for packing')
    if args.compression == 'gzip' and not shutil.which('gzip'):
        raise PackerError('gzip is required for compressed packing')
    ensure_directory(packed)
    if not args.resume and any(packed.iterdir()):
        raise PackerError('pack output must be empty; use --resume to retry an existing plan')
    return source, packed, packed / '.work'


def prepare_job(args):
    if args.command == 'pack':
        source, packed, work = prepare_pack(args)
        ensure_directory(work)
        job = dict(operation='pack', packed=str(packed))
        return packed, work, job, source
    packed, destination = args.input.resolve(), args.output.absolute()
    # Retain the final component to reject output symlinks rather than following them.
    if destination.is_symlink():
        raise PackerError('output directory must not be a symlink')
    destination = destination.resolve()
    separate_roots(packed, destination)
    work = (args.state_dir or Path(str(destination) + '.data-packer-state')).resolve()
    ownership = Path(str(destination) + '.data-packer-state')
    separate_roots(destination, work)
    separate_roots(packed, work)
    separate_roots(destination, ownership)
    separate_roots(packed, ownership)
    restore_state(work, destination)
    if ownership != work:
        restore_state(ownership, destination)
    job = dict(operation='unpack', packed=str(packed), destination=str(destination), overwrite=args.overwrite,
               ownership=str(ownership), token=uuid.uuid4().hex)
    return packed, work, job, None


def status(path):
    packed = path.resolve()
    work = packed / '.work' if (packed / 'manifest.json').exists() else packed
    if (work / 'job.json').exists():
        job = read_json(work / 'job.json')
        manifest = read_json(Path(job['packed']) / 'manifest.json')
        summary = read_json(work / 'summary.json') if (work / 'summary.json').exists() else None
        counts = dict(pending=0, running=0, complete=0, failed=0)
        for item in manifest['shards']:
            receipt = work / 'receipts' / ('{:06d}.json'.format(item['id']))
            record = (summary['shards'][item['id']] if summary else
                      read_json(receipt) if receipt.exists() else {'state': 'pending'})
            counts[record['state']] += 1
        print(json.dumps(dict(operation=job['operation'], archives=counts,
                              summary=summary,
                              submission=read_json(work / 'submission.json') if (work / 'submission.json').exists() else None), indent=2))
    else:
        print(json.dumps(read_json(packed / 'manifest.json'), indent=2))
    return 0


def coordinate(args):
    packed, work, job, source = prepare_job(args)
    with ExitStack() as stack:
        stack.enter_context(lock(work / 'coordinator.lock'))
        if args.command == 'pack' and not args.resume:
            if any(path.name != '.work' for path in packed.iterdir()):
                raise PackerError('pack output became nonempty before planning; refusing to replace existing data')
        if args.command == 'unpack':
            ownership = Path(job['ownership'])
            stack.enter_context(lock(ownership / 'destination.lock'))
            check_destination_owner(ownership)
        if args.mode == 'slurm':
            from .slurm import validate_options
            validate_options(args)
        if (work / 'submission.json').exists():
            from .slurm import check_inactive
            check_inactive(work)
        if args.command == 'pack':
            if args.resume:
                manifest = load_manifest(packed)
                if Path(manifest['config']['source']) != source:
                    raise PackerError('--resume source differs from the original plan')
                shards = pending_pack_shards(packed, work)
            else:
                create_plan(source, packed, args.target_size, args.parts, args.compression, args.balance)
                manifest = read_json(packed / 'manifest.json')
                shards = [item['id'] for item in manifest['shards']]
            manifest['state'] = 'running'
            atomic_json(packed / 'manifest.json', manifest)
        else:
            manifest = prepare_destination(packed, Path(job['destination']), args.overwrite)
            shards = [item['id'] for item in manifest['shards']]
            receipts = work / 'receipts'
            if receipts.exists():
                ensure_directory(receipts)
                for path in receipts.glob('[0-9][0-9][0-9][0-9][0-9][0-9].json'):
                    path.unlink()
            for path in work.glob('links-[0-9][0-9][0-9][0-9][0-9][0-9].jsonl'):
                path.unlink()
            atomic_json(ownership / 'destination-owner.json', dict(work=str(work), token=job['token']))
        for name in ('summary.json', 'submission.json'):
            path = work / name
            if path.exists():
                path.unlink()
        atomic_json(work / 'job.json', job)
        if args.mode == 'local' or not shards:
            return 0 if run_local(work / 'job.json', shards, args.workers) else 1
        from .slurm import launch
        return launch(work / 'job.json', shards, args)


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.command == '_worker':
            shard = args.shard
            if args.shards_file:
                shard = read_json(args.shards_file)[int(os.environ['SLURM_ARRAY_TASK_ID'])]
            worker(args.job, shard)
            return 0
        if args.command == '_finalize':
            return 0 if finalize(args.job) else 1
        if args.command == 'status':
            return status(args.input)
        return coordinate(args)
    except (PackerError, OSError, ValueError, KeyError, IndexError) as exc:
        print('Error: {}'.format(exc), file=sys.stderr, flush=True)
        return 1
    except KeyboardInterrupt:
        print('Interrupted. Completed archives are retained; submitted Slurm jobs continue.', file=sys.stderr)
        return 130
