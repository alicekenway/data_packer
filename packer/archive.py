"""Archive workers shared by the local executor and Slurm arrays."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import subprocess
import tarfile
import tempfile
import time

from .common import (PackerError, archive_name, atomic_json, check_link, check_resolved_link, clean_relative,
                     config, ensure_directory, lock, open_inventory, read_json,
                     receipt_path, sha256)


def load_manifest(packed, require_complete=False):
    manifest = read_json(packed / 'manifest.json')
    if manifest.get('version') != 1:
        raise PackerError('unsupported manifest version')
    if require_complete and manifest.get('state') != 'complete':
        raise PackerError('packing is incomplete; inspect status or resume packing first')
    if sha256(packed / 'inventory.sqlite3') != manifest['config']['inventory_sha256']:
        raise PackerError('inventory checksum mismatch')
    with open_inventory(packed) as db:
        cfg = config(db)
        shards = list(db.execute('SELECT id,file_count FROM shards ORDER BY id'))
        if len(shards) != len(manifest['shards']):
            raise PackerError('manifest and inventory disagree about archive count')
        for row, item in zip(shards, manifest['shards']):
            if (item['id'] != row['id'] or item['file_count'] != row['file_count'] or
                    item['archive'] != archive_name(row['id'], cfg['compression'])):
                raise PackerError('manifest archive does not match inventory')
    return manifest


def validated_parents(root, relative, cache):
    current = root
    for part in Path(relative).parent.parts:
        current = current / part
        if current not in cache:
            details = current.lstat()
            if not stat.S_ISDIR(details.st_mode):
                raise PackerError('parent is not a real directory: {}'.format(current))
            if len(cache) >= 4096:
                cache.clear()
            cache.add(current)


def pack_one(job, shard, work):
    packed = Path(job['packed'])
    with open_inventory(packed) as db:
        cfg = config(db)
        source = Path(cfg['source'])
        name = archive_name(shard, cfg['compression'])
        cache = {source}
        list_fd, list_name = tempfile.mkstemp(prefix='files-', suffix='.null', dir=str(work))
        temp_fd, temp_name = tempfile.mkstemp(prefix='.' + name + '.', suffix='.partial', dir=str(packed))
        proc = None
        try:
            with os.fdopen(list_fd, 'wb') as listing:
                for row in db.execute('SELECT * FROM entries WHERE shard=? ORDER BY path', (shard,)):
                    relative = clean_relative(row['path'])
                    validated_parents(source, relative, cache)
                    details = (source / relative).lstat()
                    if stat.S_ISLNK(details.st_mode):
                        target = os.readlink(source / relative)
                        check_link(relative, target, source)
                        if row['kind'] not in ('unknown', 'symlink'):
                            raise PackerError('file type changed since planning: {}'.format(relative))
                        if row['target'] is not None and row['target'] != target:
                            raise PackerError('symlink changed since planning: {}'.format(relative))
                    elif not stat.S_ISREG(details.st_mode) or row['kind'] == 'symlink':
                        raise PackerError('unsupported or changed file type: {}'.format(relative))
                    if row['mtime_ns'] is not None and (details.st_mtime_ns != row['mtime_ns'] or
                                                       details.st_size != row['size']):
                        raise PackerError('file changed since planning: {}'.format(relative))
                    listing.write(os.fsencode(relative) + b'\x00')
            command = ['tar', '--format=pax', '--hard-dereference', '--no-recursion',
                       '--null', '--verbatim-files-from', '-C', str(source),
                       '-T', list_name, '-cf', '-']
            if cfg['compression'] == 'gzip':
                command.extend(['-I', 'gzip -1'])
            ensure_directory(work / 'logs')
            digest = hashlib.sha256()
            with os.fdopen(temp_fd, 'wb') as output, (work / 'logs' / ('{:06d}.log'.format(shard))).open('ab') as log:
                temp_fd = None
                proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=log)
                try:
                    for chunk in iter(lambda: proc.stdout.read(1024 * 1024), b''):
                        digest.update(chunk)
                        output.write(chunk)
                    code = proc.wait()
                    if code:
                        raise PackerError('tar failed (exit {}); see {}'.format(code, log.name))
                finally:
                    proc.stdout.close()
                output.flush()
                os.fsync(output.fileno())
            os.replace(temp_name, packed / name)
            return dict(archive=name, sha256=digest.hexdigest(), bytes=(packed / name).stat().st_size)
        finally:
            if proc is not None and proc.poll() is None:
                proc.terminate()
                proc.wait()
            if temp_fd is not None:
                os.close(temp_fd)
            for path in (list_name, temp_name):
                if os.path.exists(path):
                    os.unlink(path)


def prepare_destination(packed, destination, overwrite):
    manifest = load_manifest(packed, require_complete=True)
    ensure_directory(destination)
    if not overwrite and any(destination.iterdir()):
        raise PackerError('restore output must be empty; use --overwrite for intentional replacement')
    with open_inventory(packed) as db:
        cache = {destination}
        for row in db.execute('SELECT path FROM directories ORDER BY length(path),path'):
            relative = clean_relative(row['path'], allow_root=True)
            validated_parents(destination, relative, cache)
            ensure_directory(destination / relative)
        for (relative,) in db.execute('SELECT path FROM entries'):
            relative = clean_relative(relative)
            # A new/empty destination has no existing file paths to inspect.
            # In overwrite mode validate them before replacing any contents.
            if overwrite:
                validated_parents(destination, relative, cache)
                output = destination / relative
                if output.is_symlink() or (output.exists() and not output.is_file()):
                    raise PackerError('unsafe existing restore entry: {}'.format(output))
    return manifest


def unpack_one(job, shard, work):
    packed, destination = Path(job['packed']), Path(job['destination'])
    manifest = read_json(packed / 'manifest.json')
    item = manifest['shards'][shard]
    name = archive_name(shard, manifest['config']['compression'])
    if item['id'] != shard or item['archive'] != name:
        raise PackerError('invalid archive record')
    path = packed / name
    if path.is_symlink() or sha256(path) != item['sha256']:
        raise PackerError('archive checksum mismatch: {}'.format(name))
    cache = {destination}
    count = 0
    with open_inventory(packed) as db, tarfile.open(str(path), 'r|*') as archive, \
            (work / ('links-{:06d}.jsonl'.format(shard))).open('w', encoding='utf-8') as links:
        # A disk-backed seen table detects duplicate members without an unbounded set.
        fd, seen_path = tempfile.mkstemp(prefix='seen-', suffix='.sqlite3', dir=str(work))
        os.close(fd)
        seen = sqlite3.connect(seen_path)
        seen.execute('CREATE TABLE paths (path TEXT PRIMARY KEY)')
        try:
            for member in archive:
                relative = clean_relative(member.name)
                expected = db.execute('SELECT kind,target FROM entries WHERE path=? AND shard=?',
                                      (relative, shard)).fetchone()
                if expected is None:
                    raise PackerError('archive contains an unplanned path: {}'.format(relative))
                try:
                    seen.execute('INSERT INTO paths VALUES (?)', (relative,))
                except sqlite3.IntegrityError:
                    raise PackerError('duplicate archive member: {}'.format(relative))
                validated_parents(destination, relative, cache)
                output = destination / relative
                if output.is_symlink() or (output.exists() and not output.is_file()):
                    raise PackerError('unsafe existing restore entry: {}'.format(output))
                if output.exists() and not job['overwrite']:
                    raise PackerError('restore file already exists: {}'.format(output))
                if member.issym():
                    if expected['kind'] not in ('unknown', 'symlink'):
                        raise PackerError('unexpected symlink in archive: {}'.format(relative))
                    check_link(relative, member.linkname)
                    check_resolved_link(output, destination, member.linkname)
                    if expected['target'] is not None and expected['target'] != member.linkname:
                        raise PackerError('symlink target disagrees with inventory')
                    # Persist the cleanup record before installing the link.
                    links.write(json.dumps(relative) + '\n')
                    links.flush()
                    fd, temporary = tempfile.mkstemp(prefix='.restore-', dir=str(output.parent))
                    os.close(fd)
                    os.unlink(temporary)
                    try:
                        os.symlink(member.linkname, temporary)
                        stamp = int(Decimal(member.pax_headers.get('mtime', str(member.mtime))) * 1000000000)
                        os.utime(temporary, ns=(stamp, stamp), follow_symlinks=False)
                        os.replace(temporary, output)
                    finally:
                        if os.path.lexists(temporary):
                            os.unlink(temporary)
                elif member.isfile():
                    if expected['kind'] == 'symlink':
                        raise PackerError('expected symlink in archive: {}'.format(relative))
                    fd, temporary = tempfile.mkstemp(prefix='.restore-', dir=str(output.parent))
                    try:
                        with os.fdopen(fd, 'wb') as stream, archive.extractfile(member) as data:
                            shutil.copyfileobj(data, stream, length=1024 * 1024)
                            stream.flush()
                            os.fchmod(stream.fileno(), member.mode & 0o777)
                        stamp = int(Decimal(member.pax_headers.get('mtime', str(member.mtime))) * 1000000000)
                        os.utime(temporary, ns=(stamp, stamp))
                        os.replace(temporary, output)
                    finally:
                        if os.path.exists(temporary):
                            os.unlink(temporary)
                else:
                    raise PackerError('unsupported tar member type: {}'.format(relative))
                count += 1
                archive.members.clear()
            if count != item['file_count']:
                raise PackerError('archive file count differs from inventory: {}'.format(name))
        finally:
            seen.close()
            os.unlink(seen_path)
    return dict(archive=name, file_count=count)


def worker(job_path, shard):
    job_path = Path(job_path)
    job = read_json(job_path)
    work = job_path.parent
    with lock(work / 'locks' / ('{:06d}.lock'.format(shard))):
        receipt = dict(id=shard, state='running', started=time.time())
        atomic_json(receipt_path(work, shard), receipt)
        try:
            receipt.update(pack_one(job, shard, work) if job['operation'] == 'pack'
                           else unpack_one(job, shard, work))
            receipt.update(state='complete', finished=time.time())
            atomic_json(receipt_path(work, shard), receipt)
        except Exception as exc:
            receipt.update(state='failed', error=str(exc), finished=time.time())
            atomic_json(receipt_path(work, shard), receipt)
            raise


def validate_restore_links(job, manifest, work):
    """Check all installed links, including the output of failed workers."""
    destination = Path(job['destination'])
    errors = []
    unsafe_shards = set()
    for item in manifest['shards']:
        links = work / ('links-{:06d}.jsonl'.format(item['id']))
        if not links.exists():
            continue
        with links.open(encoding='utf-8') as stream:
            for line in stream:
                path = None
                try:
                    path = destination / clean_relative(json.loads(line))
                    if path.is_symlink():
                        check_resolved_link(path, destination)
                except (PackerError, OSError, ValueError) as exc:
                    unsafe_shards.add(item['id'])
                    if len(errors) < 20:
                        errors.append(str(exc))
                    if path is not None and path.is_symlink():
                        try:
                            path.unlink()
                        except OSError as removal_error:
                            if len(errors) < 20:
                                errors.append('could not remove unsafe link: ' + str(removal_error))
    return unsafe_shards, errors


def restore_directory_metadata(job, packed):
    destination = Path(job['destination'])
    with open_inventory(packed) as db:
        for row in db.execute('SELECT * FROM directories ORDER BY length(path) DESC,path DESC'):
            path = destination / clean_relative(row['path'], allow_root=True)
            if path.is_symlink() or not path.is_dir():
                raise PackerError('restore directory changed: {}'.format(path))
            os.chmod(path, row['mode'] & 0o777)
            os.utime(path, ns=(row['mtime_ns'], row['mtime_ns']))


def finalize(job_path):
    job_path = Path(job_path)
    job = read_json(job_path)
    work, packed = job_path.parent, Path(job['packed'])
    manifest = read_json(packed / 'manifest.json')
    records = []
    failed = []
    for item in manifest['shards']:
        receipt = receipt_path(work, item['id'])
        result = read_json(receipt) if receipt.exists() else dict(state='failed', error='worker did not finish')
        record = dict(item)
        record.update(result)
        if job['operation'] == 'pack' and result.get('state') == 'complete':
            path = packed / item['archive']
            if not path.is_file() or path.is_symlink() or path.stat().st_size != result['bytes']:
                record.update(state='failed', error='completed archive is missing or has changed size')
        if record.get('state') != 'complete':
            record['state'] = 'failed'
            failed.append(item['id'])
        records.append(record)
    summary = dict(version=1, operation=job['operation'], state='failed' if failed else 'complete',
                   failed_shards=failed, shards=records, finished=time.time())
    if job['operation'] == 'pack':
        summary['config'] = manifest['config']
        atomic_json(packed / 'manifest.json', summary)
    else:
        try:
            unsafe, errors = validate_restore_links(job, manifest, work)
            for record in records:
                if record['id'] in unsafe:
                    record.update(state='failed', error='unsafe symlink removed during final validation')
            if unsafe:
                failed[:] = sorted(set(failed) | unsafe)
                summary.update(state='failed', error='; '.join(errors))
            if summary['state'] == 'complete':
                restore_directory_metadata(job, packed)
        except Exception as exc:
            summary.update(state='failed', error=str(exc))
    atomic_json(work / 'summary.json', summary)
    print('{}: {} ({} archives; {} failed)'.format(job['operation'], summary['state'],
                                                  len(records), len(failed)), flush=True)
    if summary.get('error'):
        print('Finalization error: ' + summary['error'], flush=True)
    return summary['state'] == 'complete'


def run_local(job_path, shards, workers):
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(worker, job_path, shard): shard for shard in shards}
        for future in as_completed(futures):
            shard = futures[future]
            try:
                future.result()
                print('Archive {} finished'.format(shard), flush=True)
            except Exception as exc:
                print('Archive {} failed: {}'.format(shard, exc), flush=True)
    return finalize(job_path)


def pending_pack_shards(packed, work):
    manifest = load_manifest(packed)
    pending = []
    for item in manifest['shards']:
        path = packed / item['archive']
        receipt = receipt_path(work, item['id'])
        valid = False
        if receipt.exists() and path.is_file() and not path.is_symlink():
            result = read_json(receipt)
            valid = (result.get('state') == 'complete' and
                     result.get('sha256') == sha256(path) and result.get('bytes') == path.stat().st_size)
        if not valid:
            pending.append(item['id'])
            if receipt.exists():
                receipt.unlink()
    return pending
