import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import tarfile

import pytest

from test_local import run


def single_pack(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'a').write_bytes(b'a')
    packed = tmp_path / 'packed'
    run('pack', '--input', source, '--output', packed)
    return source, packed


def test_pack_rejects_symlink_chain_that_escapes_after_resolution(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (tmp_path / 'outside').write_bytes(b'outside')
    (source / 'a').symlink_to('.')
    (source / 'b').symlink_to('a/../outside')
    run('pack', '--input', source, '--output', tmp_path / 'packed', ok=False)


def test_restore_rejects_link_through_existing_unplanned_external_symlink(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'link').symlink_to('unrelated/file')
    packed = tmp_path / 'packed'
    run('pack', '--input', source, '--output', packed)
    outside = tmp_path / 'outside'
    outside.mkdir()
    destination = tmp_path / 'restored'
    destination.mkdir()
    (destination / 'unrelated').symlink_to(outside)
    run('unpack', '--input', packed, '--output', destination, '--overwrite', ok=False)
    assert not os.path.lexists(destination / 'link')
    assert (destination / 'unrelated').is_symlink()


@pytest.mark.parametrize('member_name', ['../escape', '/tmp/escape', 'unexpected'])
def test_malicious_tar_members_never_write_outside_plan(tmp_path, member_name):
    _, packed = single_pack(tmp_path)
    manifest_path = packed / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    archive = packed / manifest['shards'][0]['archive']
    with tarfile.open(archive, 'w') as output:
        info = tarfile.TarInfo(member_name)
        info.size = 3
        output.addfile(info, io.BytesIO(b'bad'))
    manifest['shards'][0]['sha256'] = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest['shards'][0]['bytes'] = archive.stat().st_size
    manifest_path.write_text(json.dumps(manifest))
    destination = tmp_path / 'restored'
    run('unpack', '--input', packed, '--output', destination, ok=False)
    assert not (tmp_path / 'escape').exists()
    assert not (destination / 'unexpected').exists()


def test_foreign_state_directory_is_not_deleted_or_repurposed(tmp_path):
    _, packed = single_pack(tmp_path)
    state = tmp_path / 'state'
    (state / 'receipts').mkdir(parents=True)
    important = state / 'receipts' / 'important.txt'
    important.write_bytes(b'keep me')
    run('unpack', '--input', packed, '--output', tmp_path / 'restored',
        '--state-dir', state, ok=False)
    assert important.read_bytes() == b'keep me'
    assert not (state / 'job.json').exists()


def test_pack_rechecks_empty_output_after_acquiring_coordinator_lock(tmp_path, monkeypatch):
    from packer import cli
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'a').write_bytes(b'a')
    packed = tmp_path / 'packed'
    original = cli.prepare_job

    def competing_writer(args):
        prepared = original(args)
        (packed / 'manifest.json').write_bytes(b'other operation completed')
        return prepared

    monkeypatch.setattr(cli, 'prepare_job', competing_writer)
    assert cli.main(['pack', '--input', str(source), '--output', str(packed)]) == 1
    assert (packed / 'manifest.json').read_bytes() == b'other operation completed'


def test_restores_same_destination_cannot_overlap_with_different_state_dirs(tmp_path, monkeypatch):
    # Attempt a second coordinator while the first owns the destination.
    from packer import cli
    _, packed = single_pack(tmp_path)
    destination = tmp_path / 'restored'
    original = cli.run_local
    second_results = []
    entered = False

    def during_first(job_path, shards, workers):
        nonlocal entered
        if not entered:
            entered = True
            second_results.append(cli.main(['unpack', '--input', str(packed),
                                           '--output', str(destination), '--overwrite',
                                           '--state-dir', str(tmp_path / 'state2')]))
        return original(job_path, shards, workers)

    monkeypatch.setattr(cli, 'run_local', during_first)
    result = cli.main(['unpack', '--input', str(packed), '--output', str(destination),
                       '--state-dir', str(tmp_path / 'state1')])
    assert result == 0
    assert second_results == [1]


def test_file_and_symlink_nanosecond_timestamps_are_preserved(tmp_path):
    source, packed = single_pack(tmp_path)
    (source / 'link').symlink_to('a')
    stamp = 1234567890123456789
    os.utime(source / 'a', ns=(stamp, stamp))
    os.utime(source / 'link', ns=(stamp, stamp), follow_symlinks=False)
    other = tmp_path / 'other'
    run('pack', '--input', source, '--output', other)
    destination = tmp_path / 'restored'
    run('unpack', '--input', other, '--output', destination)
    assert (destination / 'a').stat().st_mtime_ns == stamp
    assert (destination / 'link').lstat().st_mtime_ns == stamp


def test_finalizer_records_directory_metadata_failure(tmp_path, monkeypatch):
    from packer import archive, cli
    _, packed = single_pack(tmp_path)
    destination = tmp_path / 'restored'
    original = archive.os.utime

    def fail_directory(path, *args, **kwargs):
        if Path(path).is_dir():
            raise OSError('simulated directory metadata failure')
        return original(path, *args, **kwargs)

    monkeypatch.setattr(archive.os, 'utime', fail_directory)
    result = cli.main(['unpack', '--input', str(packed), '--output', str(destination)])
    assert result == 1
    summary = json.loads(Path(str(destination) + '.data-packer-state/summary.json').read_text())
    assert summary['state'] == 'failed'
    assert 'metadata failure' in summary['error']


@pytest.mark.parametrize('worker_failure', [False, True])
def test_finalizer_removes_all_escaping_forward_links_even_after_worker_failure(tmp_path, worker_failure):
    source = tmp_path / 'source'
    source.mkdir()
    for name in ('b', 'c', 'd', 'z'):
        (source / name).write_bytes(b'x')
    packed = tmp_path / 'packed'
    run('pack', '--input', source, '--output', packed)
    # Model an untrusted, self-consistent bundle; do not rely on checksum failure
    # as the extraction sandbox. Unknown entries are valid for unsampled metadata.
    with sqlite3.connect(str(packed / 'inventory.sqlite3')) as db:
        db.execute('UPDATE entries SET kind="unknown",target=NULL')
    manifest_path = packed / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    manifest['config']['inventory_sha256'] = hashlib.sha256((packed / 'inventory.sqlite3').read_bytes()).hexdigest()
    archive = packed / manifest['shards'][0]['archive']
    with tarfile.open(archive, 'w') as output:
        for name, target in [('b', 'c/../outside'), ('d', 'c/../outside'), ('c', '.')]:
            info = tarfile.TarInfo(name)
            info.type = tarfile.SYMTYPE
            info.linkname = target
            output.addfile(info)
        info = tarfile.TarInfo('z')
        if worker_failure:
            info.type = tarfile.CHRTYPE
            output.addfile(info)
        else:
            info.size = 1
            output.addfile(info, io.BytesIO(b'z'))
    manifest['shards'][0]['sha256'] = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest))
    (tmp_path / 'outside').write_bytes(b'external')
    destination = tmp_path / 'restored'
    run('unpack', '--input', packed, '--output', destination, ok=False)
    assert not os.path.lexists(destination / 'b')
    assert not os.path.lexists(destination / 'd')
    assert (tmp_path / 'outside').read_bytes() == b'external'
