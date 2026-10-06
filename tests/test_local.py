import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tarfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / 'data_packer.py'


def run(*args, ok=True, env=None):
    result = subprocess.run([sys.executable, str(CLI)] + list(map(str, args)),
                            capture_output=True, text=True, env=env)
    if ok:
        assert result.returncode == 0, result.stdout + result.stderr
    else:
        assert result.returncode != 0, result.stdout + result.stderr
    return result


def inventory(packed):
    with sqlite3.connect(str(packed / 'inventory.sqlite3')) as db:
        return db.execute('SELECT path, shard, size FROM entries ORDER BY path').fetchall()


@pytest.mark.parametrize('compression', ['none', 'gzip'])
def test_round_trip_preserves_nested_files_and_directory_metadata(tmp_path, compression):
    source = tmp_path / 'source'
    (source / 'batch' / 'wav').mkdir(parents=True)
    (source / 'empty').mkdir()
    files = {'batch/wav/a.wav': b'a' * 513, 'batch/wav/-b\n x.wav': b'b' * 3073,
             'batch/meta.json': b'{"hello": 1}', 'script.sh': b'#!/bin/sh\n',
             'zero': b''}
    for name, data in files.items():
        (source / name).write_bytes(data)
    (source / 'script.sh').chmod(0o751)
    os.utime(source / 'empty', (1234567890, 1234567890))
    packed = tmp_path / 'packed'
    run('pack', '--input', source, '--output', packed, '--parts', 3,
        '--workers', 2, '--compression', compression)
    manifest = json.loads((packed / 'manifest.json').read_text())
    assert manifest['state'] == 'complete'
    assert len(manifest['shards']) == 3
    for shard in manifest['shards']:
        archive = packed / shard['archive']
        assert hashlib.sha256(archive.read_bytes()).hexdigest() == shard['sha256']
        assert archive.name.endswith('.tar.gz' if compression == 'gzip' else '.tar')
    restored = tmp_path / 'restored'
    run('unpack', '--input', packed, '--output', restored, '--workers', 2)
    for name, data in files.items():
        assert (restored / name).read_bytes() == data
    assert (restored / 'empty').is_dir()
    assert int((restored / 'empty').stat().st_mtime) == 1234567890
    assert (restored / 'script.sh').stat().st_mode & 0o777 == 0o751


def test_metadata_is_complete_listing_and_other_data_is_included(tmp_path):
    source = tmp_path / 'source'
    wav = source / 'batch' / 'wav'
    wav.mkdir(parents=True)
    for name, size in [('a.wav', 13), ('b.wav', 19), ('unlisted.wav', 71)]:
        (wav / name).write_bytes(b'x' * size)
    metadata = source / 'batch' / 'metadata.jsonl'
    metadata.write_text(json.dumps({'audiofile_path': 'wav/a.wav'}) + '\n' +
                        json.dumps({'path': str(wav / 'b.wav')}) + '\n' +
                        json.dumps({'path': 'wav/a.wav'}) + '\n')
    (source / 'notes').write_bytes(b'notes')
    packed = tmp_path / 'packed'
    run('pack', '--input', source, '--output', packed, '--parts', 2)
    assert [row[0] for row in inventory(packed)] == [
        'batch/metadata.jsonl', 'batch/wav/a.wav', 'batch/wav/b.wav', 'notes']
    run('unpack', '--input', packed, '--output', tmp_path / 'restored')
    assert not (tmp_path / 'restored/batch/wav/unlisted.wav').exists()


@pytest.mark.parametrize('metadata', [None, 'broken json\n', '{"text": "not a listing"}\n'])
def test_missing_or_unusable_metadata_falls_back_to_scanning(tmp_path, metadata):
    source = tmp_path / 'source'
    (source / 'wav').mkdir(parents=True)
    (source / 'wav/a.wav').write_bytes(b'a')
    if metadata is not None:
        (source / 'metadata.jsonl').write_text(metadata)
    packed = tmp_path / 'packed'
    run('pack', '--input', source, '--output', packed)
    assert 'wav/a.wav' in [row[0] for row in inventory(packed)]


def test_json_listing_and_exact_sizes_balance_deterministically(tmp_path):
    source = tmp_path / 'source'
    (source / 'wav').mkdir(parents=True)
    rows = []
    for i, size in enumerate([8000, 6000, 4000, 2000]):
        (source / 'wav' / (str(i) + '.wav')).write_bytes(b'a' * size)
        rows.append({'path': 'wav/' + str(i) + '.wav'})
    (source / 'metadata.json').write_text(json.dumps(rows))
    packed = tmp_path / 'packed'
    run('pack', '--input', source, '--output', packed, '--parts', 2, '--balance', 'exact')
    data = [row for row in inventory(packed) if row[0].endswith('.wav')]
    assert [row[2] for row in data] == [8000, 6000, 4000, 2000]
    totals = [sum(row[2] for row in data if row[1] == shard) for shard in (0, 1)]
    assert max(totals) - min(totals) <= 2000


def test_empty_source_and_oversized_file(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    packed = tmp_path / 'packed'
    run('pack', '--input', source, '--output', packed)
    run('unpack', '--input', packed, '--output', tmp_path / 'restored')
    assert (tmp_path / 'restored').is_dir()
    (source / 'huge').write_bytes(b'x' * 10000)
    other = tmp_path / 'other'
    run('pack', '--input', source, '--output', other, '--target-size', '1KiB')
    assert len(json.loads((other / 'manifest.json').read_text())['shards']) == 1


def test_corruption_is_rejected_before_extracting(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'a').write_bytes(b'original')
    packed = tmp_path / 'packed'
    run('pack', '--input', source, '--output', packed)
    archive = next(packed.glob('*.tar'))
    with archive.open('r+b') as handle:
        handle.seek(512)
        handle.write(b'bad')
    destination = tmp_path / 'restored'
    run('unpack', '--input', packed, '--output', destination, ok=False)
    assert not (destination / 'a').exists()


def test_restore_requires_empty_output_and_explicit_overwrite(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'a').write_bytes(b'new')
    packed = tmp_path / 'packed'
    run('pack', '--input', source, '--output', packed)
    destination = tmp_path / 'restored'
    destination.mkdir()
    (destination / 'a').write_bytes(b'old')
    (destination / 'other').write_bytes(b'keep')
    run('unpack', '--input', packed, '--output', destination, ok=False)
    assert (destination / 'a').read_bytes() == b'old'
    run('unpack', '--input', packed, '--output', destination, '--overwrite')
    assert (destination / 'a').read_bytes() == b'new'
    assert (destination / 'other').read_bytes() == b'keep'


def test_relative_symlink_round_trip_and_escape_rejection(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'a').write_bytes(b'a')
    (source / 'link').symlink_to('a')
    packed = tmp_path / 'packed'
    run('pack', '--input', source, '--output', packed)
    destination = tmp_path / 'restored'
    run('unpack', '--input', packed, '--output', destination)
    assert os.readlink(destination / 'link') == 'a'
    (source / 'bad').symlink_to('../outside')
    run('pack', '--input', source, '--output', tmp_path / 'bad_packed', ok=False)


def test_pack_output_cannot_be_inside_source(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    run('pack', '--input', source, '--output', source / 'packed', ok=False)
    assert not (source / 'packed').exists()


def test_missing_metadata_file_fails_worker_not_silently_omits_it(tmp_path):
    source = tmp_path / 'source'
    (source / 'wav').mkdir(parents=True)
    (source / 'metadata.jsonl').write_text('{"path":"wav/missing.wav"}\n')
    packed = tmp_path / 'packed'
    run('pack', '--input', source, '--output', packed, ok=False)
    assert not list(packed.glob('*.tar'))


def test_missing_metadata_parent_does_not_turn_into_successful_scan_fallback(tmp_path):
    source = tmp_path / 'source'
    (source / 'wav').mkdir(parents=True)
    (source / 'metadata.jsonl').write_text('{"path":"wav/missing/subdir/file.wav"}\n')
    run('pack', '--input', source, '--output', tmp_path / 'packed', ok=False)


def test_resume_reuses_valid_shards_and_replaces_corrupt_one(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    for i in range(3):
        (source / str(i)).write_bytes(bytes([i]) * 1000)
    packed = tmp_path / 'packed'
    run('pack', '--input', source, '--output', packed, '--parts', 3)
    archives = sorted(packed.glob('*.tar'))
    untouched_time = archives[1].stat().st_mtime_ns
    archives[0].write_bytes(b'corrupt')
    run('pack', '--input', source, '--output', packed, '--resume')
    assert archives[1].stat().st_mtime_ns == untouched_time
    run('unpack', '--input', packed, '--output', tmp_path / 'restored')
