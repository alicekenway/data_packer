import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tracemalloc

from packer.inventory import Scanner


def measure_metadata_memory(tmp_path):
    # Virtualize only file stats, keeping the real JSON parser and SQLite planner.
    source = tmp_path / 'source'
    (source / 'wav').mkdir(parents=True)
    template = source / 'template'
    template.write_bytes(b'x' * 123)
    details = template.stat()
    original = Path.lstat

    def peak(count):
        metadata = source / 'metadata.jsonl'
        with metadata.open('w') as stream:
            for i in range(count):
                stream.write('{"path":"wav/item-%06d.wav"}\n' % i)
        db = sqlite3.connect(str(tmp_path / ('inventory-%d.sqlite3' % count)))
        db.executescript('''
            CREATE TABLE entries(path TEXT PRIMARY KEY, kind TEXT, size REAL, shard INTEGER,
                mtime_ns INTEGER, mode INTEGER, target TEXT, metadata_group TEXT);
            CREATE INDEX group_idx ON entries(metadata_group,path);
            CREATE TABLE directories(path TEXT PRIMARY KEY,mode INTEGER,mtime_ns INTEGER);
        ''')
        scanner = Scanner(source, db, 'exact')
        high = 0
        calls = 0
        planner_file = str(Path(__file__).resolve().parents[1] / 'packer' / 'inventory.py')

        def measured_stat(path):
            nonlocal high, calls
            if not path.name.startswith('item-'):
                return original(path)
            calls += 1
            if calls % 1000 == 0:
                snapshot = tracemalloc.take_snapshot().filter_traces([
                    tracemalloc.Filter(True, planner_file)])
                high = max(high, sum(stat.size for stat in snapshot.statistics('filename')))
            return details

        Path.lstat = measured_stat
        tracemalloc.start()
        try:
            assert scanner.metadata(source, metadata)
        finally:
            tracemalloc.stop()
            Path.lstat = original
            db.close()
        return high

    return peak(3000), peak(50000)


def test_exact_metadata_planning_memory_does_not_grow_with_file_count(tmp_path):
    # Isolate tracing from pytest plugins/background threads in this process.
    code = ('import json,runpy,sys; from pathlib import Path; '
            'measure=runpy.run_path(sys.argv[1])["measure_metadata_memory"]; '
            'print(json.dumps(measure(Path(sys.argv[2]))))')
    result = subprocess.run([sys.executable, '-c', code, str(Path(__file__).resolve()), str(tmp_path)],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    small, large = json.loads(result.stdout.splitlines()[-1])
    assert large <= small + 200000, (small, large)


def test_new_restore_preflight_does_not_stat_every_planned_file(tmp_path, monkeypatch):
    from packer.archive import prepare_destination
    from test_local import run
    source = tmp_path / 'source'
    (source / 'batch' / 'wav').mkdir(parents=True)
    for i in range(1000):
        (source / 'batch' / 'wav' / str(i)).write_bytes(b'x')
    packed, destination = tmp_path / 'packed', tmp_path / 'restored'
    run('pack', '--input', source, '--output', packed)
    original = Path.lstat
    count = 0

    def counted(path):
        nonlocal count
        if path == destination or destination in path.parents:
            count += 1
        return original(path)

    monkeypatch.setattr(Path, 'lstat', counted)
    prepare_destination(packed, destination, False)
    assert count < 100, count
