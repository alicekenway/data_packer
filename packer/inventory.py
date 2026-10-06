"""Metadata-first inventory and disk-backed size balancing."""
import heapq
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
import sys

from .common import (PackerError, archive_name, atomic_json, check_link,
                     clean_relative, ensure_directory, sha256)


def warn(message):
    print('Warning: ' + message, file=sys.stderr, flush=True)


class MissingListedDirectory(PackerError):
    """Valid metadata names data that is absent, not an unsupported schema."""


def json_records(path):
    """Stream JSONL or a top-level JSON array without loading the whole file."""
    with path.open(encoding='utf-8') as stream:
        if path.suffix == '.jsonl':
            for line in stream:
                if line.strip():
                    yield json.loads(line)
            return
        decoder = json.JSONDecoder()
        buffer = ''
        eof = False

        def fill():
            nonlocal buffer, eof
            chunk = stream.read(65536)
            buffer += chunk
            eof = not chunk

        def whitespace():
            nonlocal buffer
            while True:
                buffer = buffer.lstrip()
                if buffer or eof:
                    return
                fill()

        def value():
            nonlocal buffer
            while True:
                try:
                    obj, end = decoder.raw_decode(buffer)
                    buffer = buffer[end:]
                    return obj
                except json.JSONDecodeError:
                    if eof:
                        raise
                    fill()

        whitespace()
        if not buffer.startswith('['):
            yield value()
            whitespace()
            if buffer:
                raise ValueError('unexpected trailing JSON data')
            return
        buffer = buffer[1:]
        whitespace()
        if buffer.startswith(']'):
            buffer = buffer[1:]
        else:
            while True:
                yield value()
                whitespace()
                if buffer.startswith(']'):
                    buffer = buffer[1:]
                    break
                if not buffer.startswith(','):
                    raise ValueError('expected comma in JSON array')
                buffer = buffer[1:]
                whitespace()
        whitespace()
        if buffer:
            raise ValueError('unexpected trailing JSON data')


class Scanner:
    def __init__(self, source, db, balance):
        self.source = source
        self.db = db
        self.balance = balance
        self.metadata_dirs = 0
        self.scanned_files = 0

    def directory(self, relative):
        relative = clean_relative(relative, allow_root=True)
        if self.db.execute('SELECT 1 FROM directories WHERE path=?', (relative,)).fetchone():
            return
        path = self.source / relative
        details = path.lstat()
        if not stat.S_ISDIR(details.st_mode):
            raise PackerError('directory is missing or is a symlink: {}'.format(path))
        if relative != '.':
            self.directory(str(Path(relative).parent))
        self.db.execute('INSERT INTO directories VALUES (?,?,?)',
                        (relative, stat.S_IMODE(details.st_mode), details.st_mtime_ns))

    def file(self, relative, details, group=None):
        kind = 'file'
        target = None
        if stat.S_ISLNK(details.st_mode):
            kind = 'symlink'
            target = os.readlink(self.source / relative)
            check_link(relative, target, self.source)
        elif not stat.S_ISREG(details.st_mode):
            raise PackerError('unsupported special file: {}'.format(relative))
        self.db.execute('INSERT OR IGNORE INTO entries VALUES (?,?,?,?,?,?,?,?)',
                        (relative, kind, details.st_size, None, details.st_mtime_ns,
                         stat.S_IMODE(details.st_mode), target, group))

    def metadata(self, directory, path):
        """Transactionally accept a complete listing of this directory's wav/."""
        wav = directory / 'wav'
        if not wav.is_dir() or wav.is_symlink():
            return False
        group = wav.relative_to(self.source).as_posix()
        self.db.execute('SAVEPOINT metadata')
        try:
            count = 0
            for row in json_records(path):
                if not isinstance(row, dict):
                    raise ValueError('metadata row must be an object')
                value = row.get('audiofile_path', row.get('path'))
                if not isinstance(value, str) or not value:
                    raise ValueError('row lacks audiofile_path or path')
                listed = Path(value)
                if not listed.is_absolute():
                    listed = directory / listed
                # Normalize without resolving symlinks: we pack links, not their targets.
                listed = Path(os.path.abspath(str(listed)))
                try:
                    listed.relative_to(wav)
                    relative = clean_relative(listed.relative_to(self.source).as_posix())
                except (ValueError, PackerError):
                    raise ValueError('listed audio must be inside the sibling wav/ directory')
                try:
                    self.directory(str(Path(relative).parent))
                except FileNotFoundError as exc:
                    raise MissingListedDirectory('metadata lists a missing parent directory: {}'.format(relative)) from exc
                self.db.execute('INSERT OR IGNORE INTO entries VALUES (?,?,?,?,?,?,?,?)',
                                (relative, 'unknown', 1, None, None, None, None, group))
                count += 1
            if count == 0:
                raise ValueError('empty listing cannot establish directory coverage')
            count = self.db.execute('SELECT count(*) FROM entries WHERE metadata_group=?',
                                    (group,)).fetchone()[0]
            indices = set(int((i + 0.5) * count / min(count, 128))
                          for i in range(min(count, 128)))
            sample_total = 0
            sample_count = 0
            cursor = self.db.execute('SELECT path FROM entries WHERE metadata_group=? ORDER BY path',
                                     (group,))
            for i, (relative,) in enumerate(cursor):
                if self.balance != 'exact' and i not in indices:
                    continue
                try:
                    details = (self.source / relative).lstat()
                except FileNotFoundError:
                    # Keep the listed path: its archive must fail rather than omit it.
                    continue
                if not stat.S_ISREG(details.st_mode) and not stat.S_ISLNK(details.st_mode):
                    raise PackerError('unsupported listed file: {}'.format(relative))
                target = os.readlink(self.source / relative) if stat.S_ISLNK(details.st_mode) else None
                if target is not None:
                    check_link(relative, target, self.source)
                self.db.execute('UPDATE entries SET kind=?,size=?,mtime_ns=?,mode=?,target=? WHERE path=?',
                                ('symlink' if target is not None else 'file', details.st_size,
                                 details.st_mtime_ns, stat.S_IMODE(details.st_mode), target, relative))
                sample_total += details.st_size
                sample_count += 1
            mean = max(1, sample_total / sample_count) if sample_count else 1
            self.db.execute('UPDATE entries SET size=? WHERE metadata_group=? AND kind="unknown"',
                            (mean, group))
            self.db.execute('RELEASE metadata')
            self.metadata_dirs += 1
            print('Metadata: {} ({} files, {} sizes checked)'.format(group, count, sample_count), flush=True)
            return True
        except (ValueError, OSError, PackerError) as exc:
            self.db.execute('ROLLBACK TO metadata')
            self.db.execute('RELEASE metadata')
            if isinstance(exc, MissingListedDirectory):
                raise
            warn('{} unusable ({}); scanning wav/ instead'.format(path, exc))
            return False

    def scan(self, relative='.'):
        directory = self.source / relative
        self.directory(relative)
        indexed = False
        for name in ('metadata.jsonl', 'metadata.json'):
            candidate = directory / name
            if candidate.is_file() and not candidate.is_symlink():
                if self.metadata(directory, candidate):
                    indexed = True
                    break
        with os.scandir(directory) as entries:
            for entry in entries:
                path = clean_relative(Path(entry.path).relative_to(self.source).as_posix())
                if entry.is_dir(follow_symlinks=False):
                    if indexed and entry.name == 'wav':
                        self.directory(path)
                    else:
                        self.scan(path)
                else:
                    self.file(path, entry.stat(follow_symlinks=False))
                    self.scanned_files += 1
                    if self.scanned_files % 100000 == 0:
                        print('Scanned {} ordinary files'.format(self.scanned_files), flush=True)


def create_plan(source, packed, target_size, parts, compression, balance):
    ensure_directory(packed)
    temporary = packed / 'inventory.sqlite3.partial'
    final = packed / 'inventory.sqlite3'
    if temporary.exists():
        temporary.unlink()
    db = sqlite3.connect(str(temporary))
    try:
        db.executescript('''
            CREATE TABLE entries (path TEXT PRIMARY KEY, kind TEXT NOT NULL,
                size REAL NOT NULL, shard INTEGER, mtime_ns INTEGER, mode INTEGER,
                target TEXT, metadata_group TEXT);
            CREATE INDEX metadata_group_idx ON entries(metadata_group, path);
            CREATE TABLE directories (path TEXT PRIMARY KEY, mode INTEGER, mtime_ns INTEGER);
            CREATE TABLE shards (id INTEGER PRIMARY KEY, estimated_bytes REAL, file_count INTEGER);
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);
        ''')
        scanner = Scanner(source, db, balance)
        scanner.scan()
        count, total = db.execute('SELECT count(*),coalesce(sum(size + 1024),0) FROM entries').fetchone()
        number = min(count, parts or max(1, math.ceil(total / target_size))) if count else 0
        weights = [(0, i, 0) for i in range(number)]
        heapq.heapify(weights)
        db.execute('CREATE INDEX size_idx ON entries(size DESC, path)')
        batch = []
        for path, size in db.execute('SELECT path,size FROM entries ORDER BY size DESC,path'):
            weight, shard, n = heapq.heappop(weights)
            heapq.heappush(weights, (weight + size + 1024, shard, n + 1))
            batch.append((shard, path))
            if len(batch) >= 10000:
                db.executemany('UPDATE entries SET shard=? WHERE path=?', batch)
                batch.clear()
        db.executemany('UPDATE entries SET shard=? WHERE path=?', batch)
        db.executemany('INSERT INTO shards VALUES (?,?,?)',
                       [(shard, weight, n) for weight, shard, n in weights])
        db.execute('CREATE INDEX shard_idx ON entries(shard, path)')
        details = dict(version=1, source=str(source), compression=compression, balance=balance,
                       target_size=target_size, requested_parts=parts, file_count=count,
                       estimated_bytes=total, shard_count=number, metadata_dirs=scanner.metadata_dirs)
        db.execute('INSERT INTO settings VALUES (?,?)', ('config', json.dumps(details)))
        db.commit()
    finally:
        db.close()
    os.replace(temporary, final)
    details['inventory_sha256'] = sha256(final)
    shards = [dict(id=shard, archive=archive_name(shard, compression), state='pending',
                   estimated_bytes=weight, file_count=n) for weight, shard, n in sorted(weights, key=lambda x: x[1])]
    atomic_json(packed / 'manifest.json', dict(version=1, state='planned', config=details, shards=shards))
    print('Plan: {} files, {} archives, {:.2f} GiB estimated'.format(
        count, number, total / 1024 ** 3), flush=True)
    if count and total / max(1, number) > target_size and not parts:
        warn('target size is approximate; files are not split')
    return details
