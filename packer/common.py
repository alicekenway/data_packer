"""Portable manifests, path validation, and per-operation locks."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import sqlite3
import tempfile


class PackerError(Exception):
    pass


def positive(value):
    result = int(value)
    if result < 1:
        raise ValueError('must be a positive integer')
    return result


def byte_size(value):
    match = re.fullmatch(r'\s*(\d+(?:\.\d+)?)\s*([kmgt]?i?b)?\s*', value.lower())
    if not match:
        raise ValueError('use bytes or units such as 10GiB, 500MiB, 10GB')
    unit = match.group(2) or 'b'
    power = 'kmgt'.find(unit[0]) + 1 if unit[0] != 'b' else 0
    result = int(float(match.group(1)) * (1024 if 'i' in unit else 1000) ** power)
    if result < 1:
        raise ValueError('size must be positive')
    return result


def clean_relative(value, allow_root=False):
    if not isinstance(value, str) or '\x00' in value:
        raise PackerError('invalid path in metadata or archive')
    path = PurePosixPath(value)
    if path.is_absolute() or '..' in path.parts or not path.parts:
        if allow_root and value == '.':
            return '.'
        raise PackerError('unsafe relative path: {!r}'.format(value))
    return path.as_posix()


def check_link(path, target, root=None):
    if not isinstance(target, str) or not target or '\x00' in target or target.startswith('/'):
        raise PackerError('unsafe symlink: {} -> {!r}'.format(path, target))
    depth = len(PurePosixPath(path).parent.parts)
    for component in PurePosixPath(target).parts:
        if component == '..':
            depth -= 1
            if depth < 0:
                raise PackerError('symlink escapes source: {} -> {}'.format(path, target))
        elif component != '.':
            depth += 1
    if root is not None:
        check_resolved_link(Path(root) / path, Path(root))


def check_resolved_link(path, root, target=None):
    """Validate filesystem semantics, including chains and symlink-before-.. ."""
    try:
        resolved = (path.parent / target).resolve() if target is not None else path.resolve()
        resolved.relative_to(root.resolve())
    except (ValueError, RuntimeError, OSError) as exc:
        raise PackerError('symlink resolves outside the root or forms a cycle: {}'.format(path)) from exc


def separate_roots(source, destination):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination or source in destination.parents or destination in source.parents:
        raise PackerError('input and output directories must not overlap')


def ensure_directory(path):
    path = Path(path)
    if path.is_symlink() or (path.exists() and not path.is_dir()):
        raise PackerError('expected a real directory: {}'.format(path))
    path.mkdir(parents=True, exist_ok=True)


def atomic_json(path, value):
    path = Path(path)
    ensure_directory(path.parent)
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name + '.', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, indent=2, ensure_ascii=True)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def read_json(path):
    with Path(path).open(encoding='utf-8') as stream:
        return json.load(stream)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def lock(path):
    ensure_directory(Path(path).parent)
    # Never follow a pre-existing symlink when opening a control file.
    fd = os.open(str(path), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise PackerError('another operation holds {}'.format(path))
        yield
    finally:
        os.close(fd)


@contextmanager
def open_inventory(packed):
    path = Path(packed) / 'inventory.sqlite3'
    if not path.is_file() or path.is_symlink():
        raise PackerError('missing inventory: {}'.format(path))
    db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        yield db
    finally:
        db.close()


def config(db):
    return json.loads(db.execute('SELECT value FROM settings WHERE key="config"').fetchone()[0])


def archive_name(shard, compression):
    return 'part-{:06d}.tar{}'.format(shard, '.gz' if compression == 'gzip' else '')


def receipt_path(work, shard):
    return Path(work) / 'receipts' / ('{:06d}.json'.format(shard))
