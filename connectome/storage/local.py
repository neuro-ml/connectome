import tempfile
from collections import OrderedDict
import os
import shutil
from hashlib import blake2b
from pathlib import Path
from typing import Sequence, NamedTuple

from diskcache import Disk, Cache
from diskcache.core import MODE_BINARY, UNKNOWN, DBNAME

from .relative_remote import RelativeRemote, RemoteOptions
from .utils import ChainDict

LEVEL_SIZE, FOLDER_LEVELS = 32, 2
PERMISSIONS = 0o770
FILENAME = 'data'


class DiskOptions(NamedTuple):
    path: Path
    min_free_space: int = 0
    max_volume: int = float('inf')


class GroupCache(Cache):
    def __init__(self, path: Path, disk: type):
        super().__init__(str(path), size_limit=float('inf'), cull_limit=0, disk=disk)
        copy_group_permissions(path / DBNAME, path)


class Storage:
    def __init__(self, options: Sequence[DiskOptions]):
        self.options = OrderedDict()
        for entry in options:
            cache = GroupCache(entry.path, disk=FileDisk)
            self.options[cache] = entry

        self.local = ChainDict(list(self.options), self._select_storage)

    def store(self, path: Path):
        key = _digest_file(path)
        if key in self.local:
            assert match_files(path, self.local[key])
        else:
            self.local[key] = path
        return key, self.local[key]

    def get(self, key: str, name: str = None):
        path: Path = self.local[key]
        if name is None:
            return path

        link = path.parent / name
        if not link.exists():
            link.symlink_to(path.name)

        return link

    def _select_storage(self, cache: GroupCache):
        options = self.options[cache]
        free_space = shutil.disk_usage(cache.directory).free
        return free_space >= options.min_free_space and cache.volume() <= options.max_volume


class BackupStorage(Storage):
    def __init__(self, local: Sequence[DiskOptions], remote: Sequence[RemoteOptions]):
        super().__init__(local)
        self.remotes = [RelativeRemote(**options._asdict()) for options in remote]

    def get(self, key: str, name: str = None):
        if key not in self.local:
            self._download(key)
        return super().get(key, name)

    # called only if the file is not present
    def _download(self, key):
        relative = digest_to_relative(key) / FILENAME
        with tempfile.TemporaryDirectory() as temp_dir:
            file = Path(temp_dir) / relative.name
            for remote in self.remotes:
                with remote:
                    try:
                        remote.get(relative, file)
                    except FileNotFoundError:
                        continue

                    self.local[key] = file
                    return

        raise KeyError(key)


class FileDisk(Disk):
    """
    Stores files directly on disk. The path is obtained by simply splitting the key.

    d[key] contains the relative path
    """

    def store(self, path: str, read, key=UNKNOWN):
        path = Path(path)

        assert path.is_file(), path
        assert key != UNKNOWN
        assert not read

        root = Path(self._directory)
        relative = digest_to_relative(key)
        folder = root / relative
        file = folder / FILENAME
        folder.mkdir(parents=True, exist_ok=True, mode=PERMISSIONS)

        try:
            shutil.copyfile(path, file)
            copy_group_permissions(folder, root, recursive=True)
            size = get_file_size(file)
            return size, MODE_BINARY, str(relative), None

        except BaseException as e:
            self.remove(relative)
            raise RuntimeError('An error occurred while creating the cache. Cleaned up.') from e

    def fetch(self, mode, relative, value, read):
        assert mode == MODE_BINARY, mode
        assert not read
        return Path(self._directory) / relative / FILENAME

    def remove(self, relative):
        shutil.rmtree(Path(self._directory) / relative)

    def filename(self, key=UNKNOWN, value=UNKNOWN):
        assert key != UNKNOWN
        return str(digest_to_relative(key))

    # don't need this
    def get(self, key, raw):
        raise NotImplementedError


def copy_group_permissions(target, reference, recursive=False):
    shutil.chown(target, group=reference.group())
    os.chmod(target, PERMISSIONS)
    if recursive and target.is_dir():
        for child in target.iterdir():
            copy_group_permissions(child, reference, recursive)


def get_file_size(path):
    return os.path.getsize(path)


def get_folder_size(path):
    size = 0
    for root, _, files in os.walk(path):
        for name in files:
            size += get_file_size(os.path.join(root, name))

    return size


def _digest_file(path: Path, block_size=2 ** 20):
    hasher = blake2b(digest_size=FOLDER_LEVELS * LEVEL_SIZE)

    with open(path, 'rb') as f:
        while True:
            buffer = f.read(block_size)
            if not buffer:
                break
            hasher.update(buffer)

    return hasher.hexdigest()


def digest_to_relative(key):
    assert len(key) % FOLDER_LEVELS == 0
    size = len(key) // FOLDER_LEVELS

    parts = []
    for i in range(FOLDER_LEVELS):
        i *= size
        parts.append(key[i:i + size])

    return Path(*parts)


def match_files(first: Path, second: Path):
    # TODO
    return True
