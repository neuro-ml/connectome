import getpass
import json
import os
import shutil
import time
from hashlib import blake2b
from pathlib import Path
from threading import RLock
from typing import Sequence, NamedTuple

from diskcache import Disk, Cache
from diskcache.core import MODE_BINARY, UNKNOWN, DBNAME

from .utils import ChainDict
from ..engine.base import NodeHash
from ..serializers import Serializer
from ..utils import atomize
from .base import CacheStorage
from .pickler import dumps


class DiskOptions(NamedTuple):
    path: Path
    min_free_space: int = 0
    max_volume: int = float('inf')


class DiskStorage(CacheStorage):
    def __init__(self, options: Sequence[DiskOptions], serializer: Serializer, metadata: dict):
        super().__init__()
        self._lock = RLock()

        # this is the only way to pass a custom serializer with non-json params
        disk_type = type('SerializedChild', (SerializedDisk,), {'serializer': serializer, 'meta': metadata})
        storage = []
        self.options = {}
        for entry in options:
            cache = Cache(str(entry.path), size_limit=float('inf'), cull_limit=0, disk=disk_type)
            copy_group_permissions(entry.path / DBNAME, entry.path)

            storage.append(cache)
            self.options[cache] = entry

        self.storage = ChainDict(storage, self._select_storage)

    def _select_storage(self, cache: Cache):
        options = self.options[cache]
        free_space = shutil.disk_usage(cache.directory).free
        return free_space >= options.min_free_space and cache.volume() <= options.max_volume

    @atomize()
    def contains(self, param: NodeHash) -> bool:
        return param.value in self.storage

    @atomize()
    def set(self, param: NodeHash, value):
        self.storage[param.value] = value

    @atomize()
    def get(self, param: NodeHash):
        return self.storage[param.value]


PERMISSIONS = 0o770
LEVEL_SIZE = 32
FOLDER_LEVELS = 2
DATA_FOLDER = 'data'
HASH_FILENAME = 'hash.bin'
META_FILENAME = 'meta.json'


def digest_bytes(pickled: bytes) -> str:
    return blake2b(pickled, digest_size=FOLDER_LEVELS * LEVEL_SIZE).hexdigest()


def key_to_relative(key):
    pickled = dumps(key)
    digest = digest_bytes(pickled)

    parts = []
    for i in range(FOLDER_LEVELS):
        i *= LEVEL_SIZE
        parts.append(digest[i:i + LEVEL_SIZE])

    return pickled, digest, Path(*parts)


def check_consistency(hash_path, pickled):
    with open(hash_path, 'rb') as file:
        dumped = file.read()
        assert dumped == pickled, (dumped, pickled)


def copy_group_permissions(target, reference, recursive=False):
    shutil.chown(target, group=reference.group())
    os.chmod(target, PERMISSIONS)
    if recursive and target.is_dir():
        for child in target.iterdir():
            copy_group_permissions(child, reference, recursive)


def get_folder_size(path):
    size = 0
    for root, _, files in os.walk(path):
        for name in files:
            size += os.path.getsize(os.path.join(root, name))

    return size


class SerializedDisk(Disk):
    """Adapts diskcache to our needs."""
    serializer: Serializer
    meta = {}

    def put(self, key):
        # find the right folder
        pickled, digest, relative = key_to_relative(key)
        local = Path(self._directory) / relative
        hash_path = local / HASH_FILENAME
        data_folder = local / DATA_FOLDER

        data_folder.mkdir(parents=True, exist_ok=True, mode=PERMISSIONS)
        if hash_path.exists():
            check_consistency(hash_path, pickled)

        else:
            # or save
            with open(hash_path, 'wb') as file:
                file.write(pickled)

        return super().put(digest)

    def store(self, value, read, key=UNKNOWN):
        assert key != UNKNOWN
        assert not read
        _, _, relative = key_to_relative(key)
        root = Path(self._directory)
        local = root / relative
        data_folder = local / DATA_FOLDER

        try:
            # data
            self.serializer.save(value, data_folder)
            # meta
            meta = self.meta.copy()
            meta.update({
                'time': time.time(),
                # TODO: this can possibly fail
                'user': getpass.getuser(),
            })
            with open(local / META_FILENAME, 'w') as file:
                json.dump(meta, file)

            copy_group_permissions(local, root, recursive=True)
            size = get_folder_size(local)
            return size, MODE_BINARY, str(relative), None

        except BaseException as e:
            self.remove(relative)
            raise RuntimeError('An error occurred while creating the cache. Cleaned up.') from e

    def fetch(self, mode, relative, value, read):
        assert mode == MODE_BINARY, mode
        assert not read
        return self.serializer.load(Path(self._directory) / relative / DATA_FOLDER)

    def remove(self, relative):
        shutil.rmtree(Path(self._directory) / relative)

    # don't need this
    def filename(self, key=UNKNOWN, value=UNKNOWN):
        raise NotImplementedError

    def get(self, key, raw):
        raise NotImplementedError
