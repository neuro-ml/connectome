import shutil
from hashlib import blake2b
from pathlib import Path
from threading import RLock
from typing import Sequence, NamedTuple

from diskcache import Disk, Cache
from diskcache.core import MODE_BINARY, UNKNOWN

from .utils import ChainDict
from ..engine.base import NodeHash
from ..serializers import Serializer
from ..utils import atomize
from .base import CacheStorage
from .pickler import dumps


class DiskOptions(NamedTuple):
    path: str
    min_free_space: int = 0
    max_volume: int = float('inf')


class DiskStorage(CacheStorage):
    def __init__(self, options: Sequence[DiskOptions], serializer: Serializer):
        super().__init__()
        self._lock = RLock()

        # this is the only way to pass a custom serializer with non-json params
        disk_type = type('SerializedChild', (SerializedDisk,), {'serializer': serializer})
        storage = []
        self.options = {}
        for entry in options:
            cache = Cache(entry.path, size_limit=float('inf'), cull_limit=0, disk=disk_type)
            storage.append(cache)
            self.options[cache] = entry

        self.storage = ChainDict(storage, self._select_storage)

    def _select_storage(self, cache: Cache):
        options = self.options[cache]
        free_space = shutil.disk_usage(cache.directory).free
        # TODO: add volume check
        return free_space >= options.min_free_space

    @atomize()
    def contains(self, param: NodeHash) -> bool:
        return param.value in self.storage

    @atomize()
    def set(self, param: NodeHash, value):
        self.storage[param.value] = value

    @atomize()
    def get(self, param: NodeHash):
        return self.storage[param.value]


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


class SerializedDisk(Disk):
    """Adapts diskcache to our needs."""
    serializer: Serializer

    def put(self, key):
        # find the right folder
        pickled, digest, relative = key_to_relative(key)
        local = Path(self._directory) / relative
        hash_path = local / HASH_FILENAME
        data_folder = local / DATA_FOLDER

        # TODO: permissions
        data_folder.mkdir(parents=True, exist_ok=True)

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
        data_folder = Path(self._directory) / relative / DATA_FOLDER

        try:
            # TODO: save timestamps, current user, user-defined meta, and other useful info
            size = self.serializer.save(value, data_folder)
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
