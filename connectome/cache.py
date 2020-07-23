import shutil
from hashlib import blake2b
from pathlib import Path
from threading import RLock
from typing import Union, Sequence

import cloudpickle
import pylru
from diskcache import Disk, Cache
from diskcache.core import MODE_BINARY, UNKNOWN

from .engine.base import NodeHash
from .serializers import NumpySerializer, ChainSerializer, Serializer
from .utils import atomize


class CacheStorage:
    def contains(self, param: NodeHash) -> bool:
        raise NotImplementedError

    def set(self, param: NodeHash, value):
        raise NotImplementedError

    def get(self, param: NodeHash):
        raise NotImplementedError


class MemoryStorage(CacheStorage):
    def __init__(self, size: int):
        super().__init__()
        self._cache = {} if size is None else pylru.lrucache(size)
        self._lock = RLock()

    @atomize()
    def contains(self, param: NodeHash) -> bool:
        return param.value in self._cache

    @atomize()
    def set(self, param: NodeHash, value):
        assert not self.contains(param)
        self._cache[param.value] = value

    @atomize()
    def get(self, param: NodeHash):
        return self._cache[param.value]


class DiskStorage(CacheStorage):
    def __init__(self, storage: Path, serializers: Union[Serializer, Sequence[Serializer]] = None):
        super().__init__()
        self._lock = RLock()

        if serializers is None:
            serializers = NumpySerializer()
        if isinstance(serializers, Serializer):
            serializers = [serializers]
        serializer = ChainSerializer(*serializers)

        # TODO: multiple storage
        # this is the only way to pass a custom serializer with non-json params
        disk_type = type('SerializedChild', (SerializedDisk,), {'serializer': serializer})
        self.storage = Cache(str(storage), disk=disk_type)

    @atomize()
    def contains(self, param: NodeHash) -> bool:
        return param.value in self.storage

    @atomize()
    def set(self, param: NodeHash, value):
        self.storage[param.value] = value

    @atomize()
    def get(self, param: NodeHash):
        return self.storage[param.value]


class SerializedDisk(Disk):
    """Adapts diskcache to our needs."""

    PARAMETER_FILENAME = '.parameter'
    serializer: Serializer

    def __init__(self, directory, **kwargs):
        super().__init__(str(directory), **kwargs)
        self.folder_levels = 2
        self.name_size = 32

    @staticmethod
    def _pickle(key) -> bytes:
        # TODO: how slow is this?
        return cloudpickle.dumps(key)

    def _digest(self, pickled: bytes):
        return blake2b(pickled, digest_size=self.name_size * self.folder_levels).hexdigest()

    def _filename(self, digest: str):
        parts = []
        for i in range(self.folder_levels):
            i *= self.name_size
            parts.append(digest[i:i + self.name_size])

        relative = Path(*parts)
        root = Path(self._directory) / relative
        return str(relative), root

    def put(self, key):
        pickled = self._pickle(key)
        digest = self._digest(pickled)
        filename, full_path = self._filename(digest)
        full_path.mkdir(parents=True, exist_ok=True)
        path = full_path / self.PARAMETER_FILENAME

        if path.exists():
            # check consistency
            with open(path, 'rb') as file:
                dumped = file.read()
                assert dumped == pickled, (dumped, pickled)

        else:
            # or save
            with open(path, 'wb') as file:
                file.write(pickled)

        return super().put(digest)

    def store(self, value, read, key=UNKNOWN):
        assert key != UNKNOWN
        assert not read
        filename, full_path = self._filename(self._digest(self._pickle(key)))

        try:
            # TODO: save timestamps, current user, user-defined meta, and other useful info
            size = self.serializer.save(value, full_path)
            return size, MODE_BINARY, filename, None

        except BaseException as e:
            shutil.rmtree(full_path)
            raise RuntimeError('An error occurred while creating the cache. Cleaning up.') from e

    def fetch(self, mode, filename, value, read):
        assert mode == MODE_BINARY, mode
        assert not read
        return self.serializer.load(Path(self._directory) / filename)

    def remove(self, filename):
        shutil.rmtree(Path(self._directory) / filename)

    # don't need this
    def filename(self, key=UNKNOWN, value=UNKNOWN):
        raise NotImplementedError

    def get(self, key, raw):
        raise NotImplementedError
