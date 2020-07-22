import shutil
from hashlib import blake2b
from pathlib import Path
from threading import RLock
from typing import Union, Sequence

import cloudpickle
from diskcache import Disk
from diskcache.core import MODE_BINARY, UNKNOWN, Cache

from .engine import NodeHash
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
    def __init__(self):
        super().__init__()
        self._cache = {}
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
        self.storage = Cache(str(storage), disk=SerializedDisk, disk_serializer=serializer)

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

    def __init__(self, directory, serializer: Serializer):
        super().__init__(directory)
        self.serializer = serializer
        self.folder_levels = 2
        self.name_size = 32

    def _digest(self, key):
        # TODO: how slow is this?
        pickled = cloudpickle.dumps(key)
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
        # TODO: reduce to one pickling
        digest = self._digest(key)
        filename, full_path = self._filename(digest)
        full_path.mkdir(parents=True, exist_ok=True)
        path = full_path / self.PARAMETER_FILENAME

        if path.exists():
            # check consistency
            with open(path, 'rb') as file:
                assert cloudpickle.load(file) == key

        else:
            # or save
            with open(path, 'wb') as file:
                cloudpickle.dump(key, file)

        return super().put(digest)

    def store(self, value, read, key=UNKNOWN):
        assert key != UNKNOWN
        assert not read
        filename, full_path = self._filename(self._digest(key))

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
