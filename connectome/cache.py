import shutil
from hashlib import blake2b
from pathlib import Path
from threading import RLock
from typing import Union, Sequence

import cloudpickle

from .old_engine import NodeHash
from .serializers import NumpySerializer, ChainSerializer, Serializer
from .utils import atomize


# TODO redefine operators?
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
        return param.data in self._cache

    @atomize()
    def set(self, param: NodeHash, value):
        assert not self.contains(param)
        self._cache[param.data] = value

    @atomize()
    def get(self, param: NodeHash):
        return self._cache[param.data]


PARAMETER_FILENAME = '.parameter'


# simple hash table with list-based buckets
# each bucket is a folder with integer-named subfolders
# each subfolder contains a pickled version of the parameter
# which is used to resolve collisions
class DiskStorage(CacheStorage):
    def __init__(self, storage: Path, serializers: Union[Serializer, Sequence[Serializer]] = None):
        super().__init__()
        self._lock = RLock()
        self.storage = storage
        # TODO: symlinks container?

        if serializers is None:
            serializers = NumpySerializer()
        if isinstance(serializers, Serializer):
            serializers = [serializers]
        self.serializer = ChainSerializer(*serializers)

    def _choose_storage(self):
        # TODO: multiple storage
        # in case of multiple storage _inspect_path is different for save and load
        return self.storage

    @staticmethod
    def _match_parameter(parameter, path):
        with open(path / PARAMETER_FILENAME, 'rb') as file:
            return cloudpickle.load(file) == parameter

    @staticmethod
    def _store_parameter(parameter, path):
        with open(path / PARAMETER_FILENAME, 'wb') as file:
            cloudpickle.dump(parameter, file)

    def _inspect_path(self, param: NodeHash):
        # TODO: how slow is this?
        pickled = cloudpickle.dumps(param.data)
        digest = blake2b(pickled, digest_size=32).hexdigest()
        root = self._choose_storage() / digest
        names = set()
        if root.exists():
            for local in root.iterdir():
                assert local.is_dir()
                assert local.name.isdigit(), local.name
                names.add(int(local.name))

                if self._match_parameter(pickled, local):
                    return local, pickled

        # find the smallest free name
        name = min(set(range(len(names) + 1)) - names)
        return root / str(name), pickled

    @atomize()
    def contains(self, param: NodeHash) -> bool:
        return self._inspect_path(param)[0].exists()

    @atomize()
    def set(self, param: NodeHash, value):
        path, pickled = self._inspect_path(param)
        # TODO: customize permissions
        path.mkdir(parents=True)
        try:
            # TODO: save timestamps, current user, user-defined meta, and other useful info
            self._store_parameter(pickled, path)
            self.serializer.save(value, path)

        except BaseException as e:
            shutil.rmtree(path)
            raise RuntimeError('An error occurred while creating the cache. Cleaning up.') from e

    @atomize()
    def get(self, param: NodeHash):
        return self.serializer.load(self._inspect_path(param)[0])
