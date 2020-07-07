from hashlib import blake2b
from pathlib import Path
from threading import RLock
from functools import lru_cache
from typing import Any

import cloudpickle
import numpy as np

from .engine import GraphParameter
from .utils import atomize


# TODO redefine operators
class CacheStorage:
    def __init__(self, atomized=True):
        self._atomized = atomized
        self.mutex = RLock()

    def contains(self, param: GraphParameter) -> bool:
        raise NotImplementedError

    def set(self, param: GraphParameter, value):
        raise NotImplementedError

    def get(self, param: GraphParameter) -> Any:
        raise NotImplementedError

    def __getattribute__(self, name):
        attr = super().__getattribute__(name)
        if callable(attr):
            if self.atomized:
                return atomize(self.mutex)(attr)
        else:
            return attr

    @property
    def atomized(self):
        return self._atomized

    @atomized.setter
    def atomized(self, value: bool):
        self._atomized = value


class MemoryStorage(CacheStorage):
    def __init__(self, atomized=True):
        super().__init__(atomized=atomized)
        self._cache = {}

    def contains(self, param: GraphParameter) -> bool:
        return param.data in self._cache

    def set(self, param: GraphParameter, value):
        assert not self.contains(param)
        self._cache[param.data] = value

    def get(self, param: GraphParameter) -> Any:
        return self._cache[param.data]


# TODO: deal with cache misses by saving the pickled value
class DiskStorage(CacheStorage):
    def __init__(self, path):
        super().__init__(atomized=True)
        self.path = Path(path)

    # TODO: maybe customize the caching
    @lru_cache(None)
    def _get_hash(self, o):
        o = cloudpickle.dumps(o)
        return blake2b(o, digest_size=32).hexdigest()

    def _get_path(self, param, create=False):
        path = self.path / self._get_hash(param.data)
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    # TODO: move save and load out
    def _save(self, value, path):
        np.save(path / 'value.npy', value)

    def _load(self, path):
        return np.load(path / 'value.npy')

    def contains(self, param: GraphParameter) -> bool:
        return self._get_path(param).exists()

    def set(self, param: GraphParameter, value):
        assert not self.contains(param)
        # TODO: remove the folder if something goes wrong
        self._save(value, self._get_path(param, True))

    def get(self, param: GraphParameter) -> Any:
        return self._load(self._get_path(param))
