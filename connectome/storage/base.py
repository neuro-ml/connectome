from threading import RLock

import pylru

from ..engine.base import NodeHash
from ..utils import atomize


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
