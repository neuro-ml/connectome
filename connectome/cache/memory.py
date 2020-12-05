from threading import RLock
from typing import Union, Any

import pylru

from .base import Cache
from ..engine import NodeHash
from ..utils import atomize


class MemoryCache(Cache):
    def __init__(self, size: Union[int, None]):
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
    def get(self, param: NodeHash) -> Any:
        return self._cache[param.value]
