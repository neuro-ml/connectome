from typing import Union, Any

import pylru

from .base import Cache
from ..engine import NodeHash


class MemoryCache(Cache):
    def __init__(self, size: Union[int, None]):
        super().__init__()
        self._cache = {} if size is None else pylru.lrucache(size)

    def contains(self, param: NodeHash) -> bool:
        return param.value in self._cache

    def set(self, param: NodeHash, value):
        # assert not self.contains(param)
        self._cache[param.value] = value

    def get(self, param: NodeHash) -> Any:
        return self._cache[param.value]
