from threading import Lock
from typing import Any, Tuple, Union

from pylru import lrucache

from ..engine import NodeHash
from .base import Cache


class MemoryCache(Cache):
    def __init__(self, size: Union[int, None]):
        super().__init__()
        self._lock = Lock()
        self.size = size
        if size is not None:
            self._cache = lrucache(size)
        else:
            self._cache = {}

    def get(self, key: NodeHash, context) -> Tuple[Any, bool]:
        key = key.value
        with self._lock:
            if key in self._cache:
                return self._cache[key], True
            return None, False

    def set(self, key: NodeHash, value: Any, context):
        key = key.value
        with self._lock:
            self._cache[key] = value

    def clear(self):
        with self._lock:
            self._cache = {}

    def __reduce__(self):
        return self.__class__, (self.size,)
