from typing import Union, Any, Tuple

from .base import Cache
from ..engine import NodeHash
from ..storage.locker import ThreadLocker


class MemoryCache(Cache):
    def __init__(self, size: Union[int, None]):
        super().__init__()
        if size is not None:
            raise NotImplementedError('LRU cache is currently not supported')

        self._cache = {}
        self.locker = ThreadLocker()

    def get(self, key: NodeHash, context) -> Tuple[Any, bool]:
        key = key.value
        with self.locker.read(key):
            if key in self._cache:
                return self._cache[key], True
            return None, False

    def set(self, key: NodeHash, value: Any, context):
        key = key.value
        with self.locker.write(key):
            self._cache[key] = value

    def clear(self):
        self._cache = {}
