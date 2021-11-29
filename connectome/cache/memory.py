from typing import Union, Any, Tuple

from pylru import lrucache

from .base import Cache
from ..engine import NodeHash
from ..storage.locker import ThreadLocker, GlobalThreadLocker


class MemoryCache(Cache):
    def __init__(self, size: Union[int, None]):
        super().__init__()
        # TODO: maybe just always use a global lock without the Locker interface
        if size is not None:
            self._cache = lrucache(size)
            self.locker = GlobalThreadLocker()

        else:
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
        # TODO: need a global lock
        self._cache = {}
