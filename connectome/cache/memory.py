from typing import Union, Any, Tuple

from .base import Cache
from ..engine import NodeHash
from ..storage.locker import ThreadLocker, wait_for_true


class MemoryCache(Cache):
    def __init__(self, size: Union[int, None]):
        super().__init__()
        if size is not None:
            raise NotImplementedError('LRU cache is currently not supported')

        self._cache = {}
        self._locker = ThreadLocker()
        self._sleep_time = 0.1
        self._sleep_iters = int(600 / self._sleep_time) or 1  # 10 minutes

    def get(self, param: NodeHash) -> Tuple[Any, bool]:
        key = param.value
        wait_for_true(self._locker.start_reading, key, self._sleep_time, self._sleep_iters)
        try:
            if key in self._cache:
                return self._cache[key], True
            return None, False

        finally:
            self._locker.stop_reading(key)

    def set(self, param: NodeHash, value: Any):
        key = param.value
        wait_for_true(self._locker.start_writing, key, self._sleep_time, self._sleep_iters)
        try:
            self._cache[key] = value
        finally:
            self._locker.stop_writing(key)
