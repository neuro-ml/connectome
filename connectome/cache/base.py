from abc import ABC, abstractmethod
from typing import Any, Tuple

from ..engine import NodeHash


class Cache(ABC):
    def prepare(self, param: NodeHash) -> Tuple[Any, Any]:
        return param, None

    def raw_get(self, param: NodeHash) -> Tuple[Any, bool]:
        return self.get(*self.prepare(param))

    def raw_set(self, param: NodeHash, value: Any):
        key, context = self.prepare(param)
        return self.set(key, value, context)

    @abstractmethod
    def get(self, key, context) -> Tuple[Any, bool]:
        """
        Tries to read from cache.
        Returns (value, True) if it was successful and (None, False) otherwise.
        """

    @abstractmethod
    def set(self, key, value: Any, context):
        """
        Writes to cache.
        The cache is responsible for handling collisions.
        """
