from abc import ABC, abstractmethod
from typing import Any, Tuple

from ..engine import NodeHash


class Cache(ABC):
    @abstractmethod
    def get(self, param: NodeHash) -> Tuple[Any, bool]:
        """
        Tries to read from cache.
        Returns (value, True) if it was successful and (None, False) otherwise.
        """

    @abstractmethod
    def set(self, param: NodeHash, value: Any):
        """
        Writes to cache.
        The cache is responsible for handling collisions.
        """
