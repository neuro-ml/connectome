from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Callable

from ..engine import NodeHash


class Cache:
    def reserve_write_or_read(self, param: NodeHash) -> bool:
        """
        Notifies the cache that a read/write operation will be performed during evaluation.

        Returns whether the cache is completely empty for the given key.
        """
        raise NotImplementedError

    def fail(self, param: NodeHash):
        """
        Handles a failure during cache writing.
        """
        raise NotImplementedError

    def set(self, param: NodeHash, value: Any):
        raise NotImplementedError

    def get(self, param: NodeHash) -> Any:
        raise NotImplementedError


class TransactionState(Enum):
    Write, Read = range(2)


class TransactionManager(ABC):
    @abstractmethod
    def reserve_read(self, key: Any, contains: Callable) -> bool:
        """
        Notifies the cache that a read operation will be performed during evaluation.

        Returns whether the cache can be read from.
        """

    @abstractmethod
    def reserve_write_or_read(self, key: Any, contains: Callable) -> bool:
        """
        Notifies the cache that a read/write operation will be performed during evaluation.

        Returns whether the cache can be writen to.
        """

    @abstractmethod
    def fail(self, key: Any):
        """
        Handles a failure during cache writing.
        """

    @abstractmethod
    def set(self, key: Any, value: Any, setter: Callable[[Any, Any], Any]):
        """
        Notes
        -----
        The setter must check the cache consistency, if necessary.
        """

    @abstractmethod
    def get(self, key: Any, getter: Callable[[Any], Any]) -> Any:
        pass
