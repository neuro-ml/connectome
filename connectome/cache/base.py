from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Tuple, Optional

from ..engine import NodeHash


class Cache:
    def reserve_write_or_read(self, param: NodeHash) -> Tuple[bool, Any]:
        """
        Notifies the cache that a read/write operation will be performed during evaluation.

        Returns whether the cache is completely empty for the given key, as well as a transaction id.
        """
        raise NotImplementedError

    def fail(self, param: NodeHash, transaction: Any):
        """
        Handles a failure during cache writing.
        """
        raise NotImplementedError

    def set(self, param: NodeHash, value: Any, transaction: Any):
        raise NotImplementedError

    def get(self, param: NodeHash, transaction: Any) -> Any:
        raise NotImplementedError


class TransactionState(Enum):
    Write, Read = range(2)


class TransactionManager(ABC):
    @abstractmethod
    def reserve_read(self, key: Any, contains: Callable) -> Optional[Any]:
        """
        Notifies the cache that a read operation will be performed during evaluation.

        Returns None, if the cache can't be read from, otherwise - returns the transaction id.
        """

    @abstractmethod
    def reserve_write_or_read(self, key: Any, contains: Callable) -> Tuple[bool, Any]:
        """
        Notifies the cache that a read/write operation will be performed during evaluation.

        Returns whether the cache can be writen to.
        """

    @abstractmethod
    def fail(self, key: Any, transaction: Any):
        """
        Handles a failure during cache writing.
        """

    @abstractmethod
    def release_write(self, key: Any, value: Any, transaction: Any, setter: Callable[[Any, Any], Any]):
        """
        Notes
        -----
        The setter must check the cache consistency, if necessary.
        """

    @abstractmethod
    def release_read(self, key: Any, transaction: Any, getter: Callable[[Any], Any]) -> Any:
        pass
