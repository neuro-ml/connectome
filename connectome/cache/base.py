from typing import Any

from ..engine import NodeHash


class Cache:
    def reserve_read(self, param: NodeHash) -> bool:
        """
        Tries to reserve a read operation. Returns True if it was successful.
        """
        raise NotImplementedError

    def fail(self, param: NodeHash, read: bool):
        """
        Handles a failure during cache writing.
        """
        raise NotImplementedError

    def set(self, param: NodeHash, value: Any):
        raise NotImplementedError

    def get(self, param: NodeHash) -> Any:
        raise NotImplementedError
