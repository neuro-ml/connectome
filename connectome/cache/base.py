from typing import Any

from ..engine import NodeHash


class Cache:
    def contains(self, param: NodeHash) -> bool:
        raise NotImplementedError

    def set(self, param: NodeHash, value):
        raise NotImplementedError

    def get(self, param: NodeHash) -> Any:
        raise NotImplementedError
