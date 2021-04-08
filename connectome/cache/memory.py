from typing import Union, Any
# import pylru

from .base import Cache
from .transactions import ThreadedTransaction
from ..engine import NodeHash


class MemoryCache(Cache):
    def __init__(self, size: Union[int, None]):
        super().__init__()
        if size is not None:
            raise NotImplementedError('LRU cache is currently not supported')

        self._cache = {}
        self._transactions = ThreadedTransaction()

    def reserve_write_or_read(self, param: NodeHash) -> bool:
        return self._transactions.reserve_write_or_read(param.value, self._cache.__contains__)

    def fail(self, param: NodeHash):
        return self._transactions.fail(param.value)

    def set(self, param: NodeHash, value: Any):
        return self._transactions.set(param.value, value, self._cache.__setitem__)

    def get(self, param: NodeHash) -> Any:
        return self._transactions.get(param.value, self._cache.__getitem__)
