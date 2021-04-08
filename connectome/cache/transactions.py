from threading import Lock
from typing import Any, Dict, List, Callable

from .base import TransactionState, TransactionManager


# this version uses a pessimistic approach to write/read balance
class ThreadedTransaction(TransactionManager):
    def __init__(self):
        super().__init__()
        self._lock = Lock()
        self._not_ready = set()
        self._transactions: Dict[Any, List[TransactionState]] = {}

    def reserve_read(self, key: Any, contains: Callable) -> bool:
        with self._lock:
            transactions = self._transactions.setdefault(key, [])
            if contains(key) and key not in self._not_ready:
                # it's safe to read
                transactions.append(TransactionState.Read)
                return True

            return False

    def reserve_write_or_read(self, key: Any, contains: Callable) -> bool:
        with self._lock:
            transactions = self._transactions.setdefault(key, [])
            if contains(key) and key not in self._not_ready:
                # it's safe to read
                transactions.append(TransactionState.Read)
                return False

            else:
                # better to recalculate
                self._not_ready.add(key)
                transactions.append(TransactionState.Write)
                return True

    def fail(self, key):
        with self._lock:
            # TODO: can't rely on number
            transactions = self._transactions[key]
            transactions.pop(0)
            if not transactions:
                self._transactions.pop(key)

    def set(self, key: Any, value: Any, setter: Callable[[Any, Any], Any]):
        with self._lock:
            transactions = self._transactions[key]
            assert transactions[0] == TransactionState.Write, transactions[0]

            setter(key, value)

            self._not_ready.discard(key)
            transactions.pop(0)
            if not transactions:
                self._transactions.pop(key)

    def get(self, key: Any, getter: Callable[[Any], Any]) -> Any:
        with self._lock:
            # wait until nobody is writing
            transactions = self._transactions[key]

            assert transactions[0] == TransactionState.Read, transactions[0]
            assert key not in self._not_ready
            # now we're safe
            # TODO: move outside of lock
            value = getter(key)

            transactions.pop(0)
            if not transactions:
                self._transactions.pop(key)

            return value
