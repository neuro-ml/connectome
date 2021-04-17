from threading import Lock
from typing import Any, Dict, Callable, Tuple, Optional

from .base import TransactionState, TransactionManager


class DummyTransaction(TransactionManager):
    def reserve_read(self, key: Any, contains: Callable) -> Optional[Any]:
        if contains(key):
            return 1

    def reserve_write_or_read(self, key: Any, contains: Callable) -> Tuple[bool, Any]:
        return not contains(key), 1

    def fail(self, key: Any, transaction: Any):
        pass

    def release_write(self, key: Any, value: Any, transaction: Any, setter: Callable[[Any, Any], Any]):
        setter(key, value)

    def release_read(self, key: Any, transaction: Any, getter: Callable[[Any], Any]) -> Any:
        return getter(key)


# this version uses a pessimistic approach to write/read balance
class ThreadedTransaction(TransactionManager):
    def __init__(self):
        super().__init__()
        self._lock = Lock()
        self._not_ready = set()
        self._transactions: Dict[Any, Dict[int, TransactionState]] = {}

    def reserve_read(self, key: Any, contains: Callable) -> Optional[int]:
        with self._lock:
            transactions = self._transactions.setdefault(key, {})
            if contains(key) and key not in self._not_ready:
                # it's safe to read
                i = self._new_id(transactions)
                transactions[i] = TransactionState.Read
                return i

    def reserve_write_or_read(self, key: Any, contains: Callable) -> Tuple[bool, int]:
        with self._lock:
            transactions = self._transactions.setdefault(key, {})
            if contains(key) and key not in self._not_ready:
                # it's safe to read
                i = self._new_id(transactions)
                transactions[i] = TransactionState.Read
                return False, i

            else:
                # better to recalculate
                # assert TransactionState.Read not in transactions.values()
                self._not_ready.add(key)
                i = self._new_id(transactions)
                transactions[i] = TransactionState.Write
                return True, i

    def fail(self, key: Any, transaction: int):
        with self._lock:
            self._pop(key, transaction)

    def release_write(self, key: Any, value: Any, transaction: int, setter: Callable[[Any, Any], Any]):
        with self._lock:
            transactions = self._transactions[key]
            current = transactions[transaction]
            assert current == TransactionState.Write, current

            setter(key, value)

            self._not_ready.discard(key)
            self._pop(key, transaction)

    def release_read(self, key: Any, transaction: int, getter: Callable[[Any], Any]) -> Any:
        with self._lock:
            transactions = self._transactions[key]
            current = transactions[transaction]

            assert current == TransactionState.Read, current
            assert key not in self._not_ready
            # now we're safe
            value = getter(key)

            self._pop(key, transaction)
            return value

    def _pop(self, key: Any, transaction: int):
        transactions = self._transactions[key]
        transactions.pop(transaction)
        if not transactions:
            self._transactions.pop(key)

    @staticmethod
    def _new_id(transactions):
        free = set(range(len(transactions) + 1)) - set(transactions)
        return free.pop()
