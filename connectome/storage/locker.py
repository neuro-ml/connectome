import logging
from abc import ABC, abstractmethod
from threading import Lock
from typing import ContextManager, MutableMapping

from pottery import RedisDict, Redlock
from redis import Redis
from sqlitedict import SqliteDict

from ..utils import PathLike

Key = str
logger = logging.getLogger(__name__)


class Locker(ABC):
    lock: ContextManager

    def __init__(self, track_size: bool):
        self.track_size = track_size

    @abstractmethod
    def is_reading(self, key: Key):
        pass

    @abstractmethod
    def start_reading(self, key: Key):
        pass

    @abstractmethod
    def stop_reading(self, key: Key):
        pass

    @abstractmethod
    def is_writing(self, key: Key):
        pass

    @abstractmethod
    def start_writing(self, key: Key):
        pass

    @abstractmethod
    def stop_writing(self, key: Key):
        pass

    def get_size(self):
        raise NotImplementedError

    def inc_size(self, size: int):
        raise NotImplementedError

    def set_size(self, size: int):
        raise NotImplementedError


class NoLock:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class DummyLocker(Locker):
    def __init__(self):
        super().__init__(False)
        self.lock = NoLock()

    def is_reading(self, key: Key):
        pass

    def start_reading(self, key: Key):
        pass

    def stop_reading(self, key: Key):
        pass

    def is_writing(self, key: Key):
        pass

    def start_writing(self, key: Key):
        pass

    def stop_writing(self, key: Key):
        pass


class DictRegistry:
    _reading: MutableMapping[Key, int]
    _writing: MutableMapping[Key, int]

    def _get_reading(self, key):
        value = self._reading.get(key, 0)
        logger.info(f'Read count {value}')
        assert value >= 0, value
        return value

    def _get_writing(self, key):
        value = self._writing.get(key, 0)
        logger.info(f'Write count {value}')
        assert 0 <= value <= 1, value
        return value

    def is_reading(self, key: Key):
        return bool(self._get_reading(key))

    def start_reading(self, key: Key):
        self._reading[key] = self._get_reading(key) + 1

    def stop_reading(self, key: Key):
        self._reading[key] = self._get_reading(key) - 1

    def is_writing(self, key: Key):
        return bool(self._get_writing(key))

    def start_writing(self, key: Key):
        value = self._get_writing(key)
        assert value == 0, value
        self._writing[key] = value + 1

    def stop_writing(self, key: Key):
        value = self._get_writing(key)
        assert value == 1, value
        self._writing[key] = value - 1


class ThreadLocker(DictRegistry, Locker):
    def __init__(self):
        super().__init__(False)
        self.lock = Lock()
        self._reading = {}
        self._writing = {}


class RedisLocker(DictRegistry, Locker):
    def __init__(self, redis: Redis, prefix: str):
        super().__init__(True)
        self.lock = Redlock(masters={redis}, key=f'{prefix}.L')
        self._reading = RedisDict(redis=redis, key=f'{prefix}.R')
        self._writing = RedisDict(redis=redis, key=f'{prefix}.W')
        self._meta = RedisDict(redis=redis, key=f'{prefix}.M')

    def get_size(self):
        return self._meta.get('volume', 0)

    def set_size(self, size: int):
        self._meta['volume'] = size

    def inc_size(self, size: int):
        self.set_size(self.get_size() + size)

    @classmethod
    def from_url(cls, url: str, prefix: str):
        return cls(Redis.from_url(url), prefix)


class SqliteLocker(DictRegistry, Locker):
    def __init__(self, path: PathLike):
        def identity(x):
            return x

        super().__init__(True)
        self.lock = SqliteDict(path, 'lock')
        self._reading = SqliteDict(
            path, autocommit=True, tablename='reading', encode=identity, decode=identity
        )
        self._writing = SqliteDict(
            path, autocommit=True, tablename='writing', encode=identity, decode=identity
        )
        self._meta = SqliteDict(
            path, autocommit=True, tablename='meta', encode=identity, decode=identity
        )

    def get_size(self):
        return self._meta.get('volume', 0)

    def set_size(self, size: int):
        self._meta['volume'] = size

    def inc_size(self, size: int):
        self.set_size(self.get_size() + size)
