import logging
from abc import ABC, abstractmethod
from threading import Lock
from typing import ContextManager, MutableMapping

import redis
from sqlitedict import SqliteDict

from ..utils import PathLike

Key = str
logger = logging.getLogger(__name__)


class Locker(ABC):
    def __init__(self, track_size: bool):
        self.track_size = track_size

    @abstractmethod
    def start_reading(self, key: Key) -> bool:
        pass

    @abstractmethod
    def stop_reading(self, key: Key):
        pass

    @abstractmethod
    def start_writing(self, key: Key) -> bool:
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


class DummyLocker(Locker):
    def __init__(self):
        super().__init__(False)

    def start_reading(self, key: Key) -> bool:
        return True

    def stop_reading(self, key: Key):
        pass

    def start_writing(self, key: Key) -> bool:
        return True

    def stop_writing(self, key: Key):
        pass


class DictRegistry:
    _lock: ContextManager
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

    def _is_reading(self, key: Key):
        return bool(self._get_reading(key))

    def _is_writing(self, key: Key):
        return bool(self._get_writing(key))

    def start_reading(self, key: Key) -> bool:
        with self._lock:
            if self._is_writing(key):
                return False

            self._reading[key] = self._get_reading(key) + 1
            return True

    def stop_reading(self, key: Key):
        with self._lock:
            self._reading[key] = self._get_reading(key) - 1

    def start_writing(self, key: Key) -> bool:
        with self._lock:
            if self._is_reading(key) or self._is_writing(key):
                return False

            value = self._get_writing(key)
            assert value == 0, value
            self._writing[key] = value + 1
            return True

    def stop_writing(self, key: Key):
        with self._lock:
            value = self._get_writing(key)
            assert value == 1, value
            self._writing[key] = value - 1


class ThreadLocker(DictRegistry, Locker):
    def __init__(self):
        super().__init__(False)
        self._lock = Lock()
        self._reading = {}
        self._writing = {}


class RedisLocker(Locker):
    def __init__(self, master: redis.Redis, prefix: str):
        super().__init__(True)
        self._redis = master
        self._prefix = prefix
        self._volume_key = f'{self._prefix}.V'

    def _write_key(self, key):
        return f'{self._prefix}.W.{key}'

    def _read_key(self, key):
        return f'{self._prefix}.R.{key}'

    def start_writing(self, key: Key) -> bool:
        write_key = self._write_key(key)
        read_key = self._read_key(key)

        with self._redis.pipeline() as pipe:
            while True:
                try:
                    # guarantee atomicity
                    pipe.watch(write_key, read_key)
                    writing = int(pipe.get(write_key) or 0)
                    reading = int(pipe.get(read_key) or 0)

                    assert 0 <= writing < 1
                    assert reading >= 0
                    if writing or reading:
                        return False

                    # commit changes
                    pipe.multi()
                    pipe.set(write_key, 1)
                    pipe.execute()

                    return True

                except redis.WatchError:
                    pass

    def stop_writing(self, key: Key):
        count = int(self._redis.decrby(self._write_key(key)))
        assert count == 0, count

    def start_reading(self, key: Key) -> bool:
        write_key = self._write_key(key)
        read_key = self._read_key(key)

        with self._redis.pipeline() as pipe:
            while True:
                try:
                    # guarantee atomicity
                    pipe.watch(write_key, read_key)
                    writing = int(pipe.get(write_key) or 0)

                    assert 0 <= writing < 1
                    if writing:
                        return False

                    # commit changes
                    pipe.multi()
                    pipe.incrby(read_key)
                    pipe.execute()

                    return True

                except redis.WatchError:
                    pass

    def stop_reading(self, key: Key):
        count = int(self._redis.decrby(self._read_key(key)))
        assert count >= 0, count

    def get_size(self):
        return int(self._redis.get(self._volume_key))

    def set_size(self, size: int):
        self._redis.set(self._volume_key, size)

    def inc_size(self, size: int):
        self._redis.incrby(self._volume_key, size)

    @classmethod
    def from_url(cls, url: str, prefix: str):
        return cls(redis.Redis.from_url(url), prefix)


class SqliteLocker(DictRegistry, Locker):
    def __init__(self, path: PathLike):
        def identity(x):
            return x

        super().__init__(True)
        self._lock = SqliteDict(path, 'lock')
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
