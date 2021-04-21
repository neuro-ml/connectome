from abc import ABC, abstractmethod
from threading import Lock
from typing import ContextManager, MutableMapping

from pottery import RedisDict, Redlock
from redis import Redis

Key = str


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

    def is_reading(self, key: Key):
        return bool(self._reading.get(key, 0))

    def start_reading(self, key: Key):
        self._reading[key] = self._reading.get(key, 0) + 1

    def stop_reading(self, key: Key):
        self._reading[key] -= 1

    def is_writing(self, key: Key):
        return bool(self._writing.get(key, 0))

    def start_writing(self, key: Key):
        self._writing[key] = self._reading.get(key, 0) + 1

    def stop_writing(self, key: Key):
        self._writing[key] -= 1


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
