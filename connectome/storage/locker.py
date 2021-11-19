import logging
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from threading import Lock
from typing import ContextManager, MutableMapping

from redis import Redis

Key = str
logger = logging.getLogger(__name__)


class Locker(ABC):
    def __init__(self, track_size: bool):
        self.track_size = track_size

    @contextmanager
    def read(self, key: Key):
        self.reserve_read(key)
        try:
            yield
        finally:
            self.stop_reading(key)

    @contextmanager
    def write(self, key: Key):
        self.reserve_write(key)
        try:
            yield
        finally:
            self.stop_writing(key)

    def reserve_read(self, key: Key):
        sleep_time = 0.1
        sleep_iters = int(600 / sleep_time) or 1  # 10 minutes
        wait_for_true(self.start_reading, key, sleep_time, sleep_iters)

    def reserve_write(self, key: Key):
        sleep_time = 0.1
        sleep_iters = int(600 / sleep_time) or 1  # 10 minutes
        wait_for_true(self.start_writing, key, sleep_time, sleep_iters)

    @abstractmethod
    def start_reading(self, key: Key) -> bool:
        """ Try to reserve a read operation. Return True if it was successful. """

    @abstractmethod
    def stop_reading(self, key: Key):
        """ Release a read operation. """

    @abstractmethod
    def start_writing(self, key: Key) -> bool:
        """ Try to reserve a write operation. Return True if it was successful. """

    @abstractmethod
    def stop_writing(self, key: Key):
        """ Release a write operation. """

    # TODO: move this to another interface?
    def get_size(self):
        raise NotImplementedError

    def inc_size(self, size: int):
        raise NotImplementedError

    def dec_size(self, size: int):
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
            value = self._get_reading(key)
            if value == 1:
                self._reading.pop(key)
            else:
                self._reading[key] = value - 1

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
            self._writing.pop(key)


class ThreadLocker(DictRegistry, Locker):
    def __init__(self):
        super().__init__(False)
        self._lock = Lock()
        self._reading = {}
        self._writing = {}


class RedisLocker(Locker):
    def __init__(self, *args, prefix: str, expire: int):
        super().__init__(True)
        if len(args) == 1 and isinstance(args[0], Redis):
            redis, = args
        else:
            redis = Redis(*args)

        self._redis = redis
        self._prefix = prefix + ':'
        self._expire = expire
        self._volume_key = f'{prefix}.V'
        # TODO: how slow are these checks?
        # language=Lua
        self._stop_writing = self._redis.script_load('''
        if redis.call('get', KEYS[1]) == '-1' then
            redis.call('del', KEYS[1])
        else
            error('')
        end''')
        # language=Lua
        self._start_reading = self._redis.script_load(f'''
        local lock = redis.call('get', KEYS[1])
        if lock == '-1' then 
            return 0
        elseif lock == false then
            redis.call('set', KEYS[1], 1, 'EX', {expire})
            return 1
        else
            redis.call('set', KEYS[1], lock + 1, 'EX', {expire})
            return 1
        end''')
        # language=Lua
        self._stop_reading = self._redis.script_load(f'''
        local lock = redis.call('get', KEYS[1])
        if lock == '1' then
            redis.call('del', KEYS[1])
        elseif tonumber(lock) < 1 then
            error('')
        else
            redis.call('set', KEYS[1], lock - 1, 'EX', {expire})
        end''')

    def start_writing(self, key: Key) -> bool:
        return bool(self._redis.set(self._prefix + key, -1, nx=True, ex=self._expire))

    def stop_writing(self, key: Key):
        self._redis.evalsha(self._stop_writing, 1, self._prefix + key)

    def start_reading(self, key: Key) -> bool:
        return bool(self._redis.evalsha(self._start_reading, 1, self._prefix + key))

    def stop_reading(self, key: Key):
        self._redis.evalsha(self._stop_reading, 1, self._prefix + key)

    def get_size(self):
        return int(self._redis.get(self._volume_key) or 0)

    def set_size(self, size: int):
        self._redis.set(self._volume_key, size)

    def inc_size(self, size: int):
        self._redis.incrby(self._volume_key, size)

    def dec_size(self, size: int):
        self._redis.decrby(self._volume_key, size)

    @classmethod
    def from_url(cls, url: str, prefix: str, expire: int):
        return cls(Redis.from_url(url), prefix=prefix, expire=expire)


class PotentialDeadLock(RuntimeError):
    pass


def wait_for_true(func, key, sleep_time, max_iterations):
    i = 0
    while not func(key):
        if i >= max_iterations:
            logger.error('Potential deadlock detected for %s', key)
            raise PotentialDeadLock(f"It seems like you've hit a deadlock for key {key}.")

        time.sleep(sleep_time)
        i += 1

    logger.debug('Waited for %d iterations for %s', i, key)
