import tempfile
import time
from multiprocessing.context import Process
from pathlib import Path
from threading import Thread

import pytest

from connectome import CacheToRam, Apply, CacheToDisk
from connectome.cache import MemoryCache, DiskCache
from connectome.engine.edges import CacheEdge
from connectome.serializers import JsonSerializer
from connectome.storage.locker import ThreadLocker, RedisLocker


@pytest.fixture
def temp_disk_cache(tmpdir, tmpdir_factory, temp_storage):
    def maker(serializer, locker, names):
        return CacheToDisk(tmpdir_factory.mktemp(tmpdir) / 'cache', temp_storage, serializer, names, locker=locker)

    return maker


def sleeper(s):
    def f(x):
        time.sleep(s)
        return x

    return f


def assert_empty_state(block):
    def find_cache():
        for edge in block._container.edges:
            edge = edge.edge
            if isinstance(edge, CacheEdge):
                return edge.cache

        assert False

    cache = find_cache()
    assert isinstance(cache, (MemoryCache, DiskCache))
    assert isinstance(cache._locker, ThreadLocker)
    # the state must be cleaned up
    assert not cache._locker._reading
    assert not cache._locker._writing


def test_memory_locking(block_maker):
    def visit():
        for i in ds.ids:
            assert ds.image(i) == cached.image(i)

    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    cached = ds >> Apply(image=sleeper(0.1)) >> CacheToRam()

    th = Thread(target=visit)
    th.start()
    visit()
    th.join()


def test_errors_handling(block_maker, temp_disk_cache):
    class LocalException(Exception):
        pass

    def throw(x):
        raise LocalException

    def visit(block):
        try:
            assert block.image(i)
        except LocalException:
            pass

    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    i = ds.ids[0]

    for layer in [CacheToRam(), temp_disk_cache(JsonSerializer(), ThreadLocker(), 'image')]:
        # one thread
        cached = ds >> Apply(image=throw) >> layer
        with pytest.raises(LocalException):
            cached.image(i)

        assert_empty_state(cached)

        # many threads
        cached = ds >> Apply(image=sleeper(0.1)) >> Apply(image=throw) >> layer
        th = Thread(target=visit)
        th.start()
        with pytest.raises(LocalException):
            cached.image(i)
        th.join()

        assert_empty_state(cached)


@pytest.mark.redis
def test_disk_locking_processes(block_maker, temp_storage, redis_hostname):
    def visit():
        ds = block_maker.first_ds(first_constant=2, ids_arg=3)
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            cached = ds >> Apply(image=sleeper(0.1)) >> CacheToDisk(
                folder / 'cache', temp_storage, JsonSerializer(), 'image', locker=RedisLocker.from_url(
                    f'redis://{redis_hostname}:6379/0', 'connectome.tests.locking.disk', 10))

            for i in ds.ids:
                assert ds.image(i) == cached.image(i)

    for _ in range(5):
        th = Process(target=visit)
        th.start()
        visit()
        th.join()


def test_disk_locking_threads(block_maker, temp_disk_cache):
    def visit():
        for i in ds.ids:
            assert ds.image(i) == cached.image(i)

    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    cached = ds >> Apply(image=sleeper(0.1)) >> temp_disk_cache(JsonSerializer(), ThreadLocker(), 'image')
    for _ in range(5):
        th = Thread(target=visit)
        th.start()
        visit()
        th.join()


def test_disk_idempotency(block_maker, temp_disk_cache):
    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    cache = temp_disk_cache(JsonSerializer(), None, 'image')
    cached = ds >> cache >> cache

    for i in ds.ids:
        assert ds.image(i) == cached.image(i)
