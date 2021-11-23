import tempfile
import time
from math import ceil
from multiprocessing.context import Process
from pathlib import Path
from threading import Thread

import pytest

from connectome import CacheToRam, Apply, CacheToDisk, CacheColumns
from connectome.cache import MemoryCache, DiskCache
from connectome.containers.cache import CachedColumn
from connectome.engine.edges import CacheEdge
from connectome.serializers import JsonSerializer
from connectome.storage.config import init_storage
from connectome.storage.locker import ThreadLocker


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
                yield edge.cache
            if isinstance(edge, CachedColumn):
                yield edge.disk
                yield edge.ram

    caches = list(find_cache())
    assert caches
    for cache in caches:
        assert isinstance(cache, (MemoryCache, DiskCache))
        locker = cache.locker if isinstance(cache, MemoryCache) else cache.cache.local[0].locker

        assert isinstance(locker, ThreadLocker)
        # the state must be cleaned up
        assert not locker._reading
        assert not locker._writing


def test_memory_locking(block_maker):
    def visit():
        for i in ds.ids:
            assert ds.image(i) == cached.image(i)
            assert ds.image(i) == cached.image(i)

    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    cached = ds >> Apply(image=sleeper(0.1)) >> CacheToRam()

    th = Thread(target=visit)
    th.start()
    visit()
    th.join()


def test_errors_handling(block_maker, disk_cache_factory):
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

    with disk_cache_factory('image', JsonSerializer(), {'name': 'ThreadLocker'}) as disk_cache:
        with disk_cache_factory('image', JsonSerializer(), {'name': 'ThreadLocker'}, cls=CacheColumns) as cols_cache:
            for layer in [CacheToRam(), disk_cache, cols_cache]:
                # one thread
                cached = ds >> Apply(image=throw) >> layer
                with pytest.raises(LocalException):
                    cached.image(i)

                assert_empty_state(cached)

                # many threads
                cached = ds >> Apply(image=sleeper(0.1)) >> Apply(image=throw) >> layer
                th = Thread(target=visit, args=(cached,))
                th.start()
                with pytest.raises(LocalException):
                    cached.image(i)
                th.join()

                assert_empty_state(cached)


def test_columns_cache_sharding(block_maker, disk_cache_factory):
    total = 500
    ds = block_maker.first_ds(first_constant=2, ids_arg=total)
    i = sorted(ds.ids)[0]

    for size in [100, 200, 10, 0.5, 0.1, 0.99, None]:
        with disk_cache_factory(
                'image', JsonSerializer(), {'name': 'ThreadLocker'}, shard_size=size, cls=CacheColumns
        ) as cache:
            cached = ds >> cache
            cached.image(i)

            if size is None:
                size = total
            if isinstance(size, float):
                size = ceil(size * total)

            c = cache._container.ram
            # just one shard must be populated
            assert len(c._cache) == size

            for key in ds.ids:
                cached.image(key)

            # the shards must cover all keys
            assert len(c._cache) == total


@pytest.mark.redis
def test_disk_locking_processes(block_maker, storage_factory, redis_hostname):
    def visit(storage, root):
        ds = block_maker.first_ds(first_constant=2, ids_arg=3)
        cached = ds >> Apply(image=sleeper(0.1)) >> CacheToDisk(root, storage, JsonSerializer(), 'image')

        for i in ds.ids:
            assert ds.image(i) == cached.image(i)
            assert ds.image(i) == cached.image(i)

    for _ in range(5):
        with tempfile.TemporaryDirectory() as temp, storage_factory() as temp_storage:
            temp = Path(temp) / 'cache'
            init_storage(
                temp, algorithm={'name': 'blake2b', 'digest_size': 64}, levels=[1, 63], locker={
                    'name': 'RedisLocker', 'args': [redis_hostname],
                    'kwargs': {'prefix': 'connectome.tests', 'expire': 10}
                }
            )

            th = Process(target=visit, args=(temp_storage, temp))
            th.start()
            visit(temp_storage, temp)
            th.join()


def test_disk_locking_threads(block_maker, disk_cache_factory):
    def visit():
        for i in ds.ids:
            assert ds.image(i) == cached.image(i)
            assert ds.image(i) == cached.image(i)

    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    for _ in range(5):
        with disk_cache_factory('image', JsonSerializer(), {'name': 'ThreadLocker'}) as disk_cache:
            cached = ds >> Apply(image=sleeper(0.1)) >> disk_cache
            th = Thread(target=visit)
            th.start()
            visit()
            th.join()


def test_disk_idempotency(block_maker, disk_cache_factory):
    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    with disk_cache_factory('image', JsonSerializer()) as cache:
        cached = ds >> cache >> cache

        for i in ds.ids:
            # first call is from mem, second - from disk
            assert ds.image(i) == cached.image(i)
            assert ds.image(i) == cached.image(i)


def test_simple_classmethod(block_maker, temp_dir):
    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    cached = ds >> CacheToDisk.simple('image', root=temp_dir)

    for i in ds.ids:
        assert ds.image(i) == cached.image(i)
        assert ds.image(i) == cached.image(i)


def test_relative_root(block_maker, temp_dir, chdir):
    with chdir(temp_dir):
        ds = block_maker.first_ds(first_constant=2, ids_arg=3)
        cached = ds >> CacheToDisk.simple('image', root='cache')

        for i in ds.ids:
            assert ds.image(i) == cached.image(i)
            assert ds.image(i) == cached.image(i)
