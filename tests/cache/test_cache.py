import time
from multiprocessing.context import Process
from tempfile import TemporaryDirectory
from threading import Thread

import pytest
from connectome import CacheToRam, Apply, CacheToDisk
from connectome.cache import MemoryCache
from connectome.cache.transactions import ThreadedTransaction
from connectome.engine.edges import CacheEdge
from connectome.serializers import JsonSerializer
from connectome.storage import Storage, Disk


def sleeper(s):
    def f(x):
        time.sleep(s)
        return x

    return f


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


def test_memory_errors_handling(block_maker):
    class LocalException(Exception):
        pass

    def throw(x):
        raise LocalException

    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    cached = ds >> Apply(image=throw) >> CacheToRam()
    i = ds.ids[0]
    with pytest.raises(LocalException):
        assert ds.image(i) == cached.image(i)

    for edge in cached._layer.edges:
        edge = edge.edge
        if isinstance(edge, CacheEdge):
            assert isinstance(edge.cache, MemoryCache)
            assert isinstance(edge.cache._transactions, ThreadedTransaction)
            # the state must be cleaned up
            assert not edge.cache._transactions._transactions


def test_memory_locking_errors(block_maker):
    class LocalException(Exception):
        pass

    def throw(x):
        raise LocalException

    def visit():
        try:
            assert cached.image(i)
        except LocalException:
            pass

    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    cached = ds >> Apply(image=sleeper(0.1)) >> Apply(image=throw) >> CacheToRam()
    i = ds.ids[0]

    th = Thread(target=visit)
    th.start()
    with pytest.raises(LocalException):
        cached.image(i)
    th.join()


def test_disk_locking_processes(block_maker):
    def visit():
        ds = block_maker.first_ds(first_constant=2, ids_arg=3)
        cached = ds >> Apply(image=sleeper(0.1)) >> CacheToDisk(
            root, Storage([Disk(storage)]), names=['image'], serializer=JsonSerializer())

        for i in ds.ids:
            assert ds.image(i) == cached.image(i)

    for _ in range(5):
        with TemporaryDirectory() as root, TemporaryDirectory() as storage:
            th = Process(target=visit)
            th.start()
            visit()
            th.join()


def test_disk_locking_threads(block_maker):
    def visit():
        for i in ds.ids:
            assert ds.image(i) == cached.image(i)

    for _ in range(5):
        with TemporaryDirectory() as root, TemporaryDirectory() as storage:
            ds = block_maker.first_ds(first_constant=2, ids_arg=3)
            cached = ds >> Apply(image=sleeper(0.1)) >> CacheToDisk(
                root, Storage([Disk(storage)]), names=['image'], serializer=JsonSerializer())

            th = Thread(target=visit)
            th.start()
            visit()
            th.join()


def test_disk_idempotency(block_maker):
    ds = block_maker.first_ds(first_constant=2, ids_arg=3)

    with TemporaryDirectory() as root, TemporaryDirectory() as storage:
        storage = Storage([Disk(storage)])
        cache = CacheToDisk(root, storage, names=['image'], serializer=JsonSerializer())
        cached = ds >> cache >> cache

        for i in ds.ids:
            assert ds.image(i) == cached.image(i)
