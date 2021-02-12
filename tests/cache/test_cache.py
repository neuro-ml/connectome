import time
from multiprocessing.context import Process
from tempfile import TemporaryDirectory
from threading import Thread

from connectome import CacheToRam, Apply, CacheToDisk
from connectome.serializers import JsonSerializer
from connectome.storage import DiskOptions


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


def test_disk_locking(block_maker):
    def visit():
        ds = block_maker.first_ds(first_constant=2, ids_arg=3)
        cached = ds >> Apply(image=sleeper(0.1)) >> CacheToDisk(
            root, DiskOptions(storage), names=['image'], serializer=JsonSerializer())

        for i in ds.ids:
            assert ds.image(i) == cached.image(i)

    with TemporaryDirectory() as root, TemporaryDirectory() as storage:
        th = Process(target=visit)
        th.start()
        visit()
        th.join()


def test_disk_idempotency(block_maker):
    ds = block_maker.first_ds(first_constant=2, ids_arg=3)

    with TemporaryDirectory() as root, TemporaryDirectory() as storage:
        storage = DiskOptions(storage)
        cache = CacheToDisk(root, storage, names=['image'], serializer=JsonSerializer())
        cached = ds >> cache >> cache

        for i in ds.ids:
            assert ds.image(i) == cached.image(i)
