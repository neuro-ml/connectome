import time
from tempfile import TemporaryDirectory
from threading import Thread

from connectome import CacheToRam, Apply, CacheToDisk
from connectome.storage import DiskOptions


def test_memory_locking(block_maker):
    def visit():
        for i in ds.ids:
            assert ds.image(i) == cached.image(i)

    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    cached = ds >> Apply(image=lambda x: [x, time.sleep(1)]) >> CacheToRam()

    th = Thread(target=visit)
    th.start()
    visit()
    th.join()


def test_disk_locking(block_maker):
    def visit():
        for i in ds.ids:
            assert ds.image(i) == cached.image(i)

    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    with TemporaryDirectory() as root, TemporaryDirectory() as storage:
        storage = DiskOptions(storage)
        cached = ds >> Apply(image=lambda x: [x, time.sleep(1)]) >> CacheToDisk(root, storage, names=['image'])

        th = Thread(target=visit)
        th.start()
        visit()
        th.join()


def test_disk_idempotency(block_maker):
    ds = block_maker.first_ds(first_constant=2, ids_arg=3)

    with TemporaryDirectory() as root, TemporaryDirectory() as storage:
        storage = DiskOptions(storage)
        cache = CacheToDisk(root, storage, names=['image'])
        cached = ds >> cache >> cache

        for i in ds.ids:
            assert ds.image(i) == cached.image(i)
