import time
from tempfile import TemporaryDirectory
from threading import Thread

import pytest

from connectome import CacheToRam, Apply, CacheToDisk
from connectome.storage import DiskOptions


def sleeper(x):
    time.sleep(1)
    return x


def test_memory_locking(block_maker):
    def visit():
        for i in ds.ids:
            assert ds.image(i) == cached.image(i)

    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    cached = ds >> Apply(image=sleeper) >> CacheToRam()

    th = Thread(target=visit)
    th.start()
    visit()
    th.join()


@pytest.mark.skip
def test_disk_locking(block_maker):
    def visit():
        for i in ds.ids:
            assert ds.image(i) == cached.image(i)

    ds = block_maker.first_ds(first_constant=2, ids_arg=3)
    with TemporaryDirectory() as root, TemporaryDirectory() as storage:
        storage = DiskOptions(storage)
        # TODO: need a huge file that will take a while to write to disk
        cached = ds >> Apply(image=sleeper) >> CacheToDisk(root, storage, names=['image'])

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
