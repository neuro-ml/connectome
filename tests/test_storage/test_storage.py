import filecmp
import time
from multiprocessing.pool import ThreadPool
from pathlib import Path
from threading import Thread
from multiprocessing.context import Process

import pytest

from connectome.storage import Storage, Disk
from connectome.storage.locker import ThreadLocker, RedisLocker


def test_single_local(tmpdir):
    locker = ThreadLocker()
    disk = Disk(tmpdir, locker=locker)
    storage = Storage([disk])

    # just store this file, because why not
    file = Path(__file__)
    permissions = file.stat().st_mode & 0o777
    key = storage.store(file)
    stored = storage.get_path(key)

    assert filecmp.cmp(file, stored, shallow=False)
    assert file.stat().st_mode & 0o777 == permissions
    assert stored.stat().st_mode & 0o777 == disk.permissions & 0o444
    assert stored.group() == disk.group


@pytest.mark.redis
def test_parallel_read_threads(tmpdir, subtests, redis_hostname):
    def job():
        storage.load(lambda x: time.sleep(sleep_time), key)

    sleep_time = 1
    tmpdir = Path(tmpdir)
    storage = Storage([Disk(tmpdir)])
    key = storage.store(__file__)
    lockers = [
        ThreadLocker(),
        RedisLocker.from_url(f'redis://{redis_hostname}:6379/0', 'connectome.tests', 10),
        # SqliteLocker(tmpdir / 'db.sqlite3'),
    ]

    for locker in lockers:
        with subtests.test(str(type(locker).__name__)):
            storage = Storage([Disk(tmpdir, locker=locker)])

            # single thread
            start = time.time()
            th = Thread(target=job)
            th.start()
            job()
            th.join()
            stop = time.time()

            assert stop - start < sleep_time * 1.1

            # thread pool
            pool = ThreadPool(10, job)
            start = time.time()
            pool.close()
            pool.join()
            stop = time.time()

            assert stop - start < sleep_time * 1.1


@pytest.mark.redis
def test_parallel_read_processes(tmpdir, redis_hostname):
    def job():
        storage.load(lambda x: time.sleep(1), key)

    locker = RedisLocker.from_url(f'redis://{redis_hostname}:6379/0', 'connectome.tests', 10)
    storage = Storage([Disk(tmpdir, locker=locker)])
    key = storage.store(__file__)

    start = time.time()
    th = Process(target=job)
    th.start()
    job()
    th.join()
    stop = time.time()

    assert stop - start < 1.5
