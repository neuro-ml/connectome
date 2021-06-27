import filecmp
import time
from multiprocessing.pool import ThreadPool
from pathlib import Path
from threading import Thread
from multiprocessing.context import Process

import pytest


def test_storage_fixture(storage_factory):
    for _ in range(3):
        with storage_factory() as storage:
            pass


def test_single_local(storage_factory):
    with storage_factory({'name': 'ThreadLocker'}) as storage:
        disk = storage.local[0]

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
def test_parallel_read_threads(storage_factory, subtests, redis_hostname):
    def job():
        storage.load(lambda x: time.sleep(sleep_time), key)

    sleep_time = 1
    lockers = [
        {'name': 'ThreadLocker'},
        {'name': 'RedisLocker', 'args': [redis_hostname], 'kwargs': {'prefix': 'connectome.tests', 'expire': 10}},
    ]

    for locker in lockers:
        with subtests.test(locker['name']), storage_factory(locker) as storage:
            key = storage.store(__file__)
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
def test_parallel_read_processes(storage_factory, redis_hostname):
    def job():
        storage.load(lambda x: time.sleep(1), key)

    with storage_factory({'name': 'RedisLocker', 'args': [redis_hostname],
                          'kwargs': {'prefix': 'connectome.tests', 'expire': 10}}) as storage:
        key = storage.store(__file__)

        start = time.time()
        th = Process(target=job)
        th.start()
        job()
        th.join()
        stop = time.time()

        assert stop - start < 1.5
