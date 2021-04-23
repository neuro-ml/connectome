import time
from pathlib import Path
from threading import Thread

from connectome.storage import Storage, Disk
from connectome.storage.disk import match_files
from connectome.storage.locker import ThreadLocker, RedisLocker, SqliteLocker
from multiprocessing.context import Process


def test_single_local(tmpdir):
    locker = ThreadLocker()
    storage = Storage([Disk(tmpdir, locker=locker)])

    # just store this file, because why not
    file = Path(__file__)
    permissions = file.stat().st_mode & 0o777
    key = storage.store(file)
    stored = storage.get_path(key)

    assert match_files(stored, file)
    assert file.stat().st_mode & 0o777 == permissions
    assert stored.stat().st_mode & 0o777 == permissions & 0o444


def test_parallel_read_threads(tmpdir, subtests):
    def job():
        storage.load(lambda x: time.sleep(1), key)

    tmpdir = Path(tmpdir)
    storage = Storage([Disk(tmpdir)])
    key = storage.store(__file__)
    lockers = [
        ThreadLocker(),
        RedisLocker.from_url('redis://localhost:6379/0', 'connectome.tests'),
        # SqliteLocker(tmpdir / 'db.sqlite3'),
    ]

    # threads
    for locker in lockers:
        with subtests.test(str(type(locker).__name__)):
            storage = Storage([Disk(tmpdir, locker=locker)])

            start = time.time()
            th = Thread(target=job)
            th.start()
            job()
            th.join()
            stop = time.time()

            assert stop - start < 1.5


def test_parallel_read_processes(tmpdir):
    def job():
        storage.load(lambda x: time.sleep(1), key)

    locker = RedisLocker.from_url('redis://localhost:6379/0', 'connectome.tests')
    storage = Storage([Disk(tmpdir, locker=locker)])
    key = storage.store(__file__)

    start = time.time()
    th = Process(target=job)
    th.start()
    job()
    th.join()
    stop = time.time()

    assert stop - start < 1.5
