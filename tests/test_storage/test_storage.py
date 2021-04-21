import time
from pathlib import Path
from threading import Thread

from connectome.storage import Storage, Disk
from connectome.storage.disk import match_files
from connectome.storage.locker import ThreadLocker, RedisLocker, SqliteLocker


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


def test_parallel_read(tmpdir):
    def job():
        storage.load(lambda x: time.sleep(1), key)

    tmpdir = Path(tmpdir)
    storage = Storage([Disk(tmpdir)])
    key = storage.store(__file__)
    lockers = [
        ThreadLocker(),
        SqliteLocker(tmpdir / 'db.sqlite3'),
        # RedisLocker.from_url('redis://localhost:6379/0', 'connectome.tests'),
    ]

    for locker in lockers:
        storage = Storage([Disk(tmpdir, locker=locker)])

        start = time.time()
        th = Thread(target=job)
        th.start()
        job()
        th.join()
        stop = time.time()

        assert stop - start < 1.5
