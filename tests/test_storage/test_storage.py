from connectome.storage import Storage, Disk
from connectome.storage.disk import match_files
from connectome.storage.locker import ThreadLocker


def test_single_local(tmpdir):
    locker = ThreadLocker()
    storage = Storage([Disk(tmpdir, locker=locker)])

    # just store this file, because why not
    key = storage.store(__file__)
    assert match_files(storage.get_path(key), __file__)
