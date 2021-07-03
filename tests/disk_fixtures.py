import tempfile
from contextlib import contextmanager
from pathlib import Path

import pytest

from connectome import CacheToDisk
from connectome.storage import Storage, Disk
from connectome.storage.config import init_storage


@pytest.fixture
def storage_factory():
    @contextmanager
    def factory(locker=None, group=None):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root) / 'storage'
            init_storage(
                root, group=group,
                algorithm={'name': 'blake2b', 'digest_size': 64}, levels=[1, 31, 32], locker=locker
            )
            yield Storage([Disk(root)])

    return factory


@pytest.fixture
def disk_cache_factory(storage_factory):
    def init(root, storage, serializer, names, locker):
        init_storage(root, algorithm={'name': 'blake2b', 'digest_size': 64}, levels=[1, 31, 32], locker=locker)
        return CacheToDisk(root, storage, serializer, names)

    @contextmanager
    def factory(names, serializer, locker=None, storage=None):
        if storage is None:
            with tempfile.TemporaryDirectory() as root, storage_factory() as storage:
                yield init(Path(root) / 'cache', storage, serializer, names, locker)

        else:
            with tempfile.TemporaryDirectory() as root:
                yield init(Path(root) / 'cache', storage, serializer, names, locker)

    return factory


@pytest.fixture
def temp_dir(tmpdir):
    return Path(tmpdir)
