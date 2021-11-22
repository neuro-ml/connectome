import os
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
    def factory(locker=None, group=None, names=('storage',)):
        with tempfile.TemporaryDirectory() as root:
            roots = []
            for name in names:
                root = Path(root) / name
                roots.append(root)
                init_storage(
                    root, group=group,
                    algorithm={'name': 'blake2b', 'digest_size': 64}, levels=[1, 31, 32], locker=locker
                )

            yield Storage(list(map(Disk, roots)))

    return factory


@pytest.fixture
def disk_cache_factory(storage_factory):
    def init(root, storage, serializer, names, locker, cls, **kwargs):
        init_storage(root, algorithm={'name': 'blake2b', 'digest_size': 64}, levels=[1, 31, 32], locker=locker)
        return cls(root, storage, serializer, names, **kwargs)

    @contextmanager
    def factory(names, serializer, locker=None, storage=None, cls=CacheToDisk, **kwargs):
        if storage is None:
            with tempfile.TemporaryDirectory() as root, storage_factory() as storage:
                yield init(Path(root) / 'cache', storage, serializer, names, locker, cls, **kwargs)

        else:
            with tempfile.TemporaryDirectory() as root:
                yield init(Path(root) / 'cache', storage, serializer, names, locker, cls, **kwargs)

    return factory


@pytest.fixture
def temp_dir(tmpdir):
    return Path(tmpdir)


@pytest.fixture
def chdir():
    @contextmanager
    def internal(folder):
        current = os.getcwd()
        try:
            os.chdir(folder)
            yield
        finally:
            os.chdir(current)

    return internal
