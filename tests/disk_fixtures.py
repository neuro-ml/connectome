import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest
from tarn.config import init_storage, StorageConfig

from connectome import CacheToDisk
from connectome.storage import Storage, Disk


@pytest.fixture
def storage_factory():
    @contextmanager
    def factory(locker=None, group=None, names=('storage',)) -> Iterator[Storage]:
        with tempfile.TemporaryDirectory() as root:
            roots = []
            for name in names:
                root = Path(root) / name
                roots.append(root)
                init_storage(
                    StorageConfig(hash='blake2b', levels=[1, 63], locker=locker),
                    root, group=group,
                )

            yield Storage(list(map(Disk, roots)))

    return factory


@pytest.fixture
def disk_cache_factory(storage_factory):
    def init(root, storage, serializer, names, locker, cls, **kwargs):
        init_storage(StorageConfig(hash='blake2b', levels=[1, 63], locker=locker), root)
        return cls(root, storage, serializer, names, **kwargs)

    @contextmanager
    def factory(names, serializer, locker=None, storage=None, root=None, cls=CacheToDisk, **kwargs):
        with tempfile.TemporaryDirectory() as _root, storage_factory() as _storage:
            if root is None:
                root = _root
            if storage is None:
                storage = _storage
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
