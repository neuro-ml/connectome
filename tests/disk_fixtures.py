import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest
from tarn import HashKeyStorage
from tarn.config import StorageConfig, init_storage

from connectome import CacheToDisk


@pytest.fixture
def storage_factory():
    @contextmanager
    def factory(locker=None, group=None, names=('storage',)) -> Iterator[HashKeyStorage]:
        with tempfile.TemporaryDirectory() as root:
            roots = []
            for name in names:
                root = Path(root) / name
                roots.append(root)
                init_storage(
                    StorageConfig(hash='blake2b', levels=[1, 63], locker=locker),
                    root, group=group,
                )

            yield HashKeyStorage(roots)

    return factory


@pytest.fixture
def disk_cache_factory(storage_factory):
    def init(root, storage, serializer, names, locker, cls, **kwargs):
        init_storage(StorageConfig(hash='blake2b', levels=[1, 63], locker=locker), root)
        return cls(root, storage, serializer, names, **kwargs)

    @contextmanager
    def factory(names, serializer, locker=None, storage=None, root=None, cls=CacheToDisk, **kwargs):
        with tempfile.TemporaryDirectory() as _root, storage_factory(locker=locker) as _storage:
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
