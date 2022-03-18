import os

import pytest

from connectome import Transform, CacheToDisk
from connectome.exceptions import StorageCorruption
from connectome.serializers import JsonSerializer
from tarn.cache.pickler import LATEST_VERSION


def setup_cache(temp_dir):
    cache = CacheToDisk.simple('x', root=temp_dir, serializer=JsonSerializer())
    ds = Transform(x=lambda x: 1) >> cache
    return ds, temp_dir / 'index'


def test_corrupted_cleanup(temp_dir):
    ds, index = setup_cache(temp_dir)
    # fill the cache
    ds.x(1)
    path, = index.glob('*/*')
    # make it corrupted
    hash_path = path / 'hash.bin'
    os.remove(hash_path)
    assert not hash_path.exists()
    # now it should be restored
    ds.x(1)
    assert hash_path.exists()


def test_corrupted_error(temp_dir):
    ds, index = setup_cache(temp_dir)
    ds.x(1)
    path, = index.glob('*/*')
    hash_path = path / 'hash.bin'
    os.chmod(hash_path, 0o777)
    with open(hash_path, 'wb') as file:
        file.write(b'\x00')

    with pytest.raises(StorageCorruption, match='You may want to delete'):
        ds.x(1)


@pytest.mark.skipif(LATEST_VERSION == 0, reason='Current pickler has a single version')
def test_versioning(monkeypatch, temp_dir):
    def f(x):
        nonlocal count
        count += 1
        return 1

    count = 0
    cache = CacheToDisk.simple('x', root=temp_dir, serializer=JsonSerializer())
    ds = Transform(x=f) >> cache
    index = temp_dir / 'index'
    real = LATEST_VERSION
    monkeypatch.setattr('connectome.cache.pickler.LATEST_VERSION', 0)

    value = ds.x(1)
    assert count == 1
    assert len(list(index.glob('*/*'))) == 1
    assert ds.x(1) == value
    assert count == 1
    assert len(list(index.glob('*/*'))) == 1

    monkeypatch.setattr('connectome.cache.pickler.LATEST_VERSION', real)
    assert ds.x(1) == value
    assert count == 1
    assert len(list(index.glob('*/*'))) == 2
