import os

import pytest
from tarn.cache.pickler import LATEST_VERSION

from connectome import CacheToDisk, Transform
from connectome.exceptions import StorageCorruption
from connectome.serializers import JsonSerializer


def setup_cache(temp_dir):
    cache = CacheToDisk.simple('x', root=temp_dir, serializer=JsonSerializer())
    ds = Transform(x=lambda x: 1) >> cache
    return ds, temp_dir / 'index'


def test_corrupted_cleanup(temp_dir):
    ds, index = setup_cache(temp_dir)
    default = set(index.glob('*/*'))
    # fill the cache
    assert ds.x(1) == 1
    index_path, = set(index.glob('*/*')) - default
    # make it corrupted
    os.chmod(index_path, 0o777)
    open(index_path, 'wb').close()
    assert index_path.read_text() == ''
    # now it should be restored
    assert ds.x(1) == 1
    assert index_path.exists() and index_path.read_text() != ''


# this test is allowed to fail, because we can't possibly cover all changes in all versions
@pytest.mark.skipif(LATEST_VERSION == 0, reason='Current pickler has a single version')
@pytest.mark.xfail
def test_versioning(monkeypatch, temp_dir):
    counter = A()
    counter.count = 0
    cache = CacheToDisk.simple('x', root=temp_dir, serializer=JsonSerializer())
    ds = Transform(x=counter.f) >> cache
    index = temp_dir / 'index'
    real = LATEST_VERSION
    monkeypatch.setattr('tarn.cache.pickler.LATEST_VERSION', 0)

    # fill the cache
    value = ds.x(1)
    assert counter.count == 1
    assert len(list(index.glob('*/*'))) == 1

    # restore the state - otherwise the cache is invalidated
    counter.count = 0
    assert ds.x(1) == value
    assert counter.count == 0
    assert len(list(index.glob('*/*'))) == 1

    monkeypatch.setattr('tarn.cache.pickler.LATEST_VERSION', real)
    assert ds.x(1) == value
    assert counter.count == 0
    assert len(list(index.glob('*/*'))) == 2


class A:
    def f(self, x):
        self.count += 1
        return 1

    @classmethod
    def __getversion__(cls):
        return 1
