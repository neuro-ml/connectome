import pytest

from connectome.storage import Storage, Disk


@pytest.fixture
def temp_storage(tmpdir):
    tmpdir = tmpdir / 'storage'
    tmpdir.mkdir()
    return Storage([Disk(tmpdir)])
