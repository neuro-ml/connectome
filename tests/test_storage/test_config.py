import pytest
from pydantic import ValidationError

from connectome.storage.config import DiskConfig, HashConfig


def test_config():
    a = DiskConfig(hash='sha256', levels=[1, 31])
    b = DiskConfig(hash=HashConfig(name='sha256'), levels=[1, 31])

    assert isinstance(a.hash, HashConfig)
    assert isinstance(b.hash, HashConfig)
    assert a.hash == b.hash
    assert a == b

    with pytest.raises(ValidationError, match='Could not find a locker named 1'):
        DiskConfig(hash='sha256', levels=[1, 31], locker='1')

    DiskConfig(hash='sha256', levels=[1, 31], locker='RedisLocker')
