import inspect

import pytest

from connectome import Transform
from connectome.serializers import JsonSerializer
from connectome.storage import SSHLocation, ReadError


def load_text(path):
    with open(path, 'r') as file:
        return file.read()


class Counter:
    def __init__(self, func=lambda x: x):
        self.func = func
        self.__signature__ = inspect.signature(func)
        self.n = 0

    def __call__(self, *args, **kwargs):
        self.n += 1
        return self.func(*args, **kwargs)

    def __getstate__(self):
        return self.func


def get_ssh_location(root):
    return SSHLocation('remote', root, password='password')


@pytest.mark.ssh
def test_storage_ssh(storage_factory):
    with storage_factory() as local, storage_factory() as remote:
        key = remote.write(__file__)
        with pytest.raises(ReadError):
            local.resolve(key)

        # add a remote
        local.storage.remote = [get_ssh_location(remote.local[0].root)]
        with pytest.raises(ReadError, match=r'^Key \w+ is not present locally$'):
            local.read(load_text, key, fetch=False)

        assert local.read(load_text, key) == remote.read(load_text, key) == load_text(__file__)


@pytest.mark.ssh
def test_index_ssh(temp_dir, storage_factory, disk_cache_factory):
    # we use this counter to make sure the func wasn't called too many times
    counter = Counter()

    ds = Transform(x=counter)
    serializer = JsonSerializer()
    with storage_factory() as storage:
        with disk_cache_factory('x', serializer, storage=storage, root=temp_dir) as remote_cache:
            remote_root = temp_dir / 'cache'
            # write to remote first
            cached = ds >> remote_cache
            cached.x(1)
            assert counter.n == 1
            cached.x(1)
            assert counter.n == 1

            with disk_cache_factory(
                    'x', serializer, storage=storage, remote=get_ssh_location(remote_root)
            ) as local_cache:
                cached = ds >> local_cache
                cached.x(1)
                assert counter.n == 1
                cached.x(1)
                assert counter.n == 1

            # same thing but without fetch
            with disk_cache_factory('x', serializer, storage=storage) as local_cache:
                cached = ds >> local_cache
                cached.x(1)
                assert counter.n == 2
                cached.x(1)
                assert counter.n == 2
