import pytest

from connectome import Transform
from connectome.serializers import JsonSerializer
from tarn import SSHLocation
from utils import Counter


def load_text(path):
    with open(path, 'r') as file:
        return file.read()


def get_ssh_location(root):
    return SSHLocation('remote', root, password='password')


@pytest.mark.skip
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
