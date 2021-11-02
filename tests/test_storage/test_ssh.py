import pytest

from connectome.storage import SSHLocation
from connectome.storage.storage import QueryError


def load_text(path):
    with open(path, 'r') as file:
        return file.read()


@pytest.mark.ssh
def test_ssh(storage_factory):
    with storage_factory() as local, storage_factory() as remote:
        key = remote.store(__file__)
        with pytest.raises(QueryError):
            local.get_path(key)

        # add a remote
        local.remote = [SSHLocation('localhost', remote.local[0].root)]
        assert local.load(load_text, key) == remote.load(load_text, key) == load_text(__file__)
