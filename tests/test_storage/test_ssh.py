import pytest

from connectome.storage import SSHLocation, ReadError


def load_text(path):
    with open(path, 'r') as file:
        return file.read()


@pytest.mark.ssh
def test_ssh(storage_factory):
    with storage_factory() as local, storage_factory() as remote:
        key = remote.store(__file__)
        with pytest.raises(ReadError):
            local.get_path(key)

        # add a remote
        local.remote = [SSHLocation('remote', remote.local[0].root)]
        assert local.load(load_text, key) == remote.load(load_text, key) == load_text(__file__)
