import pytest

from connectome.storage import SSHLocation, ReadError


def load_text(path):
    with open(path, 'r') as file:
        return file.read()


@pytest.mark.ssh
def test_ssh(storage_factory):
    with storage_factory() as local, storage_factory() as remote:
        key = remote.write(__file__)
        with pytest.raises(ReadError):
            local.resolve(key)

        # add a remote
        local.remote = [SSHLocation('remote', remote.local[0].root)]
        assert local.read(load_text, key) == remote.read(load_text, key) == load_text(__file__)
