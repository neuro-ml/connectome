import tempfile
from pathlib import Path
from threading import RLock
from typing import NamedTuple, Sequence

import paramiko
from paramiko import SSHClient
from paramiko.config import SSH_PORT, SSHConfig
from scp import SCPClient, SCPException

from ..engine.base import NodeHash
from ..serializers import Serializer
from ..storage.base import CacheStorage
from ..utils import atomize, PathLike
from .disk import key_to_relative, check_consistency, DATA_FOLDER, HASH_FILENAME
from .utils import ChainDict


class RemoteOptions(NamedTuple):
    hostname: str
    storage: PathLike
    port: int = SSH_PORT
    username: str = None
    password: str = None


class RemoteStorage(CacheStorage):
    def __init__(self, options: Sequence[RemoteOptions], serializer: Serializer):
        super().__init__()
        self._lock = RLock()
        self.storage = ChainDict([
            RemoteDict(serializer, **entry._asdict()) for entry in options
        ], lambda x: True)

    @atomize()
    def contains(self, param: NodeHash) -> bool:
        return param.value in self.storage

    @atomize()
    def set(self, param: NodeHash, value):
        self.storage[param.value] = value

    @atomize()
    def get(self, param: NodeHash):
        return self.storage[param.value]


class RemoteDict:
    def __init__(self, serializer: Serializer, hostname: str, storage: Path, port: int, username: str, password: str):
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        config_path = Path('~/.ssh/config').expanduser()
        if config_path.exists():
            with open(config_path) as f:
                config = SSHConfig()
                config.parse(f)
                host = config.lookup(hostname)

                hostname = host.get('hostname', hostname)
                port = host.get('port', port)
                username = host.get('user', username)

        # TODO: context manager
        ssh.connect(hostname, port, username, password, auth_timeout=10)
        self.ssh = ssh
        self.storage = Path(storage)
        self.serializer = serializer

    def _load(self, func, relative: Path, *args, **kwargs):
        remote = self.storage / relative

        with SCPClient(self.ssh.get_transport()) as scp, tempfile.TemporaryDirectory() as temp_dir:
            temp_file = Path(temp_dir) / remote.name
            scp.get(str(remote), str(temp_file), recursive=True)
            return func(temp_file, *args, **kwargs)

    def __contains__(self, key):
        pickled, _, relative = key_to_relative(key)
        try:
            self._load(check_consistency, relative / HASH_FILENAME, pickled)
        except SCPException:
            return False
        return True

    def __getitem__(self, key):
        _, _, relative = key_to_relative(key)
        try:
            return self._load(self.serializer.load, relative / DATA_FOLDER)
        except SCPException:
            raise KeyError from None

    # TODO: could this be useful?
    def __setitem__(self, key, value):
        pass
