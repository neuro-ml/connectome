import os
import socket
import tempfile
from pathlib import Path
from typing import Union, Sequence, Callable, Any

import paramiko
from paramiko import SSHClient, AuthenticationException, SSHException
from paramiko.config import SSH_PORT, SSHConfig
from scp import SCPClient, SCPException

from .config import load_config, StorageDiskConfig
from .disk import digest_to_relative, FILENAME
from .interface import RemoteLocation
from ..utils import PathLike


class UnknownHostException(SSHException):
    pass


class SSHLocation(RemoteLocation):
    def __init__(self, hostname: str, root: PathLike, port: int = SSH_PORT, username: str = None, password: str = None,
                 key: Union[Path, Sequence[Path]] = ()):
        ssh = SSHClient()
        ssh.load_system_host_keys()
        # TODO: not safe
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
                key = host.get('identityfile', key)

        self.hostname, self.port, self.username, self.password, self.key = hostname, port, username, password, key
        self.root = Path(root)
        self.ssh = ssh
        self._levels = None

    def fetch(self, keys: Sequence[str], store: Callable[[str, Path], Any]) -> Sequence[str]:
        visited = set()
        with self:
            with SCPClient(self.ssh.get_transport()) as scp, tempfile.TemporaryDirectory() as temp_dir:
                temp = Path(temp_dir) / 'value'
                for key in keys:
                    try:
                        self._get_levels(scp)
                        scp.get(str(self.root / digest_to_relative(key, self._levels) / FILENAME), str(temp))
                        store(key, temp)
                        os.remove(temp)

                        visited.add(key)
                    except (SCPException, socket.timeout):
                        pass

        return list(visited)

    def download(self, key: str, file: Path):
        with SCPClient(self.ssh.get_transport()) as scp:
            try:
                self._get_levels(scp)
                scp.get(str(self.root / digest_to_relative(key, self._levels) / FILENAME), str(file))
                return True
            except (SCPException, socket.timeout):
                return False

    def _get_levels(self, scp):
        if self._levels is not None:
            return

        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir) / 'config.yml'
            scp.get(str(self.root / 'config.yml'), str(temp))
            self._levels = load_config(temp_dir, StorageDiskConfig).levels

    def __enter__(self):
        try:
            self.ssh.connect(
                self.hostname, self.port, self.username, self.password, key_filename=self.key,
                auth_timeout=10
            )
            return self
        except AuthenticationException:
            raise AuthenticationException(self.hostname) from None
        except socket.gaierror:
            raise UnknownHostException(self.hostname) from None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.ssh.close()
