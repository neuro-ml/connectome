import shutil
import socket
import tempfile
from pathlib import Path
from typing import Union, Sequence, Callable, Any, Tuple

import paramiko
from paramiko import SSHClient, AuthenticationException, SSHException
from paramiko.config import SSH_PORT, SSHConfig
from scp import SCPClient, SCPException

from .config import load_config, HashConfig
from .digest import key_to_relative
from .interface import RemoteStorage, Key
from ..utils import PathLike


class UnknownHostException(SSHException):
    pass


class SSHLocation(RemoteStorage):
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
        self.levels = self.hash = None

    def fetch(self, keys: Sequence[Key], store: Callable[[str, Path], Any],
              config: HashConfig) -> Sequence[Tuple[Any, bool]]:

        results = []

        try:
            with self.ssh.connect(self.hostname, self.port, self.username, self.password, key_filename=self.key,
                                  auth_timeout=10):

                with SCPClient(self.ssh.get_transport()) as scp, tempfile.TemporaryDirectory() as temp_dir:
                    source = Path(temp_dir) / 'source'

                    for key in keys:
                        try:
                            self._get_config(scp)
                            scp.get(str(self.root / key_to_relative(key, self.levels)), str(source), recursive=True)
                            value = store(key, source)
                            shutil.rmtree(source)

                            results.append((value, True))
                        except (SCPException, socket.timeout):
                            results.append((None, False))

        except AuthenticationException:
            raise AuthenticationException(self.hostname) from None
        except socket.gaierror:
            raise UnknownHostException(self.hostname) from None

        return results

    def _get_config(self, scp):
        if self.levels is not None:
            return

        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir) / 'config.yml'
            scp.get(str(self.root / 'config.yml'), str(temp))
            config = load_config(temp_dir)
            self.hash, self.levels = config.hash, config.levels
