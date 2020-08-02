from pathlib import Path
from typing import NamedTuple

import paramiko
from paramiko import SSHClient
from paramiko.config import SSH_PORT, SSHConfig
from scp import SCPClient, SCPException

from ..utils import PathLike


class RemoteOptions(NamedTuple):
    hostname: str
    root: PathLike
    port: int = SSH_PORT
    username: str = None
    password: str = None


class RelativeRemote:
    def __init__(self, hostname: str, root: Path, port: int, username: str, password: str):
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

        self.hostname, self.port, self.username, self.password = hostname, port, username, password
        self.root = root
        self.ssh = ssh

    def get(self, remote, local):
        with SCPClient(self.ssh.get_transport()) as scp:
            try:
                return scp.get(str(self.root / remote), str(local), recursive=True)
            except SCPException:
                raise FileNotFoundError from None

    def __enter__(self):
        self.ssh.connect(self.hostname, self.port, self.username, self.password, auth_timeout=10)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.ssh.__exit__(exc_type, exc_val, exc_tb)
