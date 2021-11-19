import os
import shutil
from enum import Enum
from pathlib import Path
from typing import Union


class Reason(Enum):
    WrongDigestSize, WrongHash, CorruptedHash, WrongFolderStructure, CorruptedData, Expired, Filtered = range(7)


def touch(path):
    os.utime(path)


def to_read_only(path: Path, permissions, group):
    os.chmod(path, 0o444 & permissions)
    shutil.chown(path, group=group)


def get_size(file: Path) -> int:
    return file.stat().st_size


def mkdir(path: Path, permissions: Union[int, None], group: Union[str, int, None],
          parents: bool = False, exist_ok: bool = False):
    path.mkdir(parents=parents, exist_ok=exist_ok)
    if permissions is not None:
        path.chmod(permissions)
    if group is not None:
        shutil.chown(path, group=group)


def create_folders(path: Path, permissions, group):
    if not path.exists():
        create_folders(path.parent, permissions, group)
        mkdir(path, permissions, group)
