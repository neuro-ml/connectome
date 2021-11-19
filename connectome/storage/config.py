import hashlib
from functools import partial
from pathlib import Path
from typing import Union, Dict, Any, Tuple

import humanfriendly
from pydantic import BaseModel, Extra, validator
from yaml import safe_load, safe_dump

from .locker import Locker, DummyLocker
from .utils import mkdir
from ..utils import PathLike

STORAGE_CONFIG_NAME = 'config.yml'


class HashConfig(BaseModel):
    name: str
    kwargs: Dict[str, Any] = None

    @validator('kwargs', always=True)
    def normalize_kwargs(cls, v):
        if v is None:
            return {}
        return v

    def build(self):
        cls = getattr(hashlib, self.name)
        if self.kwargs:
            cls = partial(cls, **self.kwargs)
        return cls


class LockerConfig(BaseModel):
    name: str
    args: Tuple = ()
    kwargs: Dict[str, Any] = None

    @validator('name')
    def name_exists(cls, name):
        for kls in Locker.__subclasses__():
            if kls.__name__ == name:
                return name

        raise ValueError(f'Could not find a locker named {name}')

    @validator('kwargs', always=True)
    def normalize_kwargs(cls, v):
        if v is None:
            return {}
        return v

    class Config:
        extra = Extra.forbid


class DiskConfig(BaseModel):
    hash: Union[str, HashConfig]
    levels: Tuple[int, ...]
    locker: Union[str, LockerConfig] = None
    free_disk_size: Union[int, str] = 0
    max_size: Union[int, str] = None

    @validator('free_disk_size', 'max_size')
    def to_size(cls, v):
        return parse_size(v)

    @validator('hash', pre=True)
    def normalize_hash(cls, v):
        if isinstance(v, str):
            v = {'name': v}
        return v

    @validator('locker', pre=True)
    def normalize_locker(cls, v):
        if isinstance(v, str):
            v = {'name': v}
        return v

    class Config:
        extra = Extra.forbid


def root_params(root: Path):
    stat = root.stat()
    return stat.st_mode & 0o777, stat.st_gid


def load_config(root: PathLike) -> DiskConfig:
    with open(Path(root) / STORAGE_CONFIG_NAME) as file:
        # TODO: assert read-only
        return DiskConfig(**safe_load(file))


def init_storage(root: PathLike, *, permissions: Union[int, None] = None, group: Union[str, int, None] = None,
                 algorithm: dict, levels: list, locker=None, exist_ok: bool = False, **params):
    root = Path(root)
    mkdir(root, permissions, group, parents=True, exist_ok=exist_ok)
    config = {
        'hash': algorithm,
        'levels': levels,
        **params,
    }
    if locker is not None:
        config['locker'] = locker

    with open(root / STORAGE_CONFIG_NAME, 'w') as file:
        safe_dump(config, file)


def make_locker(config: LockerConfig) -> Locker:
    if config is None:
        return DummyLocker()

    name = config.name
    for cls in Locker.__subclasses__():
        if cls.__name__ == name:
            return cls(*config.args, **config.kwargs)

    raise ValueError(f'Could not find a locker named {name}')


def parse_size(x):
    if isinstance(x, int):
        return x
    if isinstance(x, str):
        return humanfriendly.parse_size(x)
    if x is not None:
        raise ValueError(f"Couldn't understand the size format: {x}")
