import hashlib
from functools import partial
from pathlib import Path
from typing import Union

from yaml import safe_load, safe_dump

from .locker import Locker, DummyLocker
from .utils import mkdir
from ..utils import PathLike

FILENAME = 'config.yml'


def root_params(root: Path):
    stat = root.stat()
    return stat.st_mode & 0o777, stat.st_gid


def load_config(root: PathLike):
    with open(Path(root) / FILENAME) as file:
        # TODO: assert read-only
        # TODO: require algorithm, levels
        return safe_load(file)


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

    with open(root / FILENAME, 'w') as file:
        safe_dump(config, file)


def make_locker(config) -> Locker:
    config = config.get('locker')
    if config is None:
        return DummyLocker()

    assert set(config) <= {'name', 'args', 'kwargs'}
    name = config['name']

    for cls in Locker.__subclasses__():
        if cls.__name__ == name:
            args = config.get('args', ())
            if not isinstance(args, (list, tuple)):
                raise ValueError(f'"args" must be a list or tuple, got {type(args)}')

            return cls(*args, **config.get('kwargs', {}))

    raise ValueError(f'Could not find a locker named {name}')


def make_algorithm(config):
    algorithm = config['hash'].copy()
    hasher = getattr(hashlib, algorithm.pop('name'))
    if algorithm:
        hasher = partial(hasher, **algorithm)

    return hasher, config['levels']
