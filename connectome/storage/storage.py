import logging
import warnings
from pathlib import Path
from typing import Sequence, Iterable, Callable, Any

from .digest import digest_file
from .disk import Disk
from .interface import HashStorage, Key, RemoteStorage, ReadError, WriteError
from ..utils import PathLike

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self, local: Sequence[Disk], remote: Sequence[RemoteStorage] = ()):
        self.storage = HashStorage(local, remote)
        self.algorithm = self.storage.hash.build()
        self.digest_size = sum(self.storage.local[0].levels)

    @property
    def local(self):
        return self.storage.local

    def read(self, func: Callable, key: Key, *args, fetch: bool = True, **kwargs):
        value, success = self.storage.read(key, lambda x: func(x, *args, **kwargs), fetch=fetch)
        if success:
            return value

        if success is None:
            raise WriteError(f"The key {key} couldn't be written to any storage")
        if fetch:
            message = f'Key {key} is not present neither locally nor among your {len(self.storage.remote)} remotes'
        else:
            message = f'Key {key} is not present locally'
        raise ReadError(message)

    def write(self, file: PathLike) -> Key:
        file = Path(file)
        assert file.exists(), file
        key = digest_file(file, self.algorithm)
        if not self.storage.write(key, file, None):
            raise WriteError('The file could not be written to any storage')

        return key

    def fetch(self, keys: Iterable[Key], *, verbose: bool) -> Sequence[Key]:
        return self.storage.fetch(keys, None, verbose=verbose)

    def resolve(self, key: Key, *, fetch: bool = True) -> Path:
        """ This is not safe, but it's fast. """
        return self.read(lambda path: path, key, fetch=fetch)

    # deprecated names
    def get_path(self, key: Key, fetch: bool = True) -> Path:
        warnings.warn('The method `get_path` was renamed to `resolve`', UserWarning, 2)
        warnings.warn('The method `get_path` was renamed to `resolve`', DeprecationWarning, 2)
        return self.resolve(key, fetch=fetch)

    def load(self, func: Callable, key: Key, *args, fetch: bool = True, **kwargs) -> Any:
        warnings.warn('The method `load` was renamed to `read`', UserWarning, 2)
        warnings.warn('The method `load` was renamed to `read`', DeprecationWarning, 2)
        return self.read(func, key, *args, fetch=fetch, **kwargs)

    def store(self, file: PathLike) -> Key:
        warnings.warn('The method `store` was renamed to `write`', UserWarning, 2)
        warnings.warn('The method `store` was renamed to `write`', DeprecationWarning, 2)
        return self.write(file)
