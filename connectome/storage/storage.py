import logging
import tempfile
from pathlib import Path
from typing import Sequence, Iterable, Callable, Any

from tqdm import tqdm

from .digest import digest_file
from .disk import Disk
from .interface import RemoteLocation
from ..utils import PathLike

Key = str
logger = logging.getLogger(__name__)


class StorageError(Exception):
    pass


class Storage:
    def __init__(self, local: Sequence[Disk], remote: Sequence[RemoteLocation] = ()):
        self.local, self.remote = local, remote

    def store(self, file: PathLike) -> Key:
        file = Path(file)
        assert file.exists(), file
        key = digest_file(file)
        self._store(key, file)
        return key

    def get_path(self, key: Key, name: str = None) -> Any:
        """ This is not safe, but it's fast. """
        path, storage = self._find_storage(key)

        try:
            if name is None:
                return path

            link = path.parent / name
            if not link.exists():
                link.symlink_to(path.name)

            return link

        finally:
            storage.release_read(key)

    def load(self, func: Callable, key: Key, *args, **kwargs) -> Any:
        path, storage = self._find_storage(key)

        try:
            return func(path, *args, **kwargs)

        finally:
            storage.release_read(key)

    def fetch(self, keys: Iterable[Key], verbose: bool) -> Sequence[Key]:
        def store(a, b):
            self._store(a, b)
            bar.update()

        keys = set(keys)
        bar = tqdm(disable=not verbose, total=len(keys))
        present = set()
        for storage in self.local:
            for key in list(keys):
                if storage.contains(key):
                    present.add(key)
                    bar.update()

        keys -= present
        logger.info(f'Fetch: {len(present)} keys already present, fetching {len(keys)}')

        for storage in self.remote:
            if not keys:
                break

            logger.info(f'Trying remote {storage}')
            keys -= set(storage.fetch(list(keys), store))

        return list(keys)

    def _store(self, key: Key, file: Path):
        for storage in self.local:
            storage.reserve_write(key)

        try:
            for storage in self.local:
                if storage.write(key, file):
                    return storage

        finally:
            for storage in self.local:
                storage.release_write(key)

        raise StorageError('The file could not be written to any storage.')

    def _find_storage(self, key: Key):
        # find in local
        for storage in self.local:
            path = storage.reserve_read(key)
            if path is not None:
                return path, storage

        # fetch
        with tempfile.TemporaryDirectory() as folder:
            file = Path(folder) / 'file'
            for remote in self.remote:
                with remote:
                    if remote.download(key, file):
                        # TODO: this is not safe
                        #  need an atomic write_and_read
                        storage = self._store(key, file)
                        path = storage.reserve_read(key)
                        assert path is not None
                        return path, storage

        raise KeyError(key)