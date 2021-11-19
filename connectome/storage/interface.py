import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable, Tuple, Callable, Any, Sequence, Union

from tqdm import tqdm

from .config import HashConfig

Key = str
logger = logging.getLogger(__name__)


class StorageError(Exception):
    pass


class WriteError(StorageError):
    pass


class ReadError(StorageError):
    pass


# TODO: deprecated
StoreError = WriteError
QueryError = ReadError


class LocalStorage(ABC):
    def __init__(self, hash: HashConfig, levels: Sequence[int]):
        self.hash = hash
        self.algorithm = self.hash.build()
        self.levels = levels

    @abstractmethod
    def write(self, key, value: Any, context: Any) -> bool:
        """
        Write a ``value`` to a given ``key``.
        Returns True if the value was written (or already present).
        """

    @abstractmethod
    def read(self, key, context: Any) -> Tuple[Any, bool]:
        """
        Read the value given the ``key``.
        Returns a pair (value, success).
        If success is False - the value could not be read.
        """

    @abstractmethod
    def contains(self, key, context: Any) -> bool:
        """
        Returns whether the ``key`` is present in the storage.
        """

    @abstractmethod
    def replicate(self, key, source: Path, context: Any) -> bool:
        """
        Populates the storage at ``key`` from a ``source`` path.
        """


class RemoteStorage:
    hash: HashConfig
    levels: Sequence[int]

    @abstractmethod
    def fetch(self, keys: Sequence[Key], store: Callable[[str, Path], Any],
              config: HashConfig) -> Sequence[Tuple[Any, bool]]:
        """
        Fetches the value for ``key`` from a remote location.
        """


class HashStorage:
    def __init__(self, local: Sequence[LocalStorage], remote: Sequence[RemoteStorage] = ()):
        if not local:
            raise ValueError('The storage must have at least 1 local config')

        reference = local[0].hash
        for loc in local[1:]:
            if loc.hash != reference:
                raise ValueError('Local storage locations have inconsistent hash algorithms')

        self.local, self.remote, self.hash = local, remote, reference

    def write(self, key, value, context) -> bool:
        for local in self.local:
            if local.write(key, value, context):
                return True

        return False

    def read(self, key, context, *, fetch: bool) -> Tuple[Any, Union[None, bool]]:
        # try to find locally
        for local in self.local:
            value, success = local.read(key, context)
            if success:
                return value, True

        # try to fetch from remote
        status = False
        if fetch:
            for remote in self.remote:
                (local, success), = remote.fetch([key], lambda k, base: self._replicate(k, base, context), self.hash)
                if success:
                    if local is WriteError:
                        status = None
                        continue

                    value, exists = local.read(key, context)
                    assert exists, exists
                    return value, True

        return None, status

    def fetch(self, keys: Iterable[Key], context, *, verbose: bool) -> Sequence[Key]:
        def store(k, base):
            status = self._replicate(k, base, context)
            bar.update()
            return status if status is WriteError else k

        keys = set(keys)
        bar = tqdm(disable=not verbose, total=len(keys))
        present = set()
        for local in self.local:
            for key in keys:
                if local.contains(key, context):
                    present.add(key)
                    bar.update()

        keys -= present
        logger.info(f'Fetch: {len(present)} keys already present, fetching {len(keys)}')

        for remote in self.remote:
            if not keys:
                break

            logger.info(f'Trying remote {remote}')
            keys -= {
                k for k, success in remote.fetch(list(keys), store, self.hash)
                if success and k is not WriteError
            }

        return list(keys)

    def _replicate(self, key, base, context):
        for local in self.local:
            if local.replicate(key, base, context):
                return local

        return WriteError
