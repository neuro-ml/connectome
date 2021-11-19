import logging
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from tqdm import tqdm

from .config import root_params, load_config, make_locker
from .digest import digest_to_relative
from .interface import LocalStorage, Key
from .utils import get_size, create_folders
from ..utils import PathLike

logger = logging.getLogger(__name__)


class DiskBase(LocalStorage, ABC):
    def __init__(self, root: PathLike):
        config = load_config(root)
        super().__init__(config.hash, config.levels)
        self.root = Path(root)
        self.permissions, self.group = root_params(self.root)

        self.locker = make_locker(config.locker)
        self.min_free_size = config.free_disk_size
        self.max_size = config.max_size

        if not self.locker.track_size:
            assert self.max_size is None or self.max_size == float('inf'), self.max_size

    def _key_to_base(self, key: Key):
        return self.root / digest_to_relative(key, self.levels)

    def _writeable(self):
        result = True

        if self.min_free_size > 0:
            result = result and shutil.disk_usage(self.root).free >= self.min_free_size

        if self.max_size is not None and self.max_size < float('inf'):
            result = result and self.locker.get_size() <= self.max_size

        return result

    @staticmethod
    def _get_size(base: Path):
        return sum(get_size(file) for file in base.glob('**/*') if file.is_file())

    def _protected_write(self, key, value: Any, context, write) -> bool:
        base = self._key_to_base(key)
        with self.locker.write(key):
            # if already stored
            if base.exists():
                self._check_consistency(base, key, value, context)
                return True

            # make sure we can write
            if not self._writeable():
                return False

            try:
                # create base folder
                create_folders(base, self.permissions, self.group)
                # populate the folder
                write(base, key, value, context)
                # increase the storage size
                if self.locker.track_size:
                    self.locker.inc_size(self._get_size(base))

            except BaseException as e:
                if base.exists():
                    shutil.rmtree(base)
                raise RuntimeError('An error occurred while copying the file') from e

            return True

    def write(self, key, value: Any, context: Any) -> bool:
        return self._protected_write(key, value, context, self._write)

    def replicate(self, key, source: Path, context: Any) -> bool:
        return self._protected_write(key, source, context, self._replicate)

    def remove(self, key: Key) -> bool:
        base = self._key_to_base(key)
        with self.locker.write(key):
            if not base.exists():
                return False

            size = self._get_size(base)
            shutil.rmtree(base)
            if self.locker.track_size:
                self.locker.dec_size(size)

            return True

    def contains(self, key: Key, context):
        """ This is not safe, but it's fast. """
        with self.locker.read(key):
            return self._key_to_base(key).exists()

    def actualize(self, verbose: bool):
        """ Useful for migration between locking mechanisms. """
        # TODO: need a global lock
        size = 0
        bar = tqdm(self.root.glob(f'**/*'), disable=not verbose)
        for file in bar:
            if file.is_dir():
                continue

            bar.set_description(str(file.parent.relative_to(self.root)))
            assert file.is_file()
            size += get_size(file)

        self.locker.set_size(size)

    @abstractmethod
    def _check_consistency(self, base: Path, key: Key, value: Any, context):
        pass

    @abstractmethod
    def _write(self, base: Path, key: Key, value: Any, context):
        pass

    @abstractmethod
    def _replicate(self, base: Path, key: Key, source: Path, context):
        pass
