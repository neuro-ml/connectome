import filecmp
import logging
import os
import errno
import shutil
from pathlib import Path
from typing import Optional

import humanfriendly
from tqdm import tqdm

from .config import root_params, load_config, make_locker, make_algorithm
from .digest import digest_to_relative, digest_file
from .utils import get_size, create_folders, to_read_only
from ..utils import PathLike

Key = str
FILENAME = 'data'
# TODO: make sure it's not a symlink
# TODO: generate a random temp name, or remove this altogether
TEMPFILE = '.temp'
logger = logging.getLogger(__name__)


class Disk:
    def __init__(self, root: PathLike):
        self.root = Path(root)
        self.permissions, self.group = root_params(self.root)
        self.config = config = load_config(self.root)
        assert set(config) <= {'hash', 'levels', 'max_size', 'free_disk_size', 'locker'}

        self.locker = make_locker(config)
        self._min_free_size = parse_size(config.get('free_disk_size', 0))
        self._max_size = parse_size(config.get('max_size'))

        if not self.locker.track_size:
            assert self._max_size is None or self._max_size == float('inf'), self._max_size

        self._hasher, self._folder_levels = make_algorithm(config)

    def _key_to_path(self, key: Key, temp: bool = False):
        name = TEMPFILE if temp else FILENAME
        return self.root / digest_to_relative(key, self._folder_levels) / name

    def _writeable(self):
        result = True

        if self._min_free_size > 0:
            result = result and shutil.disk_usage(self.root).free >= self._min_free_size

        if self._max_size is not None and self._max_size < float('inf'):
            result = result and self.locker.get_size() <= self._max_size

        return result

    def reserve_write(self, key: Key):
        self.locker.reserve_write(key)

    def release_write(self, key: Key):
        self.locker.stop_writing(key)

    def write(self, key: Key, file: Path) -> bool:
        file = Path(file)
        assert file.is_file(), file

        # TODO: copy to a different file. rename after consistency check
        stored = self._key_to_path(key)
        folder = stored.parent

        # check consistency
        if folder.exists():
            match_files(file, stored)
            return True

        temporary = self._key_to_path(key, True)
        if temporary.exists():
            raise ValueError(f'The storage is broken at {folder}')

        # make sure we can write
        if not self._writeable():
            return False

        # write
        create_folders(folder, self.permissions, self.group)

        try:
            copy_file(file, temporary)
            if self.locker.track_size:
                self.locker.inc_size(get_size(temporary))

        except BaseException as e:
            shutil.rmtree(folder)
            raise RuntimeError('An error occurred while copying the file') from e

        # TODO: need a final cache check
        # make file read-only
        to_read_only(temporary, self.permissions, self.group)
        temporary.rename(stored)
        digest = digest_file(stored, self._hasher)
        if digest != key:
            shutil.rmtree(folder)
            raise ValueError(f'The stored file has a wrong hash: expected {key} got {digest}. '
                             'The file was most likely corrupted while copying')

        return True

    def reserve_read(self, key: Key) -> Optional[Path]:
        path = self._key_to_path(key)
        temporary = self._key_to_path(key, True)

        self.locker.reserve_read(key)

        # something went really wrong
        if temporary.exists():
            self.locker.stop_reading(key)
            raise RuntimeError(f'The storage for {temporary.parent} appears to be broken.')

        if not path.exists():
            self.locker.stop_reading(key)
            return None

        return path

    def release_read(self, key: Key):
        self.locker.stop_reading(key)

    def remove(self, key: Key):
        file = self._key_to_path(key)
        folder = file.parent
        self.reserve_write(key)

        try:
            if not folder.exists():
                raise FileNotFoundError

            os.chmod(file, self.permissions)
            size = get_size(file)
            shutil.rmtree(folder)
            if self.locker.track_size:
                self.locker.dec_size(size)

        finally:
            self.release_write(key)

    def contains(self, key: Key):
        """ This is not safe, but it's fast. """
        path = self.reserve_read(key)
        if path is None:
            return False
        self.release_read(key)
        return True

    def actualize(self, verbose: bool):
        """ Useful for migration between locking mechanisms. """
        size = 0
        bar = tqdm(self.root.glob(f'**/{FILENAME}'), disable=not verbose)
        for file in bar:
            bar.set_description(str(file.parent.relative_to(self.root)))
            # TODO: add digest check
            assert not file.is_symlink()
            size += get_size(file)

        self.locker.set_size(size)


def copy_file(source, destination):
    # in Python>=3.8 the sendfile call is used, which apparently may fail
    try:
        shutil.copyfile(source, destination)
    except OSError as e:
        # BlockingIOError -> fallback to slow copy
        if e.errno != errno.EWOULDBLOCK:
            raise

        with open(source, 'rb') as src, open(destination, 'wb') as dst:
            shutil.copyfileobj(src, dst)


def match_files(first: Path, second: Path):
    if not filecmp.cmp(first, second, shallow=False):
        raise ValueError(f'Files do not match: {first} vs {second}')


def parse_size(x):
    if isinstance(x, int):
        return x
    if isinstance(x, str):
        return humanfriendly.parse_size(x)
    if x is not None:
        raise ValueError(f"Couldn't understand the size format: {x}")
