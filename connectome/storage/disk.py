import filecmp
import logging
import os
import errno
import shutil
import time
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from .digest import digest_to_relative
from .locker import Locker, DummyLocker
from ..utils import PathLike

Key = str
FILENAME = 'data'
logger = logging.getLogger(__name__)


class Disk:
    def __init__(self, root: PathLike, min_free_size: int = 0, max_size: int = None, locker: Locker = None):
        # TODO: move to args
        group = None
        permissions = None
        root = Path(root)
        if locker is None:
            locker = DummyLocker()

        if not root.exists():
            assert group is not None and permissions is not None
            mkdir(root, permissions, group)

        # TODO: need consistency check
        if permissions is None:
            permissions = root.stat().st_mode & 0o777
        if group is None:
            group = root.group()

        if not locker.track_size:
            assert max_size is None or max_size == float('inf'), max_size

        self.root = root
        self.permissions = permissions
        self.group = group
        self.min_free_space = min_free_size
        self.max_size = max_size

        self._locker = locker
        self._sleep_time = 0.01
        self._sleep_iters = int(600 / self._sleep_time) or 1  # 10 minutes
        self._prefix_size = 2

    def _key_to_path(self, key: Key):
        return self.root / digest_to_relative(key) / FILENAME

    def _to_lock_key(self, key: Key):
        return key[:self._prefix_size]

    def _writeable(self):
        result = True

        if self.min_free_space > 0:
            result = result and shutil.disk_usage(self.root).free >= self.min_free_space

        if self.max_size is not None and self.max_size < float('inf'):
            result = result and self._locker.get_size() <= self.max_size

        return result

    def reserve_write(self, key: Key):
        key = self._to_lock_key(key)
        i = 0
        while not self._locker.start_writing(key):
            if i >= self._sleep_iters:
                raise RuntimeError(f"Can't start writing for key {key}. It seems like you've hit a deadlock.")

            time.sleep(self._sleep_time)
            i += 1

    def release_write(self, key: Key):
        key = self._to_lock_key(key)
        self._locker.stop_writing(key)

    def write(self, key: Key, file: Path) -> bool:
        file = Path(file)
        assert file.is_file(), file

        stored = self._key_to_path(key)
        folder = stored.parent

        # check consistency
        if folder.exists():
            match_files(file, stored)
            return True

        # make sure we can write
        if not self._writeable():
            return False

        # write
        create_folders(folder, self.permissions, self.group)

        try:
            copy_file(file, stored)
            if self._locker.track_size:
                self._locker.inc_size(stored.stat().st_size)

        except BaseException as e:
            shutil.rmtree(folder)
            raise RuntimeError('An error occurred while copying the file') from e

        # make file read-only
        os.chmod(stored, 0o444 & self.permissions)
        return True

    def reserve_read(self, key: Key) -> Optional[Path]:
        path = self._key_to_path(key)
        key = self._to_lock_key(key)

        i = 0
        while not self._locker.start_reading(key):
            if i >= self._sleep_iters:
                raise RuntimeError(f"Can't start reading for key {key}. It seems like you've hit a deadlock.")

            time.sleep(self._sleep_time)
            i += 1

        if not path.exists():
            self._locker.stop_reading(key)
            return None

        return path

    def release_read(self, key: Key):
        key = self._to_lock_key(key)
        self._locker.stop_reading(key)

    def remove(self, key: Key):
        file = self._key_to_path(key)
        folder = file.parent
        self.reserve_write(key)

        try:
            if not folder.exists():
                raise FileNotFoundError

            os.chmod(file, self.permissions)
            shutil.rmtree(folder)

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
            size += file.stat().st_size

        self._locker.set_size(size)


def mkdir(path: Path, permissions, group):
    path.mkdir()
    if permissions is not None:
        path.chmod(permissions)
    if group is not None:
        shutil.chown(path, group=group)


def create_folders(path: Path, permissions, group):
    if not path.exists():
        create_folders(path.parent, permissions, group)
        mkdir(path, permissions, group)


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
    return filecmp.cmp(first, second, shallow=False)
