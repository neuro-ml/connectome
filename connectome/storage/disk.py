import filecmp
import logging
import os
import errno
import shutil
from pathlib import Path
from typing import Optional, Union

from tqdm import tqdm

from .digest import digest_to_relative
from .locker import Locker, DummyLocker, wait_for_true
from ..utils import PathLike

Key = str
FILENAME = 'data'
# TODO: make sure it's not a symlink
TEMPFILE = '.temp'
logger = logging.getLogger(__name__)


class Disk:
    def __init__(self, root: PathLike, min_free_size: int = 0, max_size: int = None, locker: Locker = None,
                 permissions: Union[int, None] = None, group: Union[str, int, None] = None):
        if locker is None:
            locker = DummyLocker()
        if not locker.track_size:
            assert max_size is None or max_size == float('inf'), max_size

        self.root, self.permissions, self.group = init_root(root, permissions, group)
        self.min_free_space = min_free_size
        self.max_size = max_size

        self._locker = locker
        self._sleep_time = 0.01
        self._sleep_iters = int(600 / self._sleep_time) or 1  # 10 minutes
        self._prefix_size = 2

    def _key_to_path(self, key: Key, temp: bool = False):
        name = TEMPFILE if temp else FILENAME
        return self.root / digest_to_relative(key) / name

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
        wait_for_true(self._locker.start_writing, key, self._sleep_time, self._sleep_iters)

    def release_write(self, key: Key):
        key = self._to_lock_key(key)
        self._locker.stop_writing(key)

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
            if self._locker.track_size:
                self._locker.inc_size(temporary.stat().st_size)

        except BaseException as e:
            shutil.rmtree(folder)
            raise RuntimeError('An error occurred while copying the file') from e

        # make file read-only
        to_read_only(temporary, self.permissions, self.group)
        temporary.rename(stored)
        return True

    def reserve_read(self, key: Key) -> Optional[Path]:
        path = self._key_to_path(key)
        temporary = self._key_to_path(key, True)
        key = self._to_lock_key(key)

        wait_for_true(self._locker.start_reading, key, self._sleep_time, self._sleep_iters)

        # something went really wrong
        if temporary.exists():
            self._locker.stop_reading(key)
            raise RuntimeError(f'The storage for {temporary.parent} appears to be broken.')

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


def mkdir(path: Path, permissions: Union[int, None], group: Union[str, int, None]):
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
    if not filecmp.cmp(first, second, shallow=False):
        raise ValueError(f'Files do not match: {first} vs {second}')


def init_root(root: PathLike, permissions: Union[int, None], group: Union[str, int, None]):
    root = Path(root)

    if not root.exists():
        mkdir(root, permissions, group)

    root_permissions = root.stat().st_mode & 0o777
    if permissions is None:
        permissions = root_permissions
    else:
        assert permissions == root_permissions
    if group is None:
        group = root.group()
    else:
        assert root.group() == group

    return root, permissions, group


def to_read_only(path: Path, permissions, group):
    os.chmod(path, 0o444 & permissions)
    shutil.chown(path, group=group)
