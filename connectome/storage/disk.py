import filecmp
import logging
import errno
import shutil
from datetime import datetime
from pathlib import Path
from typing import Set, Union, Any, Tuple, Callable

from .digest import digest_file, get_digest_size
from .interface import Key
from .local import DiskBase
from .utils import to_read_only, Reason

FILENAME = 'data'
logger = logging.getLogger(__name__)


class Disk(DiskBase):
    def read(self, key, context: Callable) -> Tuple[Any, bool]:
        base = self._key_to_base(key)

        with self.locker.read(key):
            if not base.exists():
                return None, False

            return context(base / FILENAME), True

    def _check_consistency(self, base: Path, key: Key, value: Path, context):
        match_files(value, self._key_to_base(key) / FILENAME)

    def _write(self, base: Path, key: Key, value: Path, context):
        value = Path(value)
        assert value.is_file(), value

        file = base / FILENAME
        copy_file(value, file)
        # make file read-only
        to_read_only(file, self.permissions, self.group)
        digest = digest_file(file, self.algorithm)
        if digest != key:
            shutil.rmtree(base)
            raise ValueError(f'The stored file has a wrong hash: expected {key} got {digest}. '
                             'The file was most likely corrupted while copying')

    def _replicate(self, base: Path, key: Key, source: Path, context):
        self._write(base, key, source / FILENAME, context)

    def inspect_entry(self, key: Key, allowed_keys: Set[Key] = None, created: Union[float, datetime] = None):
        if len(key) != get_digest_size(self.levels, string=True):
            return Reason.WrongDigestSize

        base = self._key_to_base(key)

        # we remove missing hashes only if they are older than a given age
        if allowed_keys and key not in allowed_keys:
            if created is None:
                return Reason.Filtered
            if isinstance(created, datetime):
                created = created.timestamp()
            if base.stat().st_mtime < created:
                return Reason.Filtered

        with self.locker.read(key):
            if not (base / FILENAME).exists():
                return Reason.CorruptedData

            if digest_file(base / FILENAME, self.algorithm) != key:
                return Reason.WrongHash


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
