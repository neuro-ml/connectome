import gzip
import logging
import os
import shutil
import warnings
from pathlib import Path
from typing import Any, Tuple

from ..exceptions import StorageCorruption
from ..storage import Storage
from ..storage.config import root_params, make_algorithm, load_config, make_locker
from ..storage.digest import digest_to_relative
from ..engine import NodeHash
from ..serializers import Serializer
from ..storage.utils import touch, create_folders, to_read_only, get_size
from .base import Cache
from .pickler import dumps, PREVIOUS_VERSIONS
from .compat import BadGzipFile

logger = logging.getLogger(__name__)

DATA_FOLDER = 'data'
HASH_FILENAME = 'hash.bin'
TIME_FILENAME = 'time'
GZIP_COMPRESSION = 1
Key = str


class DiskCache(Cache):
    def __init__(self, root: Path, storage: Storage, serializer: Serializer):
        super().__init__()
        self.root = Path(root)
        self.permissions, self.group = root_params(self.root)
        self.serializer = serializer
        self.storage = storage

        config = load_config(self.root)
        assert set(config) <= {'hash', 'levels', 'locker'}

        self.algorithm, self.levels = make_algorithm(config)
        self.locker = make_locker(config)

    def get(self, param: NodeHash) -> Tuple[Any, bool]:
        key = param.value
        pickled, digest = key_to_digest(self.algorithm, key)
        logger.info('Writing %s', digest)

        # try to load
        value, exists = self._load(digest, pickled)
        if exists:
            return value, exists

        # the cache is empty, but we can try an restore it from an old version
        for version in reversed(PREVIOUS_VERSIONS):
            local_pickled, local_digest = key_to_digest(self.algorithm, key, version)

            # we can simply load the previous version, because nothing really changed
            value, exists = self._load(local_digest, local_pickled)
            if exists:
                # and store it for faster access next time
                self._save(digest, value, pickled)
                return value, exists

        return None, False

    def set(self, param: NodeHash, value: Any):
        pickled, digest = key_to_digest(self.algorithm, param.value)
        logger.info('Reading %s', digest)
        self._save(digest, value, pickled)

    def _load(self, digest, pickled):
        with self.locker.read(digest):
            base = self.root / digest_to_relative(digest, self.levels)
            if not base.exists():
                return None, False

            hash_path, time_path = base / HASH_FILENAME, base / TIME_FILENAME
            # we either have a valid folder
            if hash_path.exists() and time_path.exists():
                # TODO: how slow is this?
                check_consistency(hash_path, pickled)
                touch(time_path)
                return self.serializer.load(base / DATA_FOLDER), True

        # or it is corrupted, in which case we can remove it
        with self.locker.write(digest):
            self._cleanup_corrupted(base, digest)
            return None, False

    def _save(self, digest: str, value, pickled):
        with self.locker.write(digest):
            base = self.root / digest_to_relative(digest, self.levels)
            if base.exists():
                check_consistency(base / HASH_FILENAME, pickled, check_existence=True)
                # TODO: also compare the raw bytes of `value` and dumped value?
                return

            # TODO: need a temp data folder
            data_folder = base / DATA_FOLDER
            create_folders(data_folder, self.permissions, self.group)

            try:
                # data
                self.serializer.save(value, data_folder)
                self._mirror_to_storage(data_folder)
                # meta
                size = self._save_meta(base, pickled)
                if self.locker.track_size:
                    self.locker.inc_size(size)

            except BaseException as e:
                shutil.rmtree(base)
                raise RuntimeError(f'An error occurred while caching at {base}. Cleaned up.') from e

    def _save_meta(self, local, pickled):
        hash_path, time_path = local / HASH_FILENAME, local / TIME_FILENAME
        # time
        with open(time_path, 'w'):
            pass
        os.chmod(time_path, 0o777)
        shutil.chown(time_path, group=self.group)
        # hash
        save_hash(hash_path, pickled)
        to_read_only(hash_path, self.permissions, self.group)
        return get_size(hash_path)

    def _mirror_to_storage(self, folder: Path):
        for file in folder.glob('**/*'):
            if file.is_dir():
                continue

            # FIXME
            path = self.storage.get_path(self.storage.store(file))
            # TODO: this might be incorrect if the user changes the cwd
            if not path.is_absolute():
                path = Path(os.getcwd()) / path

            assert path.exists(), path
            os.remove(file)
            file.symlink_to(path)

    def _cleanup_corrupted(self, folder, digest):
        warnings.warn(f'Corrupted storage at {self.root} for key {digest}. Cleaning up.', RuntimeWarning)
        shutil.rmtree(folder)


def key_to_digest(algorithm, key, version=None):
    pickled = dumps(key, version=version)
    digest = algorithm(pickled).hexdigest()
    return pickled, digest


def check_consistency(hash_path, pickled, check_existence: bool = False):
    if check_existence and not hash_path.exists():
        raise StorageCorruption(f'The pickled graph is missing. You may want to delete the {hash_path.parent} folder.')

    suggestion = f'You may want to delete the {hash_path.parent} folder.'
    try:
        with gzip.GzipFile(hash_path, 'rb') as file:
            dumped = file.read()
            if dumped != pickled:
                raise StorageCorruption(
                    f'The dumped and current pickle do not match at {hash_path}: {dumped} {pickled}. {suggestion}'
                )
    except BadGzipFile:
        raise StorageCorruption(f'The hash is corrupted. {suggestion}') from None


def save_hash(hash_path, pickled):
    with gzip.GzipFile(hash_path, 'wb', compresslevel=GZIP_COMPRESSION, mtime=0) as file:
        file.write(pickled)
