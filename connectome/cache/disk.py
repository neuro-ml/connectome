from datetime import datetime
import gzip
import logging
import os
import shutil
import warnings
from pathlib import Path
from typing import Any, Tuple, Union, Set

from ..exceptions import StorageCorruption
from ..storage import Storage
from ..storage.config import root_params, make_algorithm, load_config, make_locker, DiskConfig
from ..storage.digest import digest_to_relative, get_digest_size
from ..engine import NodeHash
from ..serializers import Serializer
from ..storage.storage import QueryError
from ..storage.utils import touch, create_folders, to_read_only, get_size, Reason
from .base import Cache
from .pickler import dumps, PREVIOUS_VERSIONS
from .compat import BadGzipFile

logger = logging.getLogger(__name__)

DATA_FOLDER = 'data'
TEMP_FOLDER = 'temp'
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

        config = load_config(self.root, DiskConfig)
        self.algorithm = make_algorithm(config.hash)
        self.levels = config.levels
        self.locker = make_locker(config.locker)

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
                check_consistency(hash_path, pickled)
                touch(time_path)
                try:
                    # if couldn't find the hash - the cache is corrupted
                    return self.serializer.load(base / DATA_FOLDER, self.storage), True
                except QueryError:
                    pass

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

            data_folder = base / DATA_FOLDER
            temp_folder = base / TEMP_FOLDER
            create_folders(data_folder, self.permissions, self.group)
            create_folders(temp_folder, self.permissions, self.group)

            try:
                # data
                self.serializer.save(value, temp_folder)
                self._mirror_to_storage(temp_folder, data_folder)
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

    def _mirror_to_storage(self, source: Path, destination: Path):
        for file in source.glob('**/*'):
            target = destination / file.relative_to(source)
            if file.is_dir():
                target.mkdir(parents=True)

            else:
                with open(target, 'w') as fd:
                    fd.write(self.storage.store(file))
                os.remove(file)
                to_read_only(target, self.permissions, self.group)

        shutil.rmtree(source)

    def _cleanup_corrupted(self, folder, digest):
        warnings.warn(f'Corrupted storage at {self.root} for key {digest}. Cleaning up.', RuntimeWarning)
        shutil.rmtree(folder)

    def inspect_entry(self, key: str, last_used: Union[float, datetime] = None) -> Union[Reason, Set[str]]:
        digest_size = get_digest_size(self.levels, string=True)
        if len(key) != digest_size:
            return Reason.WrongDigestSize

        data_digest_size = self.storage.get_digest_size(True)
        base = self.root / digest_to_relative(key, self.levels)
        with self.locker.read(key):
            if {x.name for x in base.iterdir()} != {HASH_FILENAME, DATA_FOLDER, TIME_FILENAME}:
                return Reason.WrongFolderStructure

            if last_used is not None:
                if isinstance(last_used, datetime):
                    last_used = last_used.timestamp()
                if (base / TIME_FILENAME).stat().st_mtime < last_used:
                    return Reason.Expired

            try:
                with gzip.GzipFile(base / HASH_FILENAME, 'rb') as file:
                    hash_bytes = file.read()

            except BadGzipFile:
                return Reason.CorruptedHash

            real_digest = self.algorithm(hash_bytes).hexdigest()
            if key != real_digest:
                return Reason.WrongHash

            hashes = set()
            for file in (base / DATA_FOLDER).glob('**/*'):
                if file.is_dir():
                    continue

                if not file.is_file():
                    return Reason.CorruptedData

                with open(file, 'r') as content:
                    try:
                        data = content.read().strip()
                    except UnicodeDecodeError:
                        return Reason.CorruptedData

                    if len(data) != data_digest_size:
                        return Reason.CorruptedData

                    hashes.add(data)

            return hashes


def key_to_digest(algorithm, key, version=None):
    pickled = dumps(key, version=version)
    digest = algorithm(pickled).hexdigest()
    return pickled, digest


def check_consistency(hash_path, pickled, check_existence: bool = False):
    suggestion = f'You may want to delete the {hash_path.parent} folder.'
    if check_existence and not hash_path.exists():
        raise StorageCorruption(f'The pickled graph is missing. {suggestion}')
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
