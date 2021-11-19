from datetime import datetime
import gzip
import logging
import os
import shutil
import warnings
from pathlib import Path
from typing import Any, Union, Set

from ...exceptions import StorageCorruption
from ...storage import Storage
from ...storage.digest import digest_to_relative, get_digest_size
from ...serializers import Serializer
from ...storage.disk import DiskBase
from ...storage.interface import QueryError, Key
from ...storage.utils import touch, create_folders, to_read_only, Reason
from ..compat import BadGzipFile

logger = logging.getLogger(__name__)

DATA_FOLDER = 'data'
TEMP_FOLDER = 'temp'
HASH_FILENAME = 'hash.bin'
TIME_FILENAME = 'time'
GZIP_COMPRESSION = 1


class CacheIndexStorage(DiskBase):
    def __init__(self, root: Path, storage: Storage, serializer: Serializer):
        super().__init__(root)
        self.storage = storage
        self.serializer = serializer

    def _check_consistency(self, base: Path, key: Key, value: Any, context):
        check_consistency(base / HASH_FILENAME, context, check_existence=True)

    def _write(self, base: Path, key: Key, value: Any, context: Any):
        data_folder, temp_folder = base / DATA_FOLDER, base / TEMP_FOLDER
        create_folders(data_folder, self.permissions, self.group)
        create_folders(temp_folder, self.permissions, self.group)

        self.serializer.save(value, temp_folder)
        self._mirror_to_storage(temp_folder, data_folder)
        self._save_meta(base, context)

    def _replicate(self, base: Path, key: Key, source: Path, context):
        raise NotImplementedError

    def _save_meta(self, local, pickled):
        hash_path, time_path = local / HASH_FILENAME, local / TIME_FILENAME
        # time
        with open(time_path, 'w'):
            pass
        os.chmod(time_path, 0o777)
        shutil.chown(time_path, group=self.group)
        # hash
        with gzip.GzipFile(hash_path, 'wb', compresslevel=GZIP_COMPRESSION, mtime=0) as file:
            file.write(pickled)
        to_read_only(hash_path, self.permissions, self.group)

    def _mirror_to_storage(self, source: Path, destination: Path):
        for file in source.glob('**/*'):
            target = destination / file.relative_to(source)
            if file.is_dir():
                target.mkdir(parents=True)

            else:
                with open(target, 'w') as fd:
                    fd.write(self.storage.write(file))
                os.remove(file)
                to_read_only(target, self.permissions, self.group)

        shutil.rmtree(source)

    def _cleanup_corrupted(self, folder, digest):
        warnings.warn(f'Corrupted storage at {self.root} for key {digest}. Cleaning up.', RuntimeWarning)
        shutil.rmtree(folder)

    def read(self, key, context):
        with self.locker.read(key):
            base = self.root / digest_to_relative(key, self.levels)
            if not base.exists():
                return None, False

            hash_path, time_path = base / HASH_FILENAME, base / TIME_FILENAME
            # we either have a valid folder
            if hash_path.exists() and time_path.exists():
                check_consistency(hash_path, context)
                touch(time_path)
                try:
                    # if couldn't find the hash - the cache is corrupted
                    return self.serializer.load(base / DATA_FOLDER, self.storage), True
                except QueryError:
                    pass

        # or it is corrupted, in which case we can remove it
        with self.locker.write(key):
            self._cleanup_corrupted(base, key)
            return None, False

    def inspect_entry(self, key: str, last_used: Union[float, datetime] = None) -> Union[Reason, Set[str]]:
        digest_size = get_digest_size(self.levels, string=True)
        if len(key) != digest_size:
            return Reason.WrongDigestSize

        data_key_size = self.storage.digest_size * 2
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

                    if len(data) != data_key_size:
                        return Reason.CorruptedData

                    hashes.add(data)

            return hashes


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
