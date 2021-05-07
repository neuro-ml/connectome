import getpass
import gzip
import json
import logging
import os
import shutil
import time
from pathlib import Path
from typing import Any, Union, Tuple

from .base import Cache
from .pickler import dumps, PREVIOUS_VERSIONS, LATEST_VERSION
from ..storage import Storage
from ..storage.digest import digest_to_relative, digest_bytes
from ..storage.disk import wait_for_true, init_root, create_folders, to_read_only, get_size
from ..engine import NodeHash
from ..serializers import Serializer
from ..storage.locker import Locker

logger = logging.getLogger(__name__)

DATA_FOLDER = 'data'
HASH_FILENAME = 'hash.bin'
META_FILENAME = 'meta.json'
GZIP_COMPRESSION = 1
Key = str


class DiskCache(Cache):
    def __init__(self, root: Path, storage: Storage, serializer: Serializer, metadata: dict, locker: Locker,
                 permissions: Union[int, None] = None, group: Union[str, int, None] = None):
        super().__init__()
        self.root, self.permissions, self.group = init_root(root, permissions, group)
        self.metadata = metadata
        self.serializer = serializer
        self.storage = storage

        self._locker = locker
        self._sleep_time = 0.1
        self._sleep_iters = int(600 / self._sleep_time) or 1  # 10 minutes

    def get(self, param: NodeHash) -> Tuple[Any, bool]:
        key = param.value
        pickled, digest = key_to_digest(key)

        # try to load
        value, exists = self._load(digest, pickled)
        if exists:
            return value, exists

        # the cache is empty, but we can try an restore it from an old version
        for version in reversed(PREVIOUS_VERSIONS):
            local_pickled, local_digest = key_to_digest(key, version)

            # we can simply copy the previous version, because nothing really changed
            value, exists = self._load(local_digest, local_pickled)
            if exists:
                return value, exists

        return None, False

    def set(self, param: NodeHash, value: Any):
        pickled, digest = key_to_digest(param.value)
        wait_for_true(self._locker.start_writing, digest, self._sleep_time, self._sleep_iters)
        try:
            self._save_value(digest, value, pickled)
        finally:
            self._locker.stop_writing(digest)

    def _digest_exists(self, digest: str):
        return (self.root / digest_to_relative(digest)).exists()

    def _load(self, digest, pickled):
        wait_for_true(self._locker.start_reading, digest, self._sleep_time, self._sleep_iters)
        try:
            if not self._digest_exists(digest):
                return None, False

            base = self.root / digest_to_relative(digest)
            # TODO: how slow is this?
            check_consistency(base / HASH_FILENAME, pickled)
            return self.serializer.load(base / DATA_FOLDER), True

        finally:
            self._locker.stop_reading(digest)

    def _save_value(self, digest: str, value, pickled):
        base = self.root / digest_to_relative(digest)
        if base.exists():
            check_consistency(base / HASH_FILENAME, pickled)
            # TODO: also compare the raw bytes of `value` and dumped value
            return

        data_folder = base / DATA_FOLDER
        create_folders(data_folder, self.permissions, self.group)

        try:
            # data
            self.serializer.save(value, data_folder)
            # meta
            self._save_meta(base, pickled)
            self._mirror_to_storage(data_folder)

        except BaseException as e:
            shutil.rmtree(base)
            raise RuntimeError('An error occurred while creating the cache. Cleaned up.') from e

    def _save_meta(self, local, pickled):
        # hash
        hash_path = local / HASH_FILENAME
        meta_path = local / META_FILENAME
        if hash_path.exists():
            check_consistency(hash_path, pickled)
            return

        save_hash(hash_path, pickled)

        # user meta
        meta = self.metadata.copy()
        meta.update({
            'time': time.time(),
            # TODO: this can possibly fail
            'user': getpass.getuser(),
        })
        with open(meta_path, 'w') as file:
            json.dump(meta, file)

        to_read_only(hash_path, self.permissions, self.group)
        to_read_only(meta_path, self.permissions, self.group)
        if self._locker.track_size:
            self._locker.inc_size(get_size(hash_path) + get_size(meta_path))

    def _mirror_to_storage(self, folder: Path):
        for file in folder.glob('**/*'):
            if file.is_dir():
                continue

            # FIXME
            path = self.storage.get_path(self.storage.store(file))
            assert path.exists(), path
            os.remove(file)
            file.symlink_to(path)


def key_to_digest(key, version=LATEST_VERSION):
    pickled = dumps(key, version=version)
    digest = digest_bytes(pickled)
    return pickled, digest


def check_consistency(hash_path, pickled):
    with gzip.GzipFile(hash_path, 'rb') as file:
        dumped = file.read()
        if dumped != pickled:
            raise RuntimeError(f'The dumped and current pickle do not match at {hash_path}: {dumped} {pickled}')


def save_hash(hash_path, pickled):
    with gzip.GzipFile(hash_path, 'wb', compresslevel=GZIP_COMPRESSION, mtime=0) as file:
        file.write(pickled)
