import getpass
import gzip
import json
import logging
import os
import shutil
import time
from hashlib import blake2b
from pathlib import Path
from typing import Any

from .base import Cache
from .pickler import dumps, PREVIOUS_VERSIONS, LATEST_VERSION
from ..storage import Storage
from ..storage.digest import digest_to_relative
from ..storage.disk import wait_for_true
from ..storage.local import copy_group_permissions, create_folders, DIGEST_SIZE
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
    def __init__(self, root: Path, storage: Storage, serializer: Serializer, metadata: dict, locker: Locker):
        super().__init__()
        self.metadata = metadata
        self.serializer = serializer
        self.storage = storage

        # TODO: pass permissions and group
        self.root = Path(root)

        self._locker = locker
        self._sleep_time = 0.1
        self._sleep_iters = int(600 / self._sleep_time) or 1  # 10 minutes

    def reserve_read(self, param: NodeHash) -> bool:
        key = param.value
        pickled, digest, _ = key_to_relative(key)

        wait_for_true(self._locker.start_reading, digest, self._sleep_time, self._sleep_iters)
        try:
            if self._digest_exists(digest):
                return True

        except BaseException:
            self._locker.stop_reading(digest)
            raise

        self._locker.stop_reading(digest)
        # the cache is empty, but we can try an restore it from an old version
        for version in reversed(PREVIOUS_VERSIONS):
            local_pickled, local_digest, _ = key_to_relative(key, version)

            # we can simply copy the previous version, because nothing really changed
            exists, value = self._load(local_digest, local_pickled)
            if exists:
                # and update the new version
                self._save(digest, value, pickled)
                return True

        return False

    def fail(self, param: NodeHash, read: bool):
        if read:
            _, digest, _ = key_to_relative(param.value)
            self._locker.stop_reading(digest)

    def set(self, param: NodeHash, value: Any):
        pickled, digest, _ = key_to_relative(param.value)
        self._save(digest, value, pickled)

    def get(self, param: NodeHash) -> Any:
        pickled, digest, _ = key_to_relative(param.value)
        exists, value = self._load(digest, pickled)
        if not exists:
            raise KeyError(digest)
        return value

    def _digest_exists(self, digest: str):
        return (self.root / digest_to_relative(digest)).exists()

    def _load(self, digest, pickled):
        try:
            if not self._digest_exists(digest):
                return False, None

            root = self.root / digest_to_relative(digest)
            # TODO: how slow is this?
            check_consistency(root / HASH_FILENAME, pickled)
            return True, self.serializer.load(root / DATA_FOLDER)

        finally:
            self._locker.stop_reading(digest)

    def _save(self, digest: str, value, pickled):
        wait_for_true(self._locker.start_writing, digest, self._sleep_time, self._sleep_iters)
        try:
            self._save_value(digest, value, pickled)
        finally:
            self._locker.stop_writing(digest)

    def _save_value(self, digest: str, value, pickled):
        root = self.root / digest_to_relative(digest)
        if root.exists():
            check_consistency(root / HASH_FILENAME, pickled)
            # TODO: also compare the raw bytes of `value` and dumped value
            return

        data_folder = root / DATA_FOLDER
        create_folders(data_folder, self.root)

        try:
            # data
            self.serializer.save(value, data_folder)
            # meta
            self._save_meta(root, pickled)

            copy_group_permissions(root, self.root, recursive=True)
            self._mirror_to_storage(data_folder)

        except BaseException as e:
            shutil.rmtree(root)
            raise RuntimeError('An error occurred while creating the cache. Cleaned up.') from e

    def _save_meta(self, local, pickled):
        # hash
        hash_path = local / HASH_FILENAME
        if hash_path.exists():
            check_consistency(hash_path, pickled)
        else:
            save_hash(hash_path, pickled)

        # user meta
        meta = self.metadata.copy()
        meta.update({
            'time': time.time(),
            # TODO: this can possibly fail
            'user': getpass.getuser(),
        })
        with open(local / META_FILENAME, 'w') as file:
            json.dump(meta, file)

    def _mirror_to_storage(self, folder: Path):
        for file in folder.glob('**/*'):
            if file.is_dir():
                continue

            # FIXME
            path = self.storage.get_path(self.storage.store(file))
            assert path.exists(), path
            os.remove(file)
            file.symlink_to(path)


def digest_bytes(pickled: bytes) -> str:
    return blake2b(pickled, digest_size=DIGEST_SIZE).hexdigest()


def key_to_relative(key, version=LATEST_VERSION):
    pickled = dumps(key, version=version)
    digest = digest_bytes(pickled)
    relative = digest_to_relative(digest)
    return pickled, digest, relative


def check_consistency(hash_path, pickled):
    with gzip.GzipFile(hash_path, 'rb') as file:
        dumped = file.read()
        if dumped != pickled:
            raise RuntimeError(f'The dumped and current pickle do not match at {hash_path}: {dumped} {pickled}')


def save_hash(hash_path, pickled):
    with gzip.GzipFile(hash_path, 'wb', compresslevel=GZIP_COMPRESSION, mtime=0) as file:
        file.write(pickled)
