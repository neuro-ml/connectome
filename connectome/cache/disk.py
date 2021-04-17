import getpass
import gzip
import json
import logging
import os
import shutil
import time
from functools import partial
from hashlib import blake2b
from pathlib import Path
from typing import Sequence, Tuple, Any

from .base import Cache
from .pickler import dumps, PREVIOUS_VERSIONS, LATEST_VERSION
from .transactions import DummyTransaction
from ..storage.base import DiskOptions, Storage, digest_to_relative
from ..storage.local import copy_group_permissions, create_folders, DIGEST_SIZE
from ..engine import NodeHash
from ..serializers import Serializer

logger = logging.getLogger(__name__)

DATA_FOLDER = 'data'
HASH_FILENAME = 'hash.bin'
META_FILENAME = 'meta.json'
GZIP_COMPRESSION = 1


class DiskCache(Cache):
    def __init__(self, root: Path, options: Sequence[DiskOptions], serializer: Serializer, metadata: dict):
        super().__init__()
        self.metadata = metadata
        self.serializer = serializer
        self.storage = Storage(options)
        self.root = Path(root)
        self._transactions = DummyTransaction()

    def reserve_write_or_read(self, param: NodeHash) -> Tuple[bool, Any]:
        value = param.value
        pickled, digest, _ = key_to_relative(value)
        empty, transaction = self._transactions.reserve_write_or_read(digest, self._digest_exists)
        # we can already read from cache
        if not empty:
            return empty, transaction

        # the cache is empty, but we can try an restore it from an old version
        for version in reversed(PREVIOUS_VERSIONS):
            local_pickled, local_digest, _ = key_to_relative(value, version)
            local_transaction = self._transactions.reserve_read(local_digest, self._digest_exists)
            if local_transaction is not None:
                # we can simply copy the previous version, because nothing really changed
                value = self._transactions.release_read(
                    local_digest, local_transaction, partial(self._load, pickled=local_pickled))
                self._transactions.release_write(digest, value, transaction, partial(self._save, pickled=pickled))
                empty, transaction = self._transactions.reserve_write_or_read(digest, self._digest_exists)
                assert not empty
                return empty, transaction

        return empty, transaction

    def fail(self, param: NodeHash, transaction: Any):
        _, digest, _ = key_to_relative(param.value)
        self._transactions.fail(digest, transaction)

    def set(self, param: NodeHash, value, transaction: Any):
        pickled, digest, _ = key_to_relative(param.value)
        return self._transactions.release_write(digest, value, transaction, partial(self._save, pickled=pickled))

    def get(self, param: NodeHash, transaction: Any):
        pickled, digest, _ = key_to_relative(param.value)
        return self._transactions.release_read(digest, transaction, partial(self._load, pickled=pickled))

    def _digest_exists(self, digest: str):
        return (self.root / digest_to_relative(digest)).exists()

    def _load(self, digest, pickled):
        root = self.root / digest_to_relative(digest)
        # TODO: how slow is this?
        check_consistency(root / HASH_FILENAME, pickled)
        return self.serializer.load(root / DATA_FOLDER)

    def _save(self, digest: str, value, pickled):
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

            path = self.storage.get(self.storage.store(file))
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
