import getpass
import gzip
import json
import logging
import os
import shutil
import time
from hashlib import blake2b
from pathlib import Path
from typing import Sequence

from .base import Cache
from .locks import NoLock
from .pickler import dumps, PREVIOUS_VERSIONS, LATEST_VERSION
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
        self._file_lock = NoLock()

    def contains(self, param: NodeHash) -> bool:
        _, _, relative = key_to_relative(param.value)
        if (self.root / relative).exists():
            return True

        for version in reversed(PREVIOUS_VERSIONS):
            pickled, _, relative = key_to_relative(param.value, version)
            local = self.root / relative
            if local.exists():
                # we can simply copy the previous version, because nothing really changed
                self.set(param, self._load(local, pickled))
                return True

        return False

    def set(self, param: NodeHash, value):
        pickled, _, relative = key_to_relative(param.value)
        root = self.root / relative
        if root.exists():
            # idempotency
            with self._file_lock.read(root):
                check_consistency(root / HASH_FILENAME, pickled)
                return

        with self._file_lock.write(root):
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

    def get(self, param: NodeHash):
        pickled, _, relative = key_to_relative(param.value)
        return self._load(self.root / relative, pickled)

    def _load(self, root, pickled):
        with self._file_lock.read(root):
            # TODO: how slow is this?
            check_consistency(root / HASH_FILENAME, pickled)
            return self.serializer.load(root / DATA_FOLDER)

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
    def _check(cls):
        with cls(hash_path, 'rb') as file:
            dumped = file.read()
            if dumped != pickled:
                raise RuntimeError(f'The dumped and current pickle do not match at {hash_path}: {dumped} {pickled}')

    try:
        _check(gzip.GzipFile)

    except OSError:
        # transition from old non-gzipped hashes
        _check(open)


def save_hash(hash_path, pickled):
    with gzip.GzipFile(hash_path, 'wb', compresslevel=GZIP_COMPRESSION, mtime=0) as file:
        file.write(pickled)
