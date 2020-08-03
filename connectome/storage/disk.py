import getpass
import json
import os
import shutil
import time
from hashlib import blake2b
from pathlib import Path
from threading import RLock
from typing import Sequence

from .local import DiskOptions, Storage, digest_to_relative, FOLDER_LEVELS, LEVEL_SIZE, copy_group_permissions, \
    create_folders
from ..engine.base import NodeHash
from ..serializers import Serializer
from ..utils import atomize
from .base import CacheStorage
from .pickler import dumps

DATA_FOLDER = 'data'
HASH_FILENAME = 'hash.bin'
META_FILENAME = 'meta.json'


class DiskStorage(CacheStorage):
    def __init__(self, root: Path, options: Sequence[DiskOptions], serializer: Serializer, metadata: dict):
        super().__init__()
        self._lock = RLock()
        self.metadata = metadata
        self.serializer = serializer
        self.storage = Storage(options)
        self.root = root

    @atomize()
    def contains(self, param: NodeHash) -> bool:
        _, _, relative = key_to_relative(param.value)
        local = self.root / relative
        return local.exists()

    @atomize()
    def set(self, param: NodeHash, value):
        local = self._key_to_path(param.value)
        data_folder = local / DATA_FOLDER

        try:
            # data
            self.serializer.save(value, data_folder)
            # meta
            meta = self.metadata.copy()
            meta.update({
                'time': time.time(),
                # TODO: this can possibly fail
                'user': getpass.getuser(),
            })
            with open(local / META_FILENAME, 'w') as file:
                json.dump(meta, file)

            copy_group_permissions(local, self.root, recursive=True)
            self._mirror_to_storage(data_folder)

        except BaseException as e:
            shutil.rmtree(local)
            raise RuntimeError('An error occurred while creating the cache. Cleaned up.') from e

    @atomize()
    def get(self, param: NodeHash):
        # TODO: check consistency?
        _, _, relative = key_to_relative(param.value)
        data_folder = self.root / relative / DATA_FOLDER
        return self.serializer.load(data_folder)

    def _key_to_path(self, key):
        pickled, digest, relative = key_to_relative(key)
        local = self.root / relative
        hash_path = local / HASH_FILENAME
        data_folder = local / DATA_FOLDER

        create_folders(data_folder, self.root)
        if hash_path.exists():
            check_consistency(hash_path, pickled)

        else:
            # or save
            with open(hash_path, 'wb') as file:
                file.write(pickled)

        return local

    def _mirror_to_storage(self, folder: Path):
        for file in folder.glob('**/*'):
            if file.is_dir():
                continue

            key, path = self.storage.store(file)
            assert path.exists()
            os.remove(file)
            file.symlink_to(path)


def digest_bytes(pickled: bytes) -> str:
    return blake2b(pickled, digest_size=FOLDER_LEVELS * LEVEL_SIZE).hexdigest()


def key_to_relative(key):
    pickled = dumps(key)
    digest = digest_bytes(pickled)
    relative = digest_to_relative(digest)
    return pickled, digest, relative


def check_consistency(hash_path, pickled):
    with open(hash_path, 'rb') as file:
        dumped = file.read()
        assert dumped == pickled, (dumped, pickled)
