import getpass
import json
import os
import shutil
import time
from hashlib import blake2b
from pathlib import Path
from threading import RLock
from typing import Sequence

from .base import Cache
from .pickler import dumps, PREVIOUS_VERSIONS, LATEST_VERSION

from ..storage.base import DiskOptions, Storage, digest_to_relative
from ..storage.local import FOLDER_LEVELS, LEVEL_SIZE, copy_group_permissions, create_folders
from ..engine import NodeHash
from ..serializers import Serializer
from ..utils import atomize

DATA_FOLDER = 'data'
HASH_FILENAME = 'hash.bin'
META_FILENAME = 'meta.json'


class DiskCache(Cache):
    def __init__(self, root: Path, options: Sequence[DiskOptions], serializer: Serializer, metadata: dict):
        super().__init__()
        self._lock = RLock()
        self.metadata = metadata
        self.serializer = serializer
        self.storage = Storage(options)
        self.root = Path(root)

    @atomize()
    def contains(self, param: NodeHash) -> bool:
        _, _, relative = key_to_relative(param.value)
        if (self.root / relative).exists():
            return True

        for version in reversed(PREVIOUS_VERSIONS):
            _, _, relative = key_to_relative(param.value, version)
            local = self.root / relative
            if local.exists():
                # we can simply copy the previous version, because nothing really changed
                self.set(param, self.serializer.load(local / DATA_FOLDER))
                return True

        return False

    @atomize()
    def set(self, param: NodeHash, value):
        pickled, digest, relative = key_to_relative(param.value)
        local = self.root / relative
        data_folder = local / DATA_FOLDER
        create_folders(data_folder, self.root)

        try:
            # data
            self.serializer.save(value, data_folder)
            # meta
            self._save_meta(local, pickled)

            copy_group_permissions(local, self.root, recursive=True)
            self._mirror_to_storage(data_folder)

        except BaseException as e:
            shutil.rmtree(local)
            raise RuntimeError('An error occurred while creating the cache. Cleaned up.') from e

    @atomize()
    def get(self, param: NodeHash):
        pickled, _, relative = key_to_relative(param.value)
        # TODO: how slow is this?
        check_consistency(self.root / relative / HASH_FILENAME, pickled)
        return self.serializer.load(self.root / relative / DATA_FOLDER)

    def _save_meta(self, local, pickled):
        # hash
        hash_path = local / HASH_FILENAME
        if hash_path.exists():
            check_consistency(hash_path, pickled)
        else:
            # or save
            with open(hash_path, 'wb') as file:
                file.write(pickled)

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
    return blake2b(pickled, digest_size=FOLDER_LEVELS * LEVEL_SIZE).hexdigest()


def key_to_relative(key, version=LATEST_VERSION):
    pickled = dumps(key, version=version)
    digest = digest_bytes(pickled)
    relative = digest_to_relative(digest)
    return pickled, digest, relative


def check_consistency(hash_path, pickled):
    with open(hash_path, 'rb') as file:
        dumped = file.read()
        assert dumped == pickled, (dumped, pickled)
