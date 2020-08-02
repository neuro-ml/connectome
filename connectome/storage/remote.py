import tempfile
from pathlib import Path
from threading import RLock
from typing import Sequence

from .relative_remote import RemoteOptions, RelativeRemote
from ..engine.base import NodeHash
from ..serializers import Serializer
from ..storage.base import CacheStorage
from ..utils import atomize
from .disk import key_to_relative, check_consistency, DATA_FOLDER, HASH_FILENAME
from .utils import ChainDict


class RemoteStorage(CacheStorage):
    def __init__(self, options: Sequence[RemoteOptions], serializer: Serializer):
        super().__init__()
        self._lock = RLock()
        self.storage = ChainDict([RemoteDict(entry, serializer) for entry in options])

    @atomize()
    def contains(self, param: NodeHash) -> bool:
        return param.value in self.storage

    @atomize()
    def get(self, param: NodeHash):
        return self.storage[param.value]

    def set(self, param: NodeHash, value):
        pass


class RemoteDict:
    def __init__(self, options: RemoteOptions, serializer: Serializer):
        self.storage = RelativeRemote(**options._asdict())
        self.serializer = serializer

    def _load(self, func, relative: Path, *args, **kwargs):
        with self.storage, tempfile.TemporaryDirectory() as temp_dir:
            temp_file = Path(temp_dir) / relative.name
            self.storage.get(relative, temp_file)
            return func(temp_file, *args, **kwargs)

    def __contains__(self, key):
        pickled, _, relative = key_to_relative(key)
        try:
            self._load(check_consistency, relative / HASH_FILENAME, pickled)
        except FileNotFoundError:
            return False
        return True

    def __getitem__(self, key):
        _, _, relative = key_to_relative(key)
        try:
            return self._load(self.serializer.load, relative / DATA_FOLDER)
        except FileNotFoundError:
            raise KeyError from None
