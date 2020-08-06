import tempfile
from collections import OrderedDict
import os
import shutil
from hashlib import blake2b
from pathlib import Path
from typing import Sequence, Union

from .relative_remote import RelativeRemote, RemoteOptions
from .utils import ChainDict

LEVEL_SIZE, FOLDER_LEVELS = 32, 2
PERMISSIONS = 0o770
FILENAME = 'data'


class DiskOptions:
    def __init__(self, path: Union[Path, str], min_free_space: int = 0, max_volume: int = float('inf')):
        self.path = Path(path)
        self.max_volume = max_volume
        self.min_free_space = min_free_space


class Storage:
    def __init__(self, options: Sequence[DiskOptions]):
        self.options = OrderedDict()
        for entry in options:
            cache = GroupCache(entry.path)
            self.options[cache] = entry

        self.local = ChainDict(list(self.options), self._select_storage)

    def store(self, path: Path):
        key = _digest_file(path)
        if key in self.local:
            assert match_files(path, self.local[key])
        else:
            self.local[key] = path
        return key, self.local[key]

    def get(self, key: str, name: str = None):
        path: Path = self.local[key]
        if name is None:
            return path

        link = path.parent / name
        if not link.exists():
            link.symlink_to(path.name)

        return link

    def _select_storage(self, cache: 'GroupCache'):
        options = self.options[cache]
        matches = True

        if options.min_free_space > 0:
            free_space = shutil.disk_usage(cache.root).free
            matches = matches and free_space >= options.min_free_space

        if options.max_volume < float('inf'):
            volume = get_folder_size(cache.root)
            matches = matches and volume <= options.max_volume

        return matches


class BackupStorage(Storage):
    def __init__(self, local: Sequence[DiskOptions], remote: Sequence[RemoteOptions]):
        super().__init__(local)
        self.remotes = [RelativeRemote(**options._asdict()) for options in remote]

    def get(self, key: str, name: str = None):
        if key not in self.local:
            self._download(key)
        return super().get(key, name)

    # called only if the file is not present
    def _download(self, key):
        relative = digest_to_relative(key) / FILENAME
        with tempfile.TemporaryDirectory() as temp_dir:
            file = Path(temp_dir) / relative.name
            for remote in self.remotes:
                with remote:
                    try:
                        remote.get(relative, file)
                    except FileNotFoundError:
                        continue

                    self.local[key] = file
                    return

        raise KeyError(key)


def copy_group_permissions(target, reference, recursive=False):
    shutil.chown(target, group=reference.group())
    os.chmod(target, PERMISSIONS)
    if recursive and target.is_dir():
        for child in target.iterdir():
            copy_group_permissions(child, reference, recursive)


def adjust_parents(target, root):
    if target == root:
        return

    adjust_parents(target.parent, root)
    copy_group_permissions(target, root)


def create_folders(path: Path, root: Path):
    if path != root:
        create_folders(path.parent, root)

    if not path.exists():
        path.mkdir(mode=PERMISSIONS)
        os.chmod(path, PERMISSIONS)


def get_file_size(path):
    return os.path.getsize(path)


def get_folder_size(path):
    size = 0
    for root, _, files in os.walk(path):
        for name in files:
            size += get_file_size(os.path.join(root, name))

    return size


def _digest_file(path: Path, block_size=2 ** 20):
    hasher = blake2b(digest_size=FOLDER_LEVELS * LEVEL_SIZE)

    with open(path, 'rb') as f:
        while True:
            buffer = f.read(block_size)
            if not buffer:
                break
            hasher.update(buffer)

    return hasher.hexdigest()


def digest_to_relative(key):
    assert len(key) % FOLDER_LEVELS == 0
    size = len(key) // FOLDER_LEVELS

    parts = []
    for i in range(FOLDER_LEVELS):
        i *= size
        parts.append(key[i:i + size])

    return Path(*parts)


def match_files(first: Path, second: Path):
    # TODO
    return True


# TODO: keep track of volume?
class GroupCache:
    def __init__(self, root: Path):
        self.root = root
        if not root.exists():
            create_folders(root, root)

    def _key_to_path(self, key):
        return self.root / digest_to_relative(key) / FILENAME

    def __contains__(self, key):
        return self._key_to_path(key).exists()

    def __getitem__(self, key):
        return self._key_to_path(key)

    def __setitem__(self, key, path):
        path = Path(path)
        assert path.is_file(), path

        file = self._key_to_path(key)
        folder = file.parent
        create_folders(folder, self.root)

        try:
            shutil.copyfile(path, file)
            copy_group_permissions(folder, self.root, recursive=True)
            adjust_parents(folder, self.root)

        except BaseException as e:
            del self[key]
            raise RuntimeError('An error occurred while creating the cache. Cleaned up.') from e

    def __delitem__(self, key):
        shutil.rmtree(self._key_to_path(key).parent)