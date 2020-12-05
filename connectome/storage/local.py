import os
import shutil
from hashlib import blake2b
from pathlib import Path

FOLDER_LEVELS = 1, 31, 32
DIGEST_SIZE = sum(FOLDER_LEVELS)
PERMISSIONS = 0o770
FILENAME = 'data'


class StorageLocation:
    def __init__(self, root: Path):
        self.root = root
        if not root.exists():
            create_folders(root, root)

    def volume(self):
        return get_folder_size(self.root)

    def _key_to_path(self, key):
        return self.root / digest_to_relative(key) / FILENAME

    def __contains__(self, key):
        return self._key_to_path(key).exists()

    def __getitem__(self, key):
        path = self._key_to_path(key)
        if not path.exists():
            raise KeyError
        return path

    def __setitem__(self, key, path):
        path = Path(path)
        assert path.is_file(), path

        stored_file = self._key_to_path(key)
        folder = stored_file.parent
        create_folders(folder, self.root)

        try:
            shutil.copyfile(path, stored_file)
            copy_group_permissions(folder, self.root, recursive=True)

        except BaseException as e:
            del self[key]
            raise RuntimeError('An error occurred while creating the cache. Cleaned up.') from e

        # make file read-only
        os.chmod(stored_file, 0o444 & PERMISSIONS)

    def __delitem__(self, key):
        file = self._key_to_path(key)
        os.chmod(file, PERMISSIONS)
        shutil.rmtree(file.parent)


def copy_group_permissions(target, reference, recursive=False):
    shutil.chown(target, group=reference.group())
    os.chmod(target, PERMISSIONS)
    if recursive and target.is_dir():
        for child in target.iterdir():
            copy_group_permissions(child, reference, recursive)


# FIXME: this became a mess
def create_folders(path: Path, root: Path):
    if path != root:
        create_folders(path.parent, root)

    if not path.exists():
        path.mkdir(mode=PERMISSIONS)
        os.chmod(path, PERMISSIONS)
        if path != root:
            shutil.chown(path, group=root.group())


def get_file_size(path):
    return os.path.getsize(path)


# FIXME: this function is VERY slow
def get_folder_size(path):
    size = 0
    for root, _, files in os.walk(path):
        for name in files:
            size += get_file_size(os.path.join(root, name))

    return size


def _digest_file(path: Path, block_size=2 ** 20):
    hasher = blake2b(digest_size=DIGEST_SIZE)

    with open(path, 'rb') as f:
        while True:
            buffer = f.read(block_size)
            if not buffer:
                break
            hasher.update(buffer)

    return hasher.hexdigest()


def digest_to_relative(key):
    assert len(key) == DIGEST_SIZE * 2

    parts = []
    start = 0
    for level in FOLDER_LEVELS:
        stop = start + level * 2
        parts.append(key[start:stop])
        start = stop

    return Path(*parts)
