import errno
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

        # self.counter = Cache(str(root), size_limit=10 * 2 ** 30, cull_limit=0, eviction_policy='none')
        # for file in root.glob(f'{DBNAME}*'):
        #     os.chmod(file, PERMISSIONS)
        #     shutil.chown(file, group=root.group())

    def volume(self) -> int:
        return 0
        # return sum(self.counter[key] for key in self.counter)

    def _key_to_path(self, key):
        return self.root / digest_to_relative(key) / FILENAME

    def __contains__(self, key):
        # TODO: use the counter?
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
            copy_file(path, stored_file)
            copy_group_permissions(folder, self.root, recursive=True)

        except BaseException as e:
            del self[key]
            raise RuntimeError('An error occurred while creating the cache. Cleaned up.') from e

        # make file read-only
        os.chmod(stored_file, 0o444 & PERMISSIONS)
        # calculate the volume
        # self.counter[key] = os.path.getsize(path)

    def __delitem__(self, key):
        file = self._key_to_path(key)
        os.chmod(file, PERMISSIONS)
        shutil.rmtree(file.parent)
        # del self.counter[key]

    # def __del__(self):
    #     self.counter.close()


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


def digest_file(path: Path, block_size=2 ** 20):
    hasher = blake2b(digest_size=DIGEST_SIZE)

    with open(path, 'rb') as f:
        while True:
            buffer = f.read(block_size)
            if not buffer:
                break
            hasher.update(buffer)

    return hasher.hexdigest()


def digest_to_relative(key):
    assert len(key) == DIGEST_SIZE * 2, len(key)

    parts = []
    start = 0
    for level in FOLDER_LEVELS:
        stop = start + level * 2
        parts.append(key[start:stop])
        start = stop

    return Path(*parts)


def copy_file(source, destination):
    # in Python>=3.8 the sendfile call is used, which apparently may fail
    try:
        shutil.copyfile(source, destination)
        return
    except OSError as e:
        # BlockingIOError -> fallback to slow copy
        if e.errno != errno.EWOULDBLOCK:
            raise

    with open(source, 'rb') as src, open(destination, 'wb') as dst:
        shutil.copyfileobj(src, dst)
