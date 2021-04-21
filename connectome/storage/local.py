import os
import shutil
from pathlib import Path

FOLDER_LEVELS = 1, 31, 32
DIGEST_SIZE = sum(FOLDER_LEVELS)
PERMISSIONS = 0o770
FILENAME = 'data'


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
