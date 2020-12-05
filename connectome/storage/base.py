from collections import OrderedDict

import filecmp
import tempfile
import shutil
from pathlib import Path
from typing import Sequence, Union
from tqdm import tqdm

from .local import StorageLocation, _digest_file, digest_to_relative, FILENAME
from .remote import RelativeRemote, RemoteOptions
from ..utils import ChainDict, InsertError


class DiskOptions:
    def __init__(self, path: Union[Path, str], min_free_space: int = 0, max_volume: int = None):
        if max_volume is None:
            max_volume = float('inf')
        self.path = Path(path)
        self.max_volume = max_volume
        self.min_free_space = min_free_space


class Storage:
    def __init__(self, options: Sequence[DiskOptions]):
        self.options = OrderedDict()
        for entry in options:
            cache = StorageLocation(entry.path)
            self.options[cache] = entry

        self.local = ChainDict(list(self.options), self._select_storage)

    def store(self, path: Path) -> str:
        key = _digest_file(path)
        if key in self.local:
            assert match_files(path, self.local[key]), (path, self.local[key])
        else:
            try:
                self.local[key] = path
            except InsertError:
                raise InsertError('No appropriate storage was found.') from None
        return key

    def get(self, key: str, name: str = None) -> Path:
        path: Path = self.local[key]
        if name is None:
            return path

        link = path.parent / name
        if not link.exists():
            link.symlink_to(path.name)

        return link

    def _select_storage(self, cache: 'StorageLocation'):
        options = self.options[cache]
        matches = True

        if options.min_free_space > 0:
            free_space = shutil.disk_usage(cache.root).free
            matches = matches and free_space >= options.min_free_space

        if options.max_volume < float('inf'):
            volume = cache.volume()
            matches = matches and volume <= options.max_volume

        return matches


class BackupStorage(Storage):
    def __init__(self, local: Sequence[DiskOptions], remote: Sequence[RemoteOptions]):
        super().__init__(local)
        self.remotes = [RelativeRemote(**options._asdict()) for options in remote]

    def get(self, key: str, name: str = None):
        self.fetch(key)
        return super().get(key, name)

    def fetch(self, *keys: str, verbose: bool = False):
        bar = tqdm(disable=not verbose, total=len(keys))
        missing = set()
        for key in keys:
            if key in self.local:
                bar.update()
            else:
                missing.add(key)

        # extract as much as we can from each remote
        for remote in self.remotes:
            if not missing:
                break

            with remote:
                for key in list(missing):
                    relative = digest_to_relative(key) / FILENAME
                    with tempfile.TemporaryDirectory() as temp_dir:
                        file = Path(temp_dir) / relative.name
                        try:
                            remote.get(relative, file)
                        except FileNotFoundError:
                            continue

                        self.local[key] = file
                        missing.remove(key)
                        bar.update()

        if missing:
            raise FileNotFoundError(f'Could not fetch {len(missing)} keys from remote.')


def match_files(first: Path, second: Path):
    return filecmp.cmp(first, second, shallow=False)
