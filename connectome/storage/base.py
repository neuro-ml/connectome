from pathlib import Path
from typing import Union


# TODO: deprecated?
class DiskOptions:
    def __init__(self, path: Union[Path, str], min_free_space: int = 0, max_volume: int = None):
        if max_volume is None:
            max_volume = float('inf')
        self.path = Path(path)
        self.max_volume = max_volume
        self.min_free_space = min_free_space
