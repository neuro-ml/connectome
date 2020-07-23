from pathlib import Path
from typing import Union

from .base import FromLayer
from ..layers import MemoryCacheLayer, Sequence, DiskCacheLayer

PathLike = Union[Path, str]


class CacheToRam(FromLayer):
    def __init__(self, names: Sequence[str] = None, size: int = None):
        super().__init__(MemoryCacheLayer(names, size))


class CacheToDisk(FromLayer):
    def __init__(self, storage: PathLike, names: Sequence[str] = None):
        super().__init__(DiskCacheLayer(names, storage))
