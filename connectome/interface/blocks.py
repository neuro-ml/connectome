from pathlib import Path
from typing import Union, Sequence

from .base import FromLayer, CallableBlock
from ..layers.cache import MemoryCacheLayer, DiskCacheLayer
from ..layers.merge import SwitchLayer
from ..layers.shortcuts import ApplyLayer

PathLike = Union[Path, str]


class Merge(CallableBlock):
    def __init__(self, *blocks: CallableBlock):
        super().__init__()

        # FIXME: hope this works :)
        idx_union = set.union(*[set(layer.ids()) for layer in blocks])
        if sum(len(layer.ids()) for layer in blocks) != len(idx_union):
            raise RuntimeError('Datasets have same indices')

        def branch_selector(identifier):
            for idx, ds in enumerate(blocks):
                if identifier in ds.ids():
                    return idx

            raise ValueError(identifier)

        self._layer = SwitchLayer(branch_selector, *(s._layer for s in blocks))


class Apply(FromLayer):
    def __init__(self, **transform):
        super().__init__(ApplyLayer(transform))


class CacheToRam(FromLayer):
    def __init__(self, names: Sequence[str] = None, size: int = None):
        super().__init__(MemoryCacheLayer(names, size))


class CacheToDisk(FromLayer):
    def __init__(self, storage: PathLike, names: Sequence[str] = None):
        super().__init__(DiskCacheLayer(names, storage))
