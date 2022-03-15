from .base import Cache
from .disk import DiskCache
from .memory import MemoryCache
# to simplify usage
from stash.cache import is_stable, is_unstable, unstable_module
