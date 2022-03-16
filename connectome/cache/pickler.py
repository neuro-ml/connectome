# TODO: legacy support
from typing import Any, NamedTuple


class VersionedClass(NamedTuple):
    type: type
    version: Any
