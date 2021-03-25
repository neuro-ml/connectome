from enum import unique, IntEnum
from typing import Sequence


@unique
class HashType(IntEnum):
    LEAF = 0
    COMPOUND = 1
    FILTER = 2

    # experimental
    MAPPING = 3
    GROUPING = 4
    MERGE = 5
    MULTI_MAPPING = 6


class NodeHash:
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return isinstance(other, NodeHash) and self.value == other.value


NodeHashes = Sequence[NodeHash]


class PrecomputeHash(NodeHash):
    def __init__(self, value, hash_):
        super().__init__(value)
        self._hash = hash_

    def __hash__(self):
        return self._hash


class LeafHash(PrecomputeHash):
    def __init__(self, data):
        value = (HashType.LEAF.value, data)
        super().__init__(value, hash(value))
        self.data = data


class CompoundHash(PrecomputeHash):
    def __init__(self, *children: NodeHash, kind: int = HashType.COMPOUND.value):
        if isinstance(kind, HashType):
            kind = kind.value

        # TODO: during hash migration unpack these tuples
        super().__init__(
            (kind, tuple(h.value for h in children)),
            hash((kind, *children)),
        )
        self.children = children
