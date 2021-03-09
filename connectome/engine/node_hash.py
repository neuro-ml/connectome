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
    __slots__ = 'kind', 'data', 'value', 'children', '_hash'

    def __init__(self, data, children, kind: HashType):
        self.children: NodeHashes = children
        self.kind = kind
        self.data = data
        self.value = kind.value, data
        # TODO: reuse children?
        self._hash = hash(self.value)

    # def __init__(self, value, _hash):
    #     self.value = value
    #     self._hash = _hash

    # TODO: at this point it looks like 2 different objects
    @classmethod
    def from_leaf(cls, data):
        assert not isinstance(data, NodeHash)
        return cls(data, (), kind=HashType.LEAF)

    @classmethod
    def from_hash_nodes(cls, *hashes: 'NodeHash', kind=HashType.COMPOUND):
        data = tuple(h.value for h in hashes)
        return cls(data, hashes, kind=kind)

    def __hash__(self):
        return self._hash

    def __repr__(self):
        if self.kind == HashType.LEAF:
            # FIXME
            from connectome.engine.edges import Nothing

            if self.data is Nothing:
                name = 'Nothing'
            else:
                name = f'Leaf'
        else:
            name = f'Compound'

        return f'<NodeHash: {name}>'

    def __eq__(self, other):
        return isinstance(other, NodeHash) and self.value == other.value


NodeHashes = Sequence[NodeHash]

# class LeafHash(NodeHash):
#     def __init__(self, value):
#         value = HashType.LEAF.value, value
#         super().__init__(value, hash(value))
#
#
# class CompoundHash(NodeHash):
#     def __init__(self, *children: NodeHash, kind: int):
#         super().__init__(
#             (kind, *(h.value for h in children)),
#             hash((kind, *children)),
#         )
#         self.children = children
