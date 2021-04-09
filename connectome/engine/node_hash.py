from typing import Sequence, Callable

NODE_TYPES = set()


class NodeHash:
    type: int

    def __init__(self, value):
        assert self.type is not None
        assert value[0] == self.type
        self.value = value

    def __eq__(self, other):
        return isinstance(other, NodeHash) and self.value == other.value

    def __init_subclass__(cls, **kwargs):
        if cls.type is not None:
            assert cls.type not in NODE_TYPES, (cls, cls.type, NODE_TYPES)
            NODE_TYPES.add(cls.type)


NodeHashes = Sequence[NodeHash]


class PrecomputeHash(NodeHash):
    type = None

    def __init__(self, value, hash_):
        super().__init__(value)
        self._hash = hash_

    def __hash__(self):
        return self._hash


class CompoundBase(PrecomputeHash):
    type = None

    def __init__(self, *children: NodeHash):
        super().__init__(
            (self.type, *(h.value for h in children)),
            hash((self.type, *children)),
        )


class LeafHash(PrecomputeHash):
    type = 0

    def __init__(self, data):
        value = self.type, data
        super().__init__(value, hash(value))
        self.data = data


class ApplyHash(PrecomputeHash):
    type = 1

    def __init__(self, func: Callable, *args: NodeHash):
        super().__init__(
            (self.type, func, tuple(h.value for h in args)),
            hash((self.type, func, args)),
        )


class GraphHash(CompoundBase):
    type = 2

    def __init__(self, output: NodeHash):
        super().__init__(output)


class TupleHash(CompoundBase):
    type = 3


# Higher order functions

class FilterHash(CompoundBase):
    type = 10

    def __init__(self, func: GraphHash, values: NodeHash):
        super().__init__(func, values)


class MergeHash(CompoundBase):
    type = 11


class GroupByHash(CompoundBase):
    type = 12

    def __init__(self, key: GraphHash, values: NodeHash):
        super().__init__(key, values)


class DictFromKeys(CompoundBase):
    type = 13

    def __init__(self, func: GraphHash, key: NodeHash, mapping: NodeHash):
        super().__init__(func, key, mapping)


# Experimental stuff

class MultiMappingHash(CompoundBase):
    type = -1
