from typing import Sequence, Callable, Tuple

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

    def __init__(self, value, hash_target):
        super().__init__(value)
        self._hash_target = hash_target
        self._hash = None

    def __hash__(self):
        if self._hash is None:
            self._hash = hash(self._hash_target)
        return self._hash


class CompoundBase(PrecomputeHash):
    type = None

    def __init__(self, *children: NodeHash):
        super().__init__(
            (self.type, *(h.value for h in children)),
            (self.type, *children),
        )


class LeafHash(PrecomputeHash):
    type = 0

    def __init__(self, data):
        value = self.type, data
        super().__init__(value, value)
        self.data = data


class ApplyHash(PrecomputeHash):
    type = 1

    def __init__(self, func: Callable, *args: NodeHash, kw_names: Tuple[str, ...]):
        # TODO: unify this during the next big migration
        values = tuple(h.value for h in args)
        kw_names = tuple(kw_names)
        if kw_names:
            seq = values, kw_names
        else:
            seq = values,

        super().__init__(
            (self.type, func, *seq),
            (self.type, func, args, kw_names),
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


class JoinMappingHash(PrecomputeHash):
    type = 14

    def __init__(self, left: GraphHash, right: GraphHash, left_keys: NodeHash, right_keys: NodeHash, id_maker):
        args = left, right, left_keys, right_keys
        super().__init__(
            (self.type, *tuple(h.value for h in args), id_maker),
            (self.type, *args, id_maker),
        )


# Experimental stuff

class MultiMappingHash(CompoundBase):
    type = -1
