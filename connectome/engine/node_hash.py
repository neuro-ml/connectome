import warnings
from typing import Sequence, Callable, Tuple, Any

NODE_TYPES = set()


class NodeHash:
    type: int

    def __init__(self, value, hash_target):
        assert self.type is not None
        assert value[0] == self.type
        self.value = value
        self._hash_target = hash_target
        self._hash = None

    def __hash__(self):
        if self._hash is None:
            self._hash = hash(self._hash_target)
        return self._hash

    def __eq__(self, other):
        return isinstance(other, NodeHash) and self.value == other.value

    def __init_subclass__(cls, **kwargs):
        if cls.type is not None:
            assert cls.type not in NODE_TYPES, (cls, cls.type, NODE_TYPES)
            NODE_TYPES.add(cls.type)


NodeHashes = Sequence[NodeHash]


class LeafHash(NodeHash):
    """ A hash for a single object, without any dependencies """
    type = 0

    def __init__(self, data):
        value = self.type, data
        super().__init__(value, value)
        self.data = data


class ApplyHash(NodeHash):
    """ A hash for a function call """
    type = 1

    def __init__(self, func: Callable, *args: NodeHash, kw_names: Tuple[str, ...] = ()):
        kw_names = tuple(kw_names)
        super().__init__(
            (self.type, func, tuple(h.value for h in args), kw_names),
            (self.type, func, args, kw_names),
        )


class GraphHash(NodeHash):
    """ Denotes a hash of a static computational graph """
    type = 2

    def __init__(self, output: NodeHash):
        super().__init__(
            (self.type, output.value),
            (self.type, output),
        )


class CustomHash(NodeHash):
    """ A special hash for various custom behaviors """
    type = 3

    def __init__(self, marker: Any, *children: NodeHash):
        super().__init__(
            (self.type, marker, *(h.value for h in children)),
            (self.type, marker, *children),
        )


# TODO: deprecated
class CompoundBase(NodeHash):
    type = None

    def __init__(self, *children: NodeHash):
        warnings.warn('This interface is deprecated', DeprecationWarning)
        warnings.warn('This interface is deprecated', UserWarning)
        super().__init__(
            (self.type, *(h.value for h in children)),
            (self.type, *children),
        )


class TupleHash(ApplyHash):
    type = -1

    def __init__(self, *children: NodeHash):
        warnings.warn('This interface is deprecated. Use ApplyHash instead', DeprecationWarning)
        warnings.warn('This interface is deprecated. Use ApplyHash instead', UserWarning)
        super().__init__(tuple, *children)


class FilterHash(ApplyHash):
    type = -2

    def __init__(self, graph: GraphHash, values: NodeHash):
        warnings.warn('This interface is deprecated. Use ApplyHash instead', DeprecationWarning)
        warnings.warn('This interface is deprecated. Use ApplyHash instead', UserWarning)
        super().__init__(filter, graph, values)


class MergeHash(CustomHash):
    type = -3

    def __init__(self, *children: NodeHash):
        warnings.warn('This interface is deprecated. Use CustomHash instead', DeprecationWarning)
        warnings.warn('This interface is deprecated. Use CustomHash instead', UserWarning)
        super().__init__('connectome.SwitchEdge', *children)


PrecomputeHash = NodeHash
