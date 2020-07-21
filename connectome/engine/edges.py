from typing import Sequence, Tuple, Callable

from connectome.cache import CacheStorage
from connectome.engine import NodeHash, TreeNode, Edge, NodesMask, FULL_MASK


# TODO: maybe the engine itself should deal with these
class Nothing:
    """
    A unity-like which is propagated through functional edges.
    """

    # TODO: singleton
    def __init__(self):
        raise RuntimeError("Don't init me!")

    @staticmethod
    def in_data(data):
        return any(x is Nothing for x in data)

    @staticmethod
    def in_hashes(hashes: Sequence[NodeHash]):
        return any(x.data is Nothing for x in hashes)


class FunctionEdge(Edge):
    def __init__(self, function: Callable, arity: int):
        super().__init__(arity)
        self.function = function

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        if Nothing.in_data(arguments):
            return Nothing

        return self.function(*arguments)

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        if Nothing.in_hashes(hashes):
            return NodeHash(data=Nothing, prev_edge=self), FULL_MASK

        return NodeHash.from_hash_nodes([NodeHash(data=self.function)] + list(hashes), prev_edge=self), FULL_MASK


class IdentityEdge(Edge):
    def __init__(self):
        super().__init__(arity=1)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return arguments[0]

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        return hashes[0], FULL_MASK


class CacheEdge(Edge):
    def __init__(self, storage: CacheStorage):
        super().__init__(arity=1)
        self.storage = storage

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        node_hash, = hashes
        if self.storage.contains(node_hash):
            mask = []
        else:
            mask = FULL_MASK

        return node_hash, mask

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        # no arguments means that the value is cached
        if not arguments:
            return self.storage.get(node_hash)

        value, = arguments
        # TODO: need a subclass for edges that interact with Nothing
        if value is Nothing:
            return value

        self.storage.set(node_hash, value)
        return value


class ProductEdge(Edge):
    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return arguments

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        return NodeHash.from_hash_nodes(hashes, prev_edge=self), FULL_MASK


# TODO: are Switch and Projection the only edges that need Nothing?
# TODO: does Nothing live only in hashes?
class SwitchEdge(Edge):
    def __init__(self, selector: Callable):
        super().__init__(arity=1)
        self.selector = selector

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        if node_hash.data is Nothing:
            return Nothing

        return arguments[0]

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        node_hash, = hashes
        if not self.selector(node_hash.data):
            # TODO: need a special type for hash of nothing
            node_hash = NodeHash(data=Nothing, prev_edge=self)
        return node_hash, FULL_MASK


class ProjectionEdge(Edge):
    def __init__(self):
        super().__init__(arity=1)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        # take the only non-Nothing value
        real = []
        for v in arguments[0]:
            if v is not Nothing:
                real.append(v)

        assert len(real) == 1, real
        return real[0]

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        # take the only non-Nothing hash
        real = []
        for v in hashes[0].children:
            if v.data is not Nothing:
                real.append(v)

        assert len(real) == 1
        return real[0], FULL_MASK


# TODO: move the code below to interface

class ValueEdge(Edge):
    """
    Used in interface to provide constant parameters.
    """

    def __init__(self, value):
        super().__init__(arity=0)
        self.value = value

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[TreeNode], parameter: NodeHash):
        return self.value

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        return NodeHash(data=self.value, prev_edge=self), FULL_MASK


class InitEdge(FunctionEdge):
    """
    Used to hold the ``self`` object created after calling __init__.
    ``function`` is stored for hashing purposes.
    """

    def __init__(self, init, this, inputs: Sequence[TreeNode], output: TreeNode):
        super().__init__(init, inputs, output)
        self.this = this

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[TreeNode], parameter: NodeHash):
        return self.this


class ItemGetterEdge(Edge):
    """
    Used in conjunction with `SelfEdge` to provide constant parameters.
    """

    def __init__(self, name: str, incoming: TreeNode, output: TreeNode):
        super().__init__([incoming], output)
        self.name = name

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[TreeNode], parameter: NodeHash):
        return arguments[0][self.name]

    def process_hashes(self, parameters: Sequence[NodeHash]):
        return NodeHash.from_hash_nodes([NodeHash(data=self.name), *parameters], prev_edge=self)


class MuxEdge(Edge):
    def __init__(self, branch_selector: Callable, inputs: Sequence[TreeNode], output: TreeNode):
        super().__init__(inputs, output)
        self.branch_selector = branch_selector

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[TreeNode], parameter):
        return arguments[0]

    def process_hashes(self, parameters: Sequence[NodeHash]):
        branch_codes = self.find_node_by_name(parameters)

        assert len(set(branch_codes.values())) == 1
        branch_code = list(branch_codes.values())[0]
        return self.branch_selector(branch_code, self.inputs, parameters)

    @staticmethod
    def find_node_by_name(parameters: Sequence[NodeHash], target_name='id'):
        result = {}

        # TODO generalize it somehow
        def find_name_rec(params: Sequence[NodeHash]):
            for param in params:
                if param.prev_edge is not None:
                    for i in param.prev_edge.inputs:
                        if i.name == target_name:
                            assert isinstance(param.prev_edge, FunctionEdge)
                            result[param] = param.data[1]

                if param.children is not None:
                    find_name_rec(param.children)

        find_name_rec(parameters)
        return result
