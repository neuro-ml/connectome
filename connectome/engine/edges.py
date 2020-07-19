from typing import Sequence, Tuple, Callable

from connectome.cache import CacheStorage
from connectome.engine import NodeHash, TreeNode, Edge, NodesMask


class FunctionEdge(Edge):
    def __init__(self, function: Callable, arity: int):
        super().__init__(arity)
        self.function = function

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return self.function(*arguments)

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        return NodeHash.from_hash_nodes([NodeHash(data=self.function)] + list(hashes), prev_edge=self), None


class IdentityEdge(Edge):
    def __init__(self):
        super().__init__(1)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return arguments[0]

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        return hashes[0], None


# TODO: everything below is old

class CacheEdge(Edge):
    def __init__(self, storage: CacheStorage):
        super().__init__(1)
        self.storage = storage

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        assert len(hashes) == 1
        if self.storage.contains(hashes[0]):
            inputs = []
        else:
            inputs = None

        return hashes[0], inputs

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        # no arguments means that the value is cached
        if not arguments:
            return self.storage.get(node_hash)

        assert len(arguments) == 1
        value = arguments[0]
        self.storage.set(node_hash, value)
        return value


class ValueEdge(Edge):
    """
    Used in interface to provide constant parameters.
    """

    def __init__(self, target: TreeNode, value):
        super().__init__([], target)
        self.value = value

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[TreeNode], parameter: NodeHash):
        return self.value

    def process_hashes(self, parameters: Sequence[NodeHash]):
        assert not parameters
        return self.inputs, NodeHash(data=self.value, prev_edge=self)


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
