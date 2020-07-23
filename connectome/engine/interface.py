from typing import Sequence, Tuple

from .base import Edge, TreeNode, NodeHash, NodesMask, FULL_MASK
from .edges import FunctionEdge


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
        return NodeHash.from_leaf(self.value), FULL_MASK


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
