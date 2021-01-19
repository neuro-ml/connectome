from typing import Iterable, Sequence

from .transform import TransformLayer, INHERIT_ALL
from ..cache.disk import key_to_relative
from ..engine.base import Node, NodeHash, NodesMask
from ..engine.edges import IdentityEdge


class HashDigest(IdentityEdge):
    def __init__(self):
        super().__init__()
        self._uses_hash = True

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        pickled, digest, _ = key_to_relative(node_hash.value)
        return arguments[0], node_hash.value, digest, pickled


class HashDigestLayer(TransformLayer):
    def __init__(self, names: Iterable[str]):
        inputs, outputs, edges = [], [], []
        for name in names:
            inp, out = Node(name), Node(name)
            inputs.append(inp)
            outputs.append(out)
            edges.append(HashDigest().bind(inp, out))

        super().__init__(inputs, outputs, edges, inherit_nodes=INHERIT_ALL)
