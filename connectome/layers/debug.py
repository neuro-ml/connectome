from typing import Iterable, Sequence, Any

from .transform import TransformLayer
from ..cache.disk import key_to_digest
from ..engine.base import Node, NodeHash
from ..engine.edges import IdentityEdge


class HashDigestEdge(IdentityEdge):
    def __init__(self):
        super().__init__()
        self._uses_hash = True

    def _evaluate(self, inputs: Sequence[Any], output: NodeHash, payload: Any) -> Any:
        pickled, digest = key_to_digest(output.value)
        return inputs[0], output.value, digest, pickled


class HashDigestLayer(TransformLayer):
    def __init__(self, names: Iterable[str]):
        inputs, outputs, edges = [], [], []
        for name in names:
            inp, out = Node(name), Node(name)
            inputs.append(inp)
            outputs.append(out)
            edges.append(HashDigestEdge().bind(inp, out))

        super().__init__(inputs, outputs, edges, virtual_nodes=True)
