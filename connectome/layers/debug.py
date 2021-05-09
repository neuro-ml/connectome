from typing import Iterable, Any, Generator

from .transform import TransformLayer
from ..cache.disk import key_to_digest
from ..engine.base import Node, Request, Response, Command
from ..engine.edges import StaticHash, StaticGraph
from ..engine.node_hash import NodeHashes, NodeHash


class HashDigestEdge(StaticGraph, StaticHash):
    def __init__(self):
        super().__init__(arity=1, uses_hash=True)

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]

    def evaluate(self) -> Generator[Request, Response, Any]:
        value = yield Command.ParentValue, 0
        output = yield Command.CurrentHash,

        pickled, digest = key_to_digest(output.value)
        return value, output.value, digest, pickled


class HashDigestLayer(TransformLayer):
    def __init__(self, names: Iterable[str]):
        inputs, outputs, edges = [], [], []
        for name in names:
            inp, out = Node(name), Node(name)
            inputs.append(inp)
            outputs.append(out)
            edges.append(HashDigestEdge().bind(inp, out))

        super().__init__(inputs, outputs, edges, virtual_nodes=True)
