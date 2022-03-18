from functools import partial
from hashlib import blake2b
from typing import Iterable, Any, Generator

from tarn.cache.storage import key_to_digest

from .transform import TransformContainer
from ..engine.base import Node, Request, Response, Command
from ..engine.edges import StaticHash, StaticGraph
from ..engine.node_hash import NodeHashes, NodeHash


class HashDigestEdge(StaticGraph, StaticHash):
    def __init__(self):
        super().__init__(arity=1)
        # TODO: find a way to pass different hashers
        self._hasher = partial(blake2b, digest_size=64)

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]

    def evaluate(self) -> Generator[Request, Response, Any]:
        value = yield Command.ParentValue, 0
        output = yield Command.CurrentHash,

        pickled, digest = key_to_digest(self._hasher, output.value)
        return value, output.value, digest, pickled


class HashDigestContainer(TransformContainer):
    def __init__(self, names: Iterable[str]):
        inputs, outputs, edges = [], [], []
        for name in names:
            inp, out = Node(name), Node(name)
            inputs.append(inp)
            outputs.append(out)
            edges.append(HashDigestEdge().bind(inp, out))

        super().__init__(inputs, outputs, edges, forward_virtual=True, backward_virtual=True)
