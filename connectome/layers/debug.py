import hashlib
from typing import Any, Generator

from tarn.cache.storage import key_to_digest

from ..containers import EdgesBag
from ..engine import Command, Details, Node, NodeHash, NodeHashes, Request, Response, StaticGraph, StaticHash
from ..utils import StringsLike
from .base import CallableLayer
from .cache import to_seq


class HashDigest(CallableLayer):
    def __init__(self, names: StringsLike, algorithm):
        if isinstance(algorithm, str):
            algorithm = getattr(hashlib, algorithm)

        names = to_seq(names)
        details = Details(type(self))
        inputs, outputs, edges = [], [], []
        for name in names:
            inp, out = Node(name, details), Node(name, details)
            inputs.append(inp)
            outputs.append(out)
            edges.append(HashDigestEdge(algorithm).bind(inp, out))

        super().__init__(EdgesBag(
            inputs, outputs, edges,
            context=None, virtual=None, persistent=None, optional=None,
        ), ())


class HashDigestEdge(StaticGraph, StaticHash):
    def __init__(self, algorithm):
        super().__init__(arity=1)
        self.algorithm = algorithm

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]

    def evaluate(self) -> Generator[Request, Response, Any]:
        value = yield Command.ParentValue, 0
        output = yield Command.CurrentHash,

        pickled, digest = key_to_digest(self.algorithm, output.value)
        return value, output.value, digest, pickled
