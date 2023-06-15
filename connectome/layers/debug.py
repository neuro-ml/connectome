import hashlib
from typing import Any, Generator, Type, Union

from tarn.compat import HashAlgorithm
from tarn.pickler import dumps

from ..containers import EdgesBag
from ..engine import (
    Command, Details, Node, NodeHash, NodeHashes, Request, Response, StaticGraph, StaticHash, CustomHash, LeafHash
)
from ..utils import StringsLike
from .base import CallableLayer
from .cache import to_seq


class HashDigest(CallableLayer):
    def __init__(self, names: StringsLike, algorithm: Union[Type[HashAlgorithm], str, None] = None,
                 return_value: bool = False):
        if isinstance(algorithm, str):
            algorithm = getattr(hashlib, algorithm)

        names = to_seq(names)
        details = Details(type(self))
        inputs, outputs, edges = [], [], []
        for name in names:
            inp, out = Node(name, details), Node(name, details)
            inputs.append(inp)
            outputs.append(out)
            edges.append(HashDigestEdge(algorithm, return_value).bind(inp, out))

        super().__init__(EdgesBag(
            inputs, outputs, edges,
            context=None, virtual=None, persistent=None, optional=None,
        ), ())


class HashDigestEdge(StaticGraph, StaticHash):
    def __init__(self, algorithm, return_value):
        super().__init__(arity=1)
        self.algorithm = algorithm
        self.return_value = return_value

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return CustomHash('connectome.HashDigest', LeafHash(self.algorithm), LeafHash(self.return_value), *inputs)

    def evaluate(self) -> Generator[Request, Response, Any]:
        result = []
        if self.return_value:
            value = yield Command.ParentValue, 0
            result.append(value)

        node_hash = yield Command.ParentHash, 0
        result.append(node_hash)
        node_hash = node_hash.value

        pickled = dumps(node_hash)
        result.append(pickled)
        if self.algorithm is not None:
            result.append(self.algorithm(pickled).digest())

        return tuple(result)
