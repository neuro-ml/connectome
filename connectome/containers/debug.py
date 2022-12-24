from hashlib import blake2b
from typing import Iterable

from ..engine import Node
from ..layers.chain import connect
from ..layers.debug import HashDigestEdge
from .base import EdgesBag
from ..utils import deprecation_warn


class HashDigestContainer(EdgesBag):  # pragma: no cover
    def __init__(self, names: Iterable[str]):
        deprecation_warn()
        inputs, outputs, edges = [], [], []
        for name in names:
            inp, out = Node(name), Node(name)
            inputs.append(inp)
            outputs.append(out)
            edges.append(HashDigestEdge(blake2b).bind(inp, out))

        super().__init__(
            inputs, outputs, edges,
            context=None, virtual_nodes=None, persistent_nodes=None, optional_nodes=None,
        )

    def wrap(self, container: 'EdgesBag') -> 'EdgesBag':
        return connect(container, self)
