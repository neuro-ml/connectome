from typing import Callable, Dict, Iterable

from .transform import TransformLayer
from ..engine.base import Node
from ..engine.edges import FunctionEdge, IdentityEdge


class ApplyLayer(TransformLayer):
    def __init__(self, transforms: Dict[str, Callable]):
        inputs, outputs, edges = [], [], []
        for name, func in transforms.items():
            inp, out = Node(name), Node(name)
            inputs.append(inp)
            outputs.append(out)
            edges.append(FunctionEdge(func, arity=1).bind(inp, out))

        super().__init__(inputs, outputs, edges, virtual_nodes=True)


class IdentityLayer(TransformLayer):
    def __init__(self, names: Iterable[str]):
        inputs, outputs, edges = [], [], []
        for name in names:
            inp, out = Node(name), Node(name)
            inputs.append(inp)
            outputs.append(out)
            edges.append(IdentityEdge().bind(inp, out))

        super().__init__(inputs, outputs, edges, virtual_nodes=True)
