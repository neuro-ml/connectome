from typing import Callable, Dict

from .transform import TransformLayer, INHERIT_ALL
from ..engine.base import Node, BoundEdge
from ..engine.edges import FunctionEdge


class ApplyLayer(TransformLayer):
    def __init__(self, transforms: Dict[str, Callable]):
        inputs, outputs, edges = [], [], []
        for name, func in transforms.items():
            inp, out = Node(name), Node(name)
            inputs.append(inp)
            outputs.append(out)
            edges.append(BoundEdge(FunctionEdge(func, arity=1), [inp], out))

        super().__init__(inputs, outputs, edges, inherit_nodes=INHERIT_ALL)
