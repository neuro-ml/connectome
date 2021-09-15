from typing import Callable, Dict

from .transform import TransformContainer
from ..engine.base import Node
from ..engine.edges import FunctionEdge


class ApplyContainer(TransformContainer):
    def __init__(self, transforms: Dict[str, Callable]):
        inputs, outputs, edges = [], [], []
        for name, func in transforms.items():
            inp, out = Node(name), Node(name)
            inputs.append(inp)
            outputs.append(out)
            edges.append(FunctionEdge(func, arity=1).bind(inp, out))

        super().__init__(inputs, outputs, edges, forward_virtual=True, backward_virtual=True)
