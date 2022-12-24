from typing import Callable

from ..containers.base import EdgesBag, BagContext
from .base import CallableLayer
from ..engine.base import Node
from ..engine.edges import FunctionEdge
from ..utils import AntiSet


class Apply(CallableLayer):
    """
    A layer that applies separate functions to each of the specified names.

    `Apply` provides a convenient shortcut for transformations that only depend on the previous value of the name.

    Examples
    --------
    >>> Apply(image=zoom, mask=zoom_binary)
    >>> # is the same as using
    >>> class Zoom(Transform):
    ...     __inherit__ = True
    ...
    ...     def image(image):
    ...         return zoom(image)
    ...
    ...     def mask(mask):
    ...         return zoom_binary(mask)
    """

    def __init__(self, **transform: Callable):
        self._names = sorted(transform)

        inputs, outputs, edges = [], [], []
        for name, func in transform.items():
            inp, out = Node(name), Node(name)
            inputs.append(inp)
            outputs.append(out)
            edges.append(FunctionEdge(func, arity=1).bind(inp, out))

        super().__init__(EdgesBag(
            inputs, outputs, edges, context=BagContext((), (), AntiSet()),
            virtual_nodes=AntiSet(transform), optional_nodes=None, persistent_nodes=None,
        ), ())
