from typing import Callable

from ..containers import ReversibleContainer
from ..engine import Node, FunctionEdge, Details
from ..utils import AntiSet
from .base import CallableLayer


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

        details = Details(type(self))
        inputs, outputs, edges = [], [], []
        for name, func in transform.items():
            inp, out = Node(name, details), Node(name, details)
            inputs.append(inp)
            outputs.append(out)
            edges.append(FunctionEdge(func, arity=1).bind(inp, out))

        super().__init__(ReversibleContainer(
            inputs, outputs, edges, forward_virtual=AntiSet(transform), backward_virtual=AntiSet(),
        ), ())
