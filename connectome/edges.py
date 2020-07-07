from typing import Sequence, Tuple, Any

from .cache import MemoryStorage, CacheStorage
from .engine import GraphParameter, Node, Edge


class IdentityEdge(Edge):
    def __init__(self, incoming: Node, output: Node):
        super().__init__([incoming], output)

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: GraphParameter) -> Tuple[Any]:
        return arguments[0]

    def process_parameters(self, parameters: Sequence[GraphParameter]):
        assert len(parameters) == 1
        return self.inputs, parameters[0]


class CacheEdge(Edge):
    def __init__(self, incoming: Node, output: Node, *, storage: CacheStorage = None):
        super().__init__([incoming], output)
        if storage is None:
            storage = MemoryStorage()
        self.storage = storage

    def process_parameters(self, parameters: Sequence[GraphParameter]):
        assert len(parameters) == 1
        parameter = parameters[0]
        if self.storage.contains(parameter):
            inputs = []
        else:
            inputs = self.inputs

        return inputs, parameter

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: GraphParameter):
        if self.storage.contains(parameter):
            return self.storage.get(parameter)
        else:
            self.storage.set(parameter, arguments[0])
            return arguments[0]


class FunctionEdge(Edge):
    def __init__(self, function, inputs: Sequence[Node], output: Node):
        super().__init__(inputs, output)
        self.function = function

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: GraphParameter) -> Tuple[Any]:
        return self.function(*arguments)

    def process_parameters(self, parameters: Sequence[GraphParameter]):
        return self.inputs, self._merge_parameters([GraphParameter(self.function)] + list(parameters))


class ValueEdge(Edge):
    def __init__(self, target: Node, value):
        super().__init__([], target)
        self.value = value

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: GraphParameter) -> Tuple[Any]:
        return self.value

    def process_parameters(self, parameters: Sequence[GraphParameter]):
        assert not parameters
        return self.inputs, GraphParameter(self.value)
