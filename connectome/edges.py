from typing import Sequence, Tuple, Any, Callable
from .cache import CacheStorage
from .engine import NodeHash, Node, Edge


class IdentityEdge(Edge):
    def __init__(self, incoming: Node, output: Node):
        super().__init__([incoming], output)

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: NodeHash):
        return arguments[0]

    def process_parameters(self, parameters: Sequence[NodeHash]):
        assert len(parameters) == 1
        return self.inputs, parameters[0]


class CacheEdge(Edge):
    def __init__(self, incoming: Node, output: Node, *, storage: CacheStorage):
        super().__init__([incoming], output)
        self.storage = storage

    def process_parameters(self, parameters: Sequence[NodeHash]):
        assert len(parameters) == 1
        parameter = parameters[0]
        if self.storage.contains(parameter):
            inputs = []
        else:
            inputs = self.inputs

        return inputs, parameter

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: NodeHash):
        # no arguments means that the value is cached
        if not arguments:
            return self.storage.get(parameter)

        assert len(arguments) == 1
        value = arguments[0]
        self.storage.set(parameter, value)
        return value


class FunctionEdge(Edge):
    def __init__(self, function, inputs: Sequence[Node], output: Node):
        super().__init__(inputs, output)
        self.function = function

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: NodeHash):
        return self.function(*arguments)

    def process_parameters(self, parameters: Sequence[NodeHash]):
        return self.inputs, NodeHash.from_hash_nodes([NodeHash(data=self.function)] + list(parameters), prev_edge=self)


class ValueEdge(Edge):
    """
    Used in interface to provide constant parameters.
    """

    def __init__(self, target: Node, value):
        super().__init__([], target)
        self.value = value

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: NodeHash):
        return self.value

    def process_parameters(self, parameters: Sequence[NodeHash]):
        assert not parameters
        return self.inputs, NodeHash(data=self.value, prev_edge=self)


class InitEdge(FunctionEdge):
    """
    Used to hold the ``self`` object created after calling __init__.
    ``function`` is stored for hashing purposes.
    """

    def __init__(self, init, this, inputs: Sequence[Node], output: Node):
        super().__init__(init, inputs, output)
        self.this = this

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: NodeHash):
        return self.this


class ItemGetterEdge(Edge):
    """
    Used in conjunction with `SelfEdge` to provide constant parameters.
    """

    def __init__(self, name: str, incoming: Node, output: Node):
        super().__init__([incoming], output)
        self.name = name

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: NodeHash):
        return arguments[0][self.name]

    def process_parameters(self, parameters: Sequence[NodeHash]):
        return NodeHash.from_hash_nodes([NodeHash(data=self.name), *parameters], prev_edge=self)


class MuxEdge(Edge):
    def __init__(self, branch_selector: Callable, inputs: Sequence[Node], output: Node):
        super().__init__(inputs, output)
        self.branch_selector = branch_selector

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter):
        return arguments[0]

    def process_parameters(self, parameters: Sequence[NodeHash]):
        branch_codes = self.find_node_by_name(parameters)

        assert len(set(branch_codes.values())) == 1
        branch_code = list(branch_codes.values())[0]
        return self.branch_selector(branch_code, self.inputs, parameters)

    @staticmethod
    def find_node_by_name(parameters: Sequence[NodeHash], target_name='id'):
        result = {}

        # TODO generalize it somehow
        def find_name_rec(params: Sequence[NodeHash]):
            for param in params:
                if param.prev_edge is not None:
                    for i in param.prev_edge.inputs:
                        if i.name == target_name:
                            assert isinstance(param.prev_edge, FunctionEdge)
                            result[param] = param.data[1]

                if param.children is not None:
                    find_name_rec(param.children)

        find_name_rec(parameters)
        return result
