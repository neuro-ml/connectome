from concurrent.futures import Executor
from typing import Union, Tuple

from ..exceptions import FieldError, DependencyError
from ..utils import NameSet
from .base import TreeNode, Nodes, BoundEdges, TreeNodes
from .edges import ProductEdge
from .graph import Graph


class GraphCompiler:
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, virtuals: NameSet, optionals: Nodes,
                 executor: Executor):
        self._mapping = TreeNode.from_edges(edges)
        # TODO: optimize:
        #  - remove identity edges

        self._edges = edges
        self._inputs = {self._mapping[x] for x in inputs}
        # TODO: make sure the inputs are leaves
        self._all_outputs = {x.name: self._mapping[x] for x in outputs}
        # some optional nodes might be unreachable
        self._optionals = {self._mapping[x] for x in optionals if x in self._mapping}
        self._virtuals = virtuals
        self._executor = executor

        self._cache = {}
        # TODO: validate:
        #  - find unreachable nodes
        self._outputs = None

    def fields(self):
        if self._outputs is None:
            self._outputs = self._validate_optionals()

        return list(self._outputs)

    def compile(self, output: Union[str, Tuple[str]]):
        if not isinstance(output, (str, tuple)):
            raise TypeError(f'The name must be either a string or a tuple of strings, not {type(output)}')

        if self._outputs is None:
            self._outputs = self._validate_optionals()

        if output not in self._cache:
            self._cache[output] = self._compile(output)

        return self._cache[output]

    def _compile(self, item):
        if isinstance(item, str):
            if item in self._virtuals:
                # TODO: signature
                return identity

            if item not in self._outputs:
                # TODO:
                raise FieldError(f'"{item}" is not an available output: {tuple(self._outputs)}')

            return Graph(self._inputs, self._outputs[item], self._executor).call

        if isinstance(item, tuple):
            inputs, outputs = [], []
            for name in item:
                if name not in self._all_outputs:
                    if name in self._virtuals:
                        output = TreeNode(name, None, None)
                        inputs.append(output)
                    else:
                        raise FieldError(f'"{name}" is not an available output: {tuple(self._outputs)}')
                else:
                    output = self._outputs[name]
                outputs.append(output)

            product = TreeNode('tuple', (ProductEdge(len(item)), outputs), None)
            return Graph(self._inputs | set(inputs), product, self._executor).call

        raise FieldError(f'"{item}" is not an available output: {tuple(self._outputs)}')

    def __getitem__(self, item):
        # TODO: deprecate
        return self.compile(item)

    def _validate_optionals(self):
        def pretty(node: TreeNode):
            result, parents = repr(node.name), []
            details = node.details
            while details is not None:
                parents.append(f'"{details.layer.__name__}"')
                details = details.parent

            if parents:
                result += f' (layer {" -> ".join(parents)})'
            return result

        inputs = find_dependencies(self._all_outputs.values())

        available = {}
        for output in self._all_outputs.values():
            missing = inputs[output] - self._inputs
            if missing:
                if output not in self._optionals:
                    raise DependencyError(
                        f'The output {pretty(output)} has unreachable inputs: {", ".join(map(pretty, missing))}'
                    )

                not_optional = missing - self._optionals
                if not_optional:
                    raise DependencyError(
                        f'The output {pretty(output)} has unreachable inputs: {", ".join(map(pretty, missing))}, '
                        f'some of which are not optional: {tuple(map(pretty, not_optional))}'
                    )

            else:
                available[output.name] = output

        return available


def identity(x):
    return x


def find_dependencies(outputs: TreeNodes):
    def visit(node: TreeNode):
        if node in inputs:
            return

        local = set()
        if not node.is_leaf:
            for parent in node.parents:
                if parent.is_leaf:
                    local.add(parent)
                else:
                    visit(parent)
                    local.update(inputs[parent])

        inputs[node] = local

    inputs = {}
    for output in outputs:
        visit(output)

    return inputs
