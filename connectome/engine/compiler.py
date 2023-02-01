from concurrent.futures import Executor
from typing import Union, Tuple

from ..exceptions import FieldError, DependencyError
from ..utils import NameSet, check_for_duplicates
from .base import TreeNode, Nodes, BoundEdges, TreeNodes
from .edges import ProductEdge
from .graph import Graph


class GraphCompiler:
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, virtuals: NameSet, optionals: Nodes,
                 executor: Executor):
        check_for_duplicates(inputs)
        check_for_duplicates(outputs)
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
        self._dependencies = self._outputs = None

    def fields(self):
        if self._outputs is None:
            self._validate_optionals()

        return list(self._outputs)

    def compile(self, output: Union[str, Tuple[str]]):
        if not isinstance(output, (str, tuple)):
            raise TypeError(f'The name must be either a string or a tuple of strings, not {type(output)}')

        if self._outputs is None:
            self._validate_optionals()

        if output not in self._cache:
            self._cache[output] = self._compile(output)

        return self._cache[output]

    def _compile(self, item):
        def get_node(out):
            # either present in the outputs
            if out in self._outputs:
                return self._outputs[out]
            # or it's virtual
            if out in self._virtuals:
                return
            # or it was optional
            if out in self._all_outputs:
                missing = self._dependencies[self._all_outputs[out]] - self._inputs
                raise FieldError(
                    f'The field "{out}" was discarded because it had unreachable inputs: '
                    f'{", ".join(map(pretty, missing))}.'
                )
            # or it wasn't defined at all
            raise FieldError(f'The field "{out}" is not defined')

        if isinstance(item, str):
            node = get_node(item)
            if node is None:
                # TODO: signature
                return identity

            return Graph(self._inputs, node, self._executor).call

        inputs, outputs = [], []
        for name in item:
            node = get_node(name)
            if node is None:
                node = TreeNode(name, None, None)
                inputs.append(node)

            outputs.append(node)

        product = TreeNode('tuple', (ProductEdge(len(item)), outputs), None)
        return Graph(self._inputs | set(inputs), product, self._executor).call

    def __getitem__(self, item):
        # TODO: deprecate
        return self.compile(item)

    def _validate_optionals(self):
        self._dependencies = find_dependencies(self._all_outputs.values())

        available = {}
        for output in self._all_outputs.values():
            missing = self._dependencies[output] - self._inputs
            if missing:
                if output not in self._optionals:
                    raise DependencyError(
                        f'The output {pretty(output)} has unreachable inputs: {", ".join(map(pretty, missing))}'
                    )

                not_optional = missing - self._optionals
                if not_optional:
                    raise DependencyError(
                        f'The output {pretty(output)} has unreachable inputs: {", ".join(map(pretty, missing))}, '
                        f'some of which are not optional: {", ".join(map(pretty, not_optional))}'
                    )

            else:
                available[output.name] = output

        self._outputs = available


def identity(x):
    return x


def pretty(node: TreeNode):
    result, parents = repr(node.name), []
    details = node.details
    while details is not None:
        parents.append(repr(details.layer))
        details = details.parent

    if parents:
        result += f' (layer {" -> ".join(parents)})'
    return result


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
