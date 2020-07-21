import pytest

from connectome.layers import EdgesBag, BoundEdge
from connectome.engine.edges import FunctionEdge
from connectome.engine.base import Node
from connectome.utils import extract_signature

pytest_plugins = ['graph_fixtures']


class Builder:
    @staticmethod
    def build_layer(**kwargs):

        parameters = {}
        forward_methods = {}
        backward_methods = {}

        for name, func in kwargs.items():
            if name.startswith('inverse_'):
                name = name[len('inverse_'):]
                backward_methods[name] = func
            elif name.startswith('_'):
                parameters[name] = func
            else:
                forward_methods[name] = func

        def get_node(n, dct):
            if n not in dct:
                dct[n] = Node(n)
            return dct[n]

        def get_related_nodes(key: str, backward=False):
            if key.startswith('_'):
                return parameter_nodes[key]
            else:
                if backward:
                    return get_node(key, backward_inputs)
                else:
                    return get_node(key, inputs)

        inputs = {}
        outputs = {}
        parameter_nodes = {n: Node(n) for n, _ in parameters.items()}

        backward_inputs = {}
        backward_outputs = {}

        edges = []
        for name, func in forward_methods.items():
            output_node = get_node(name, outputs)
            attr_names = extract_signature(func)
            cur_inputs = list(map(get_related_nodes, attr_names))
            edges.append(BoundEdge(FunctionEdge(func, len(attr_names)), cur_inputs, output_node))

        for name, func in parameters.items():
            output_node = parameter_nodes[name]
            attr_names = extract_signature(func)
            cur_inputs = list(map(get_related_nodes, attr_names))
            edges.append(BoundEdge(FunctionEdge(func, len(attr_names)), cur_inputs, output_node))

        for name, func in backward_methods.items():
            output_node = get_node(name, backward_outputs)
            attr_names = extract_signature(func)
            cur_inputs = [get_related_nodes(name, True) for name in attr_names]
            edges.append(BoundEdge(FunctionEdge(func, len(attr_names)), cur_inputs, output_node))

        return EdgesBag(list(inputs.values()), list(outputs.values()), edges,
                        list(backward_inputs.values()), list(backward_outputs.values()))


@pytest.fixture(scope='session')
def builder():
    return Builder
