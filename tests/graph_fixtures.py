import pytest

from connectome.containers.pipeline import PipelineContainer

from connectome.engine.base import Node, BoundEdge
from connectome.engine.edges import FunctionEdge
from connectome.containers.transform import TransformContainer

from connectome.utils import extract_signature


class LayerMaker:
    @staticmethod
    def make_layer(optional_nodes=(), **kwargs):

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
            attr_names, _ = extract_signature(func)
            cur_inputs = list(map(get_related_nodes, attr_names))
            edges.append(BoundEdge(FunctionEdge(func, len(attr_names)), cur_inputs, output_node))

        for name, func in parameters.items():
            output_node = parameter_nodes[name]
            attr_names, _ = extract_signature(func)
            cur_inputs = list(map(get_related_nodes, attr_names))
            edges.append(BoundEdge(FunctionEdge(func, len(attr_names)), cur_inputs, output_node))

        for name, func in backward_methods.items():
            output_node = get_node(name, backward_outputs)
            attr_names, _ = extract_signature(func)
            cur_inputs = [get_related_nodes(name, True) for name in attr_names]
            edges.append(BoundEdge(FunctionEdge(func, len(attr_names)), cur_inputs, output_node))

        return TransformContainer(
            list(inputs.values()), list(outputs.values()), edges,
            list(backward_inputs.values()), list(backward_outputs.values()), optional_nodes=optional_nodes,
            forward_virtual=(), backward_virtual=(),
        )


@pytest.fixture(scope='module')
def layer_maker():
    return LayerMaker


@pytest.fixture
def first_simple(layer_maker):
    return PipelineContainer(layer_maker.make_layer(
        sum=lambda x, y: x + y,
        sub=lambda x, y: x - y,
        squared=lambda x: x ** 2,
        cube=lambda x: x ** 3,
        x=lambda x: x,
        y=lambda y: y,
    ))


@pytest.fixture
def second_simple(layer_maker):
    return layer_maker.make_layer(
        prod=lambda squared, cube: squared * cube,
        min=lambda squared, cube: min(squared, cube),
        x=lambda x: x,
        y=lambda y: y,
        sub=lambda sub: sub,
    )


@pytest.fixture
def third_simple(layer_maker):
    return layer_maker.make_layer(
        div=lambda prod, x: prod / x,
        original=lambda sub, y: sub + y,
    )


@pytest.fixture
def first_backward(layer_maker):
    return layer_maker.make_layer(
        prod=lambda x, _spacing: x * _spacing,
        inverse_prod=lambda prod, _spacing: prod / _spacing,
        _spacing=lambda: 2
    )


@pytest.fixture
def second_backward(layer_maker):
    return layer_maker.make_layer(
        prod=lambda prod: str(prod + 1),
        inverse_prod=lambda prod: int(prod) - 1,
    )


@pytest.fixture
def all_optional(layer_maker):
    return layer_maker.make_layer(sum=lambda x, y: x + y,
                                  prod=lambda x, y: x * y,
                                  sub=lambda x, y: x - y,
                                  optional_nodes=['sub'])
