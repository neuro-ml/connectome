import pytest

from connectome.layers import PipelineLayer, EdgesBag, BoundEdge, MemoryCacheLayer
from connectome.engine.edges import FunctionEdge, ValueEdge
from connectome.engine.base import Node
from connectome.utils import extract_signature


def funcs_layer(**kwargs):
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


# TODO move fixtures to separate file

@pytest.fixture(scope='module')
def first_simple():
    return PipelineLayer(funcs_layer(
        sum=lambda x, y: x + y,
        sub=lambda x, y: x - y,
        squared=lambda x: x ** 2,
        cube=lambda x: x ** 3,
        x=lambda x: x,
        y=lambda y: y,
    ))


@pytest.fixture(scope='module')
def second_simple():
    return funcs_layer(
        prod=lambda squared, cube: squared * cube,
        min=lambda squared, cube: min(squared, cube),
        x=lambda x: x,
        y=lambda y: y,
        sub=lambda sub: sub,
    )


@pytest.fixture(scope='module')
def third_simple():
    return funcs_layer(
        div=lambda prod, x: prod / x,
        original=lambda sub, y: sub + y,
    )


@pytest.fixture(scope='module')
def first_backward():
    return funcs_layer(
        prod=lambda x, _spacing: x * _spacing,
        inverse_prod=lambda prod, _spacing: prod / _spacing,
        _spacing=lambda: 2
    )


@pytest.fixture(scope='module')
def second_backward():
    return funcs_layer(
        prod=lambda prod: str(prod + 1),
        inverse_prod=lambda prod: int(prod) - 1,
    )


def test_single(first_simple):
    assert first_simple.get_forward_method('sum')(1, 2) == 3
    assert first_simple.get_forward_method('sub')(1, 2) == -1
    assert first_simple.get_forward_method('squared')(9) == 81


def test_duplicates():
    double = funcs_layer(x=lambda x: 2 * x)
    assert double.get_forward_method('x')(4) == 8
    eight = PipelineLayer(
        double, double, double,
    )
    assert eight.get_forward_method('x')(4) == 32


def test_chain(first_simple, second_simple, third_simple):
    chain = PipelineLayer(first_simple)

    assert chain.get_forward_method('sum')(1, 2) == 3
    assert chain.get_forward_method('squared')(4) == 16

    chain = PipelineLayer(first_simple, second_simple)
    assert chain.get_forward_method('prod')(7) == 7 ** 5
    assert chain.get_forward_method('min')(3) == 9
    assert chain.get_forward_method('sub')(5, 3) == 2

    chain = PipelineLayer(first_simple, second_simple, third_simple)
    assert chain.get_forward_method('div')(7) == 7 ** 4
    assert chain.get_forward_method('original')(x=9, y=10) == 9


def test_cache():
    def counter(x):
        nonlocal count
        count += 1
        return x

    count = 0
    first = funcs_layer(x=counter)
    assert first.get_forward_method('x')(1) == 1
    assert count == 1

    chain = PipelineLayer(first, MemoryCacheLayer(names=['x']))
    assert chain.get_forward_method('x')(1) == 1
    assert count == 2
    assert chain.get_forward_method('x')(1) == 1
    assert count == 2

    assert chain.get_forward_method('x')(2) == 2
    assert count == 3
    assert chain.get_forward_method('x')(2) == 2
    assert count == 3


def test_slicing(first_simple, second_simple, third_simple):
    chain = PipelineLayer(first_simple, second_simple, third_simple)

    assert chain.slice(1, 3).get_forward_method('div')(squared=4, cube=3, x=3) == 4
    assert chain.slice(0, 1).get_forward_method('sum')(x=2, y=10) == 12
    assert chain.slice(0, 2).get_forward_method('min')(x=5) == 25


def test_backward_methods(first_backward, second_backward):
    assert first_backward.get_backward_method('prod')(10) == 5
    assert first_backward.get_backward_method('prod')(first_backward.get_forward_method('prod')(15)) == 15

    first_backward = PipelineLayer(first_backward)
    assert first_backward.get_backward_method('prod')(10) == 5
    assert first_backward.get_backward_method('prod')(first_backward.get_forward_method('prod')(15)) == 15

    chain = PipelineLayer(first_backward, second_backward)
    assert chain.get_forward_method('prod')(10) == '21'
    assert chain.get_backward_method('prod')(chain.get_forward_method('prod')(15)) == 15.0


def test_loopback(first_backward, second_backward):
    layer = PipelineLayer(first_backward, second_backward)

    wrapped = layer.get_loopback(lambda prod: prod, 'prod')
    assert wrapped(4) == 4

    wrapped = layer.get_loopback(lambda prod: prod * 2, 'prod')
    assert wrapped(4) == 49.

    def counter():
        nonlocal count
        count += 1
        return 5

    count = 0
    cross_pipes_checker = funcs_layer(
        prod=lambda prod, _counter: prod,
        inverse_prod=lambda prod, _counter: prod,
        _counter=counter
    )

    layer = PipelineLayer(layer, cross_pipes_checker)
    wrapped = layer.get_loopback(lambda prod: prod * 2, 'prod')
    assert wrapped(4) == 49.
    assert count == 1


def test_mux():
    pass
