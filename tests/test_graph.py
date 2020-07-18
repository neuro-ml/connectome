import pytest

from connectome.layers import CustomLayer, PipelineLayer, EdgesBag, BoundEdge
from connectome.engine.edges import FunctionEdge, ValueEdge
from connectome.engine.base import Node
from connectome.utils import extract_signature


def funcs_layer(**kwargs):
    def get_input(n):
        if n in outputs:
            return outputs[n]
        if n not in inputs:
            inputs[n] = Node(n)

        return inputs[n]

    def get_output(n):
        outputs[n] = Node(n)
        return outputs[n]

    inputs = {}
    outputs = {}
    edges = [
        BoundEdge(
            FunctionEdge(func, len(extract_signature(func))),
            list(map(get_input, extract_signature(func))),
            get_output(name),
        ) for name, func in kwargs.items()
    ]
    return EdgesBag(list(inputs.values()), list(outputs.values()), edges)


@pytest.fixture(scope='module')
def first_layer():
    return funcs_layer(
        sum=lambda x, y: x + y,
        sub=lambda x, y: x - y,
        squared=lambda x: x ** 2,
        cube=lambda x: x ** 3,
        x=lambda x: x,
        y=lambda y: y,
    )


@pytest.fixture(scope='module')
def second_layer():
    return funcs_layer(
        prod=lambda squared, cube: squared * cube,
        min=lambda squared, cube: min(squared, cube),
        x=lambda x: x,
        y=lambda y: y,
        sub=lambda sub: sub,
    )


@pytest.fixture(scope='module')
def third_layer():
    return funcs_layer(
        div=lambda prod, x: prod / x,
        original=lambda sub, y: sub + y,
    )


def test_single(first_layer):
    assert first_layer.get_forward_method('sum')(1, 2) == 3
    assert first_layer.get_forward_method('sub')(1, 2) == -1
    assert first_layer.get_forward_method('squared')(9) == 81


def test_chain(first_layer, second_layer, third_layer):
    chain = PipelineLayer(first_layer)

    assert chain.get_forward_method('sum')(1, 2) == 3
    assert chain.get_forward_method('squared')(4) == 16

    chain = PipelineLayer(first_layer, second_layer)
    assert chain.get_forward_method('prod')(7) == 7 ** 5
    assert chain.get_forward_method('min')(3) == 9
    assert chain.get_forward_method('sub')(5, 3) == 2

    chain = PipelineLayer(first_layer, second_layer, third_layer)
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


def test_slicing(first_layer, second_layer, third_layer):
    chain = PipelineLayer(first_layer, second_layer, third_layer)

    assert chain.slice(1, 3).get_forward_method('div')(squared=4, cube=3, x=3) == 4
    assert chain.slice(0, 1).get_forward_method('sum')(x=2, y=10) == 12
    assert chain.slice(0, 2).get_forward_method('min')(x=5) == 25


def test_backward():
    forward_input = Node('image')
    forward_output = Node('image')

    backward_input = Node('image')
    backward_output = Node('image')

    spacing = 2
    spacing_node = Node('spacing')
    spacing_edge = ValueEdge(spacing_node, spacing)

    forward_edge = FunctionEdge(lambda x, y: x * y, [forward_input, spacing_node], forward_output)

    backward_edge = FunctionEdge(lambda x, y: x / y, [backward_input, spacing_node], backward_output)

    first = CustomLayer([forward_input], [forward_output], [spacing_edge, forward_edge, backward_edge],
                        [backward_input], [backward_output])

    node_interface = first.get_node_interface('image')
    assert node_interface.backward(10) == 5
    assert node_interface.backward(node_interface.forward(15)) == 15

    first = PipelineLayer(first)

    node_interface = first.get_node_interface('image')
    assert node_interface.backward(10) == 5
    assert node_interface.backward(node_interface.forward(15)) == 15

    forward_input = Node('image')
    forward_output = Node('image')

    backward_input = Node('image')
    backward_output = Node('image')

    forward_edge = FunctionEdge(lambda x: str(x), [forward_input], forward_output)
    backward_edge = FunctionEdge(lambda x: int(x), [backward_input], backward_output)

    second = CustomLayer([forward_input], [forward_output], [forward_edge, backward_edge],
                         [backward_input], [backward_output])

    chain = PipelineLayer(first, second)
    node_interface = chain.get_node_interface('image')

    assert node_interface.forward(10) == '20'
    assert node_interface.backward(node_interface.forward(15)) == 15.0


def test_mux():
    pass
