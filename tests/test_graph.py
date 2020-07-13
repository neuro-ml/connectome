from connectome.layers import CustomLayer, MemoryCacheLayer, PipelineLayer
from connectome.edges import FunctionEdge
from connectome.engine import Node
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
        FunctionEdge(func, list(map(get_input, extract_signature(func))), get_output(name))
        for name, func in kwargs.items()
    ]
    return CustomLayer(list(inputs.values()), list(outputs.values()), edges)


def test_single():
    block = funcs_layer(
        sum=lambda x, y: x + y,
        sub=lambda x, y: x - y,
        squared=lambda x: x ** 2,
    )
    assert block(3, 2) == (5, 1, 9)
    assert block.get_method('sum')(1, 2) == 3
    assert block.get_method('sub')(1, 2) == -1
    assert block.get_method('squared')(9) == 81


def test_chain():
    first = funcs_layer(
        sum=lambda x, y: x + y,
        sub=lambda x, y: x - y,
        squared=lambda x: x ** 2,
        cube=lambda x: x ** 3,
        x=lambda x: x,
        y=lambda y: y,
    )
    second = funcs_layer(
        prod=lambda squared, cube: squared * cube,
        min=lambda squared, cube: min(squared, cube),
        x=lambda x: x,
        y=lambda y: y,
        sub=lambda sub: sub,
    )
    third = funcs_layer(
        div=lambda prod, x: prod / x,
        original=lambda sub, y: sub + y,
    )
    chain = PipelineLayer(first)
    assert chain.get_method('sum')(1, 2) == 3
    assert chain.get_method('squared')(4) == 16

    chain = PipelineLayer(first, second)
    assert chain.get_method('prod')(7) == 7 ** 5
    assert chain.get_method('min')(3) == 9
    assert chain.get_method('sub')(5, 3) == 2

    chain = PipelineLayer(first, second, third)
    assert chain.get_method('div')(7) == 7 ** 4
    assert chain.get_method('original')(x=9, y=10) == 9


def test_cache():
    def counter(x):
        nonlocal count
        count += 1
        return x

    count = 0
    first = funcs_layer(x=counter)
    assert first.get_method('x')(1) == 1
    assert count == 1

    chain = PipelineLayer(first, MemoryCacheLayer())
    assert chain.get_method('x')(1) == 1
    assert count == 2
    assert chain.get_method('x')(1) == 1
    assert count == 2

    assert chain.get_method('x')(2) == 2
    assert count == 3
    assert chain.get_method('x')(2) == 2
    assert count == 3


def test_mux():
    pass
