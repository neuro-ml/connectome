import pytest

from connectome.layers.cache import MemoryCacheLayer
from connectome.layers.pipeline import PipelineLayer


def test_single(first_simple):
    methods = first_simple.compile()
    assert methods['sum'](1, 2) == 3
    assert methods['sub'](1, 2) == -1
    assert methods['squared'](9) == 81


def test_duplicates(layer_maker):
    double = layer_maker.make_layer(x=lambda x: 2 * x)
    assert double.compile()['x'](4) == 8
    eight = PipelineLayer(
        double, double, double,
    )
    assert eight.compile()['x'](4) == 32


def test_chain(first_simple, second_simple, third_simple):
    chain = PipelineLayer(first_simple)
    methods = chain.compile()

    assert methods['sum'](1, 2) == 3
    assert methods['squared'](4) == 16

    chain = PipelineLayer(first_simple, second_simple)
    methods = chain.compile()
    assert methods['prod'](7) == 7 ** 5
    assert methods['min'](3) == 9
    assert methods['sub'](5, 3) == 2

    chain = PipelineLayer(first_simple, second_simple, third_simple)
    methods = chain.compile()
    assert methods['div'](7) == 7 ** 4
    assert methods['original'](x=9, y=10) == 9


def test_cache(layer_maker):
    def counter(x):
        nonlocal count
        count += 1
        return x

    count = 0
    first = layer_maker.make_layer(x=counter, inverse_x=lambda x: x)
    assert first.compile()['x'](1) == 1
    assert count == 1

    chain = PipelineLayer(first, MemoryCacheLayer(size=None, names=['x']))
    methods = chain.compile()
    assert methods['x'](1) == 1
    assert count == 2
    assert methods['x'](1) == 1
    assert count == 2

    assert methods['x'](2) == 2
    assert count == 3
    assert methods['x'](2) == 2
    assert count == 3
    # assert methods['x'](chain.get_backward_method('x')(3)) == 3
    # assert count == 4


def test_slicing(first_simple, second_simple, third_simple):
    chain = PipelineLayer(first_simple, second_simple, third_simple)

    assert chain.slice(1, 3).compile()['div'](squared=4, cube=3, x=3) == 4
    assert chain.slice(0, 1).compile()['sum'](x=2, y=10) == 12
    assert chain.slice(0, 2).compile()['min'](x=5) == 25


@pytest.mark.skip
def test_backward_methods(first_backward, second_backward):
    assert first_backward.get_backward_method('prod')(10) == 5
    assert first_backward.get_backward_method('prod')(first_backward.get_forward_method('prod')(15)) == 15

    first_backward = PipelineLayer(first_backward)
    assert first_backward.get_backward_method('prod')(10) == 5
    assert first_backward.get_backward_method('prod')(first_backward.get_forward_method('prod')(15)) == 15

    chain = PipelineLayer(first_backward, second_backward)
    methods = chain.compile()
    assert methods['prod'](10) == '21'
    assert chain.get_backward_method('prod')(methods['prod'](15)) == 15.0


def test_loopback(first_backward, second_backward, layer_maker):
    layer = PipelineLayer(first_backward, second_backward)

    wrapped = layer.loopback([[lambda x: x, 'prod', 'prod']])
    assert wrapped['prod'](4) == 4

    wrapped = layer.loopback([[lambda x: x * 2, 'prod', 'prod']])
    assert wrapped['prod'](4) == 49

    def counter():
        nonlocal count
        count += 1
        return 5

    count = 0
    cross_pipes_checker = layer_maker.make_layer(
        prod=lambda prod, _counter: prod,
        inverse_prod=lambda prod, _counter: prod,
        _counter=counter
    )

    layer = PipelineLayer(layer, cross_pipes_checker)
    wrapped = layer.loopback([[lambda x: x * 2, 'prod', 'prod']])
    assert wrapped['prod'](4) == 49
    assert count == 1


def test_optional(first_simple, layer_maker):
    first = layer_maker.make_layer(
        sum=lambda x, y: x + y,
        prod=lambda x, y: x * y,
    )
    second = layer_maker.make_layer(
        first_out=lambda sum, prod: sum * prod,
        second_out=lambda sub: 2 * sub,
        optional_nodes=['sub']
    )

    layer = PipelineLayer(first, second)
    assert layer.compile()['first_out'](4, 3) == 84
