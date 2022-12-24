import pytest

from connectome import CacheToRam
from connectome.layers.chain import connect


def test_single(first_simple):
    methods = first_simple.compile()
    assert methods['sum'](1, 2) == 3
    assert methods['sub'](1, 2) == -1
    assert methods['squared'](9) == 81


def test_duplicates(layer_maker):
    double = layer_maker.make_layer(x=lambda x: 2 * x)
    assert double.compile()['x'](4) == 8
    eight = connect(
        double, double, double,
    )
    assert eight.compile()['x'](4) == 32


def test_chain(first_simple, second_simple, third_simple):
    chain = first_simple
    methods = chain.compile()

    assert methods['sum'](1, 2) == 3
    assert methods['squared'](4) == 16

    chain = connect(first_simple, second_simple)
    methods = chain.compile()
    assert methods['prod'](7) == 7 ** 5
    assert methods['min'](3) == 9
    assert methods['sub'](5, 3) == 2

    chain = connect(first_simple, second_simple, third_simple)
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

    chain = CacheToRam('x')._connect(first)
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


def test_nodes_caching(layer_maker):
    def counter(x):
        nonlocal count
        count += 1
        return x

    count = 0
    chain = connect(
        layer_maker.make_layer(x=lambda x: x + 1),
        layer_maker.make_layer(x=counter),
        layer_maker.make_layer(a=lambda x: x, b=lambda x: x, x=lambda x: x),
        layer_maker.make_layer(c=lambda a, b: (a, b), d=lambda x: x),
    )
    method = chain.compile()['c']
    assert method(0) == (1, 1)
    assert count == 1

    method = chain.compile()['c',]
    assert method(0) == ((1, 1),)
    assert count == 2

    method = chain.compile()['c', 'd']
    assert method(0) == ((1, 1), 1)
    assert count == 3


@pytest.mark.skip
def test_backward_methods(first_backward, second_backward):
    assert first_backward.get_backward_method('prod')(10) == 5
    assert first_backward.get_backward_method('prod')(first_backward.get_forward_method('prod')(15)) == 15

    first_backward = first_backward
    assert first_backward.get_backward_method('prod')(10) == 5
    assert first_backward.get_backward_method('prod')(first_backward.get_forward_method('prod')(15)) == 15

    chain = connect(first_backward, second_backward)
    methods = chain.compile()
    assert methods['prod'](10) == '21'
    assert chain.get_backward_method('prod')(methods['prod'](15)) == 15.0


def test_loopback(first_backward, second_backward, layer_maker):
    layer = connect(first_backward, second_backward)

    wrapped = layer.loopback(lambda x: x, 'prod', 'prod')
    assert wrapped['prod'](4) == 4

    wrapped = layer.loopback(lambda x: x * 2, 'prod', 'prod')
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

    layer = connect(layer, cross_pipes_checker)
    wrapped = layer.loopback(lambda x: x * 2, 'prod', 'prod')
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

    layer = connect(first, second)
    assert layer.compile()['first_out'](4, 3) == 84
