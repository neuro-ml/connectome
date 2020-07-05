from connectome.blocks import FunctionEdge, CustomLayer, Pipeline, MemoryCacheLayer
from connectome.engine import Node
from connectome.utils import extract_signature


def funcs_layer(**kwargs):
    def get_node(n):
        if n not in scope:
            scope[n] = Node(n)
        return scope[n]

    scope = {}
    return CustomLayer([
        FunctionEdge(func, list(map(get_node, extract_signature(func))), Node(name))
        for name, func in kwargs.items()
    ])


def test_single():
    block = funcs_layer(
        sum=lambda x, y: x + y,
        sub=lambda x, y: x - y,
        squared=lambda x: x ** 2,
    )
    assert block(3, 2) == (5, 1, 9)
    assert block.sum(1, 2) == 3
    assert block.sub(1, 2) == -1
    assert block.squared(9) == 81


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
    chain = Pipeline(first)
    assert chain.sum(1, 2) == 3
    assert chain.squared(4) == 16

    chain = Pipeline(first, second)
    assert chain.prod(7) == 7 ** 5
    assert chain.min(3) == 9
    assert chain.sub(5, 3) == 2

    chain = Pipeline(first, second, third)
    assert chain.div(7) == 7 ** 4
    assert chain.original(x=9, y=10) == 9


def test_cache():
    def counter(x):
        nonlocal count
        count += 1
        return x

    count = 0
    first = funcs_layer(x=counter)
    assert first.x(1) == 1
    assert count == 1

    chain = Pipeline(first, MemoryCacheLayer())
    assert chain.x(1) == 1
    assert count == 2
    assert chain.x(1) == 1
    assert count == 2

    assert chain.x(2) == 2
    assert count == 3
    assert chain.x(2) == 2
    assert count == 3
