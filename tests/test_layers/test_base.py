from connectome.engine import FunctionEdge
from connectome.layer import EdgeContainer, Chain, Node


def test_basic_chain():
    x = Node('x')
    y = Node('x')
    layer = EdgeContainer([x], [y], [FunctionEdge(lambda x: x + 1, 1).bind(x, y)], None)
    assert layer._compile('x')(0) == 1
    # TODO: return a list?
    assert layer._compile(['x'])(0) == (1,)

    layer = Chain(layer, layer, layer)
    assert layer._compile('x')(0) == 3
    # TODO: return a list?
    assert layer._compile(['x'])(0) == (3,)
