from connectome.blocks import FunctionEdge, CustomLayer
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
