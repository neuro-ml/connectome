from connectome import Transform, impure


def test_node_hash():
    class A(Transform):
        @impure
        def f(x):
            return x

    f = A().f
    assert f({}) == {}
