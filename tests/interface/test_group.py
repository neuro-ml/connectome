from connectome import Source, GroupBy, meta


class DS(Source):
    @meta
    def ids():
        return tuple('0123456789')

    def same(i):
        return i

    def two(i):
        return str(int(i) // 2)

    def three(i):
        return str(int(i) // 3)


def test_group_by():
    ds = DS()
    chain = ds >> GroupBy('two')
    assert chain.ids == tuple('01234')
    assert 'two' not in dir(chain)

    for i in chain.ids:
        n = int(i)
        assert chain.same(i) == {str(n * 2): str(n * 2), str(n * 2 + 1): str(n * 2 + 1)}
        assert chain.three(i) == {str(n * 2): str(n * 2 // 3), str(n * 2 + 1): str((n * 2 + 1) // 3)}

    chain = ds >> GroupBy('same')
    assert chain.ids == ds.ids
    assert 'same' not in dir(chain)

    for i in chain.ids:
        n = int(i)
        assert chain.two(i) == {i: str(n // 2)}
        assert chain.three(i) == {i: str(n // 3)}
