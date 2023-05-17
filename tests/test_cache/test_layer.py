from connectome import CacheToRam, Transform


def test_nested_virtual():
    a = Transform(
        a=lambda x: x,
        b=lambda x: x,
        c=lambda x: x,
    )
    b = Transform(
        __inherit__=True,
        d=lambda a: a,
    )
    c = CacheToRam('d')

    assert set(dir(a >> b >> c)) == set('abcd')
    assert set(dir(a >> (b >> c))) == set('abcd')
