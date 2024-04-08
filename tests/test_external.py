import itertools

import pytest

from connectome import Transform, Chain, External, ExternalBase


@pytest.mark.parametrize('fields, properties', itertools.product(
    [None, ['image', 'mask']], [None, ['ids']],
))
def test_external_wrapper(fields, properties):
    class A:
        @property
        def ids(self):
            return tuple('0123')

        def image(self, i):
            return i

        def mask(self, i):
            return i + 'mask'

        @classmethod
        def redundant(cls, i):
            return i

    ds = External(A(), fields=fields, properties=properties, inputs=['id'], inherit=['id'])
    assert 'redundant' not in [x.name for x in ds._container.outputs]
    assert ds.ids == tuple('0123')
    for i in ds.ids:
        assert ds.image(i) == i
        assert ds.mask(i) == i + 'mask'

    ds2 = Chain(
        ds, Transform(image=lambda image: image + 't', mask=lambda mask: mask + 't', my_id=lambda id: id),
    )

    assert ds2.ids == tuple('0123')
    for i in ds2.ids:
        assert ds2.image(i) == i + 't'
        assert ds2.mask(i) == i + 'maskt'
        assert ds2.my_id(i) == i


def test_external_base():
    class A(ExternalBase):
        def __init__(self):
            super().__init__(inputs=['id'], inherit=['id'])

        @property
        def ids(self):
            return tuple('0123')

        def image(self, i):
            return i

        def mask(self, i):
            return i + 'mask'

        @classmethod
        def redundant(cls, i):
            return i

    ds = A()
    assert ds.ids == tuple('0123')
    assert 'redundant' not in [x.name for x in ds._container.outputs]
    for i in ds.ids:
        assert ds.image(i) == i
        assert ds.mask(i) == i + 'mask'

    ds2 = Chain(
        ds, Transform(image=lambda image: image + 't', mask=lambda mask: mask + 't', my_id=lambda id: id),
    )

    assert ds2.ids == tuple('0123')
    for i in ds2.ids:
        assert ds2.image(i) == i + 't'
        assert ds2.mask(i) == i + 'maskt'
        assert ds2.my_id(i) == i

    ds2 = ds >> Transform(image=lambda image: image + 't', mask=lambda mask: mask + 't', my_id=lambda id: id)

    assert ds2.ids == tuple('0123')
    for i in ds2.ids:
        assert ds2.image(i) == i + 't'
        assert ds2.mask(i) == i + 'maskt'
        assert ds2.my_id(i) == i
