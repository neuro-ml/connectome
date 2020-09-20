from connectome import Source, Transform, Chain
from connectome.interface.decorators import insert
from connectome.layers.base import INHERIT_ALL


class DS(Source):
    @staticmethod
    def image(i):
        return i


# here `image` is a required input that has no output, but must be inherited
class Some(Transform):
    __inherit__ = INHERIT_ALL

    @staticmethod
    @insert
    def shape(image):
        return image.shape

    @staticmethod
    @insert
    def some_false():
        return False


def test_chain():
    ds = Chain(
        DS(),
        Some(),
    )
    assert ds.image('id') == 'id'
