import re
import pytest

from connectome import Source, Transform, inverse, optional, positional, meta


class FirstDS(Source):
    """Doc"""

    _first_constant = 1
    _ids_arg: int = 4

    @meta
    def ids(_ids_arg):
        return tuple(map(str, range(_ids_arg)))

    def image(i, _first_constant):
        return f'image, {_first_constant}: {i}'

    def lungs(i):
        return f'lungs: {i}'

    def spacing(i):
        return f'spacing: {i}'


class SecondDS(Source):
    _ids_arg = 4
    _second_constant = 3

    @meta
    def ids(_ids_arg):
        return [f'second:{i}' for i in range(_ids_arg)]

    def image(i, _second_constant):
        return f'second_ds_{_second_constant}_' + i

    def lungs(i):
        return f'lungs: {i}'

    def spacing(i):
        return f'spacing: {i}'


class Crop(Transform):
    def _size(image):
        return len(image)

    @positional
    def image(x, _size):
        return x + f' transformed {_size}'

    spacing = lungs = image

    @positional
    @inverse
    def image(x, _size):
        return re.sub(f' transformed {_size}', '', x)

    spacing = lungs = image


class Zoom(Transform):
    _spacing = None

    @positional
    def image(x, _spacing):
        return x + _spacing

    spacing = lungs = image

    @positional
    @inverse
    def image(x, _spacing):
        return x[:-len(_spacing)]

    spacing = lungs = image


class Optional(Transform):
    __inherit__ = ['image', 'spacing', 'lungs']

    @optional
    @positional
    def first_optional(x):
        return x + 1

    @optional
    @positional
    def second_optional(x):
        return str(x)

    @positional
    @inverse
    def first_optional(x):
        return x - 1

    @positional
    @inverse
    def second_optional(x):
        return int(x)


class Identity(Transform):
    __inherit__ = True


class BlockMaker:
    @staticmethod
    def first_ds(**kwargs):
        return FirstDS(**kwargs)

    @staticmethod
    def second_ds(**kwargs):
        return SecondDS(**kwargs)

    @staticmethod
    def zoom(**kwargs):
        return Zoom(**kwargs)

    @staticmethod
    def crop(**kwargs):
        return Crop(**kwargs)

    @staticmethod
    def optional():
        return Optional()

    @staticmethod
    def identity():
        return Identity()


@pytest.fixture(scope='module')
def block_maker():
    return BlockMaker
