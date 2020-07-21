import re
import pytest

from connectome.interface.base import Source, Transform
from connectome.interface.decorators import inverse


class FirstDS(Source):
    _first_constant = 1
    _ids_arg = 4

    @staticmethod
    def ids(_ids_arg):
        return list(range(_ids_arg))

    @staticmethod
    def image(i, _first_constant):
        return f'image, {_first_constant}: {i}'

    @staticmethod
    def lungs(i):
        return f'lungs: {i}'

    @staticmethod
    def spacing(i):
        return f'spacing: {i}'


class SecondDS(Source):
    _ids_arg = 4
    _second_constant = 3

    @staticmethod
    def ids(_ids_arg):
        return [str(i) for i in range(_ids_arg)]

    @staticmethod
    def image(i, _second_constant):
        return f'second_ds_{_second_constant}_' + i

    @staticmethod
    def lungs(i):
        return f'lungs: {i}'

    @staticmethod
    def spacing(i):
        return f'spacing: {i}'


class Crop(Transform):
    @staticmethod
    def _size(image):
        return len(image)

    @staticmethod
    def image(x, _size):
        return x + f' transformed {_size}'

    spacing = lungs = image

    @staticmethod
    @inverse
    def image(x, _size):
        return re.sub(f' transformed {_size}', '', x)


class Zoom(Transform):
    _spacing = None

    @staticmethod
    def image(x, _spacing):
        return str(x + _spacing)

    spacing = lungs = image

    @staticmethod
    @inverse
    def image(x, _spacing):
        return int(x) - _spacing


class BlockBuilder:
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


@pytest.fixture(scope='module')
def block_builder():
    return BlockBuilder
