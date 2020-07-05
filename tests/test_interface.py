from connectome.interface import Source, Transform, Chain


class SomeDS(Source):
    _some_constant = 1

    @staticmethod
    def image(i, _some_constant):
        return f'image, {_some_constant}: {i}'

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


def test_single():
    pipeline = SomeDS(_some_constant=2)
    cc = Crop()
    assert pipeline.image(id='123123') == 'image, 2: 123123'
    assert cc.image(image='input') == f'input transformed 5'


def test_chain():
    pipeline = Chain(
        SomeDS(_some_constant=2),
        Crop(),
    )
    assert pipeline.image(id='123123') == f'image, 2: 123123 transformed 5'
