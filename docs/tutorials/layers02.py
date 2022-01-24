from pathlib import Path

import imageio
from skimage.transform import rescale
from dpipe.im.box import mask2bounding_box
from dpipe.im import crop_to_box
from connectome import Source, meta, Transform


class HeLa(Source):
    _root: str

    def _base(_root):
        return Path(_root)

    @meta
    def ids(_base):
        return sorted({str(f.relative_to(_base)) for f in _base.glob('*/*.tif')})

    def image(key, _base):
        return imageio.imread(_base / key)

    def mask(key, _base):
        path = key.replace('/t', '_ST/SEG/man_seg')
        return imageio.imread(_base / path)


class Binarize(Transform):
    # do I hear an echo here?
    def image(image):
        return image

    def mask(mask):
        return mask > 0


class Zoom(Transform):
    _factor: int

    def image(image, _factor):
        return rescale(image.astype(float), _factor, order=1)

    def mask(mask, _factor):
        smooth = rescale(mask.astype(float), _factor, order=1)
        # the output will be a float ndarray, we need to convert it back to bool
        return smooth >= 0.5


class Crop(Transform):
    def _box(image):
        return mask2bounding_box(image < 120)

    def image(image, _box):
        return crop_to_box(image, _box)

    def mask(mask, _box):
        return crop_to_box(mask, _box)
