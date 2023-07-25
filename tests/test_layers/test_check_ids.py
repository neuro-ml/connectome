import pytest

from connectome import Chain, Filter, HashDigest
from connectome.exceptions import FieldError
from connectome.layers.check_ids import CheckIds


def test_check_ids(block_maker):
    block = block_maker.first_ds(first_constant=2, ids_arg=15)
    pipeline = Chain(
        block, Filter(lambda id: id in ['2', '5']), CheckIds()
    )
    for i in range(1, 20):
        i = str(i)
        if i in ['2', '5']:
            pipeline.image(i)
        else:
            with pytest.raises(KeyError):
                pipeline.image(i)

    hash_layer = HashDigest(['image', 'lungs', 'spacing'], 'blake2b', return_value=True)
    hashed = Chain(block, hash_layer)
    pipeline = Chain(
        block, CheckIds(), hash_layer,
    )
    for i in pipeline.ids:
        assert hashed.image(i) == pipeline.image(i)
        assert hashed.lungs(i) == pipeline.lungs(i)
        assert hashed.spacing(i) == pipeline.spacing(i)
