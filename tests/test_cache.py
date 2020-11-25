from tempfile import TemporaryDirectory

from connectome import Chain, CacheToRam, CacheToDisk
from connectome.interface.blocks import CacheRows
from connectome.storage.local import DiskOptions


def test_hash(block_maker, hash_layer):
    pipeline = Chain(
        block_maker.first_ds(first_constant=2, ids_arg=15),
        block_maker.crop(),
    )

    hashed = Chain(pipeline, hash_layer)
    ram = Chain(pipeline, CacheToRam(['image']), hash_layer)
    with TemporaryDirectory() as root, TemporaryDirectory() as storage:
        storage = DiskOptions(storage)
        disk = Chain(pipeline, CacheToDisk(root, storage, names=['image']), hash_layer)
        rows = Chain(pipeline, CacheRows(root, storage, names=['image']), hash_layer)

        rows.image(pipeline.ids[0])
        for i in pipeline.ids:
            assert hashed.image(i) == ram.image(i) == disk.image(i) == rows.image(i)
