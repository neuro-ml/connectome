from pathlib import Path
from typing import Sequence


def digest_file(path: Path, algorithm, block_size=2 ** 20) -> str:
    hasher = algorithm()

    with open(path, 'rb') as f:
        while True:
            buffer = f.read(block_size)
            if not buffer:
                break
            hasher.update(buffer)

    return hasher.hexdigest()


def key_to_relative(key: str, levels: Sequence[int]):
    # TODO: too expensive?
    assert len(key) == get_digest_size(levels, string=True), len(key)

    parts = []
    start = 0
    for level in levels:
        stop = start + level * 2
        parts.append(key[start:stop])
        start = stop

    return Path(*parts)


digest_to_relative = key_to_relative


def get_digest_size(levels, string: bool):
    size = sum(levels)
    if string:
        size *= 2
    return size
