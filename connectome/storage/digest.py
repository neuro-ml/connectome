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


def digest_to_relative(key: str, levels: Sequence[int]):
    # TODO: too expensive?
    assert len(key) == sum(levels) * 2, len(key)

    parts = []
    start = 0
    for level in levels:
        stop = start + level * 2
        parts.append(key[start:stop])
        start = stop

    return Path(*parts)
