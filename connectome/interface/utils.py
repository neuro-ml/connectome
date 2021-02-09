from typing import Union, Sequence, Generic, TypeVar

MaybeStr = Union[Sequence[str], str]
T = TypeVar('T')


class Local(Generic[T]):
    pass
