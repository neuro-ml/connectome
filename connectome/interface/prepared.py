from typing import Callable


class Prepared:
    pass


class ComputableHash(Prepared):
    def __init__(self, precompute: Callable, func: Callable):
        self.precompute = precompute
        self.func = func
