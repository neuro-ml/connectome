import inspect
from functools import wraps
from collections import defaultdict


def extract_signature(func):
    res = []
    signature = inspect.signature(func)
    for parameter in signature.parameters.values():
        assert parameter.default == parameter.empty, parameter
        assert parameter.kind == parameter.POSITIONAL_OR_KEYWORD, parameter

        res.append(parameter.name)

    return res


def atomize(attribute: str = '_lock'):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            mutex = getattr(self, attribute)
            if mutex is None:
                return func(self, *args, **kwargs)
            with mutex:
                return func(self, *args, **kwargs)

        return wrapper

    return decorator


def count_duplicates(sequence):
    counts = defaultdict(int)
    for x in sequence:
        counts[x] += 1

    return counts
