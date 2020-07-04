import inspect
from functools import wraps
from collections import defaultdict

from threading import Lock


def extract_signature(func):
    res = []
    signature = inspect.signature(func)
    for parameter in signature.parameters.values():
        assert parameter.default == parameter.empty, parameter
        assert parameter.kind == parameter.POSITIONAL_OR_KEYWORD, parameter

        res.append(parameter.name)

    return res


def atomize(mutex: Lock):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with mutex:
                result = func(*args, **kwargs)
            return result

        return wrapper

    return decorator


def count_duplicates(sequence):
    counts = defaultdict(int)
    for x in sequence:
        counts[x] += 1

    return counts
