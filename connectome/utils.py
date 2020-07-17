import inspect
from functools import wraps
from collections import defaultdict


# TODO: better keep all in lists, override `items` and `getitem`
class MultiDict(dict):
    def __setitem__(self, key, new_value):
        if key in self:
            cur_value = self[key]
            if isinstance(cur_value, list):
                cur_value.append(new_value)
            else:
                super().__setitem__(key, [cur_value, new_value])
        else:
            super().__setitem__(key, new_value)


class DecoratorAdapter(object):
    name = None

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        self.instance = instance
        return self.func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


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


# TODO add error message
def check_for_duplicates(collection):
    counts: dict = count_duplicates([x for x in collection])
    assert not any(v > 1 for k, v in counts.items())


def node_to_dict(nodes):
    return {node.name: node for node in nodes}
