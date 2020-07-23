import inspect
from functools import wraps
from collections import Counter


class MultiDict(dict):
    def items(self):
        for key, values in super().items():
            for value in values:
                yield key, value

    def __setitem__(self, key, value):
        if key not in self:
            container = []
            super().__setitem__(key, container)
        else:
            container = super().__getitem__(key)

        container.append(value)

    def __getitem__(self, key):
        container = super().__getitem__(key)
        assert len(container) == 1
        return container[0]


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


# TODO add error message
def check_for_duplicates(collection):
    counts: dict = Counter(list(collection))
    assert not any(v > 1 for k, v in counts.items())


def node_to_dict(nodes):
    return {node.name: node for node in nodes}
