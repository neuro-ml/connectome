import inspect
from functools import wraps
from typing import Sequence, Callable

from .edges import FunctionEdge, ValueEdge, IdentityEdge, InitEdge, AttrGetterEdge
from .engine import Node
from .utils import extract_signature


# this class will play the role of `self` inside init
class SelfMock:
    def __init__(self, allowed_names):
        self.allowed_names = allowed_names
        self.scope = {}

    # TODO: __getattribute__
    def __getattr__(self, item):
        return self.scope[item]

    def __setattr__(self, key, value):
        assert key in self.allowed_names
        self.scope[key] = value


class DecoratorAdapter(object):
    name = None

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        self.instance = instance
        return self.func

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class InverseDecoratorAdapter(DecoratorAdapter):
    name = 'inverse'


def inverse(func: Callable):
    return wraps(func)(InverseDecoratorAdapter(func))


def is_private(name: str):
    return name.startswith('_')


def is_constant(name: str, value):
    return is_private(name) and not isinstance(value, staticmethod)


def is_parameter(name: str, value):
    return is_private(name) and isinstance(value, staticmethod)


def is_output(name: str, value):
    return not is_private(name) and isinstance(value, staticmethod)


def is_backward(name, value):
    return not is_private(name) and isinstance(value, staticmethod) \
           and isinstance(value.__func__, InverseDecoratorAdapter)


def unwrap_transform(value):
    assert isinstance(value, staticmethod)
    value = value.__func__
    while isinstance(value, DecoratorAdapter):
        value = value.__func__
    return value


INIT_NAME = '__init__'
ALLOWED_MAGIC = {'__module__', '__qualname__'}


class GraphFactory:
    def __init__(self, scope):
        self.scope = scope
        # layer inputs
        self.inputs = {}
        self.backward_inputs = {}
        # layer outputs
        self.outputs = {}
        self.backward_outputs = {}
        # __init__ arguments
        self.arguments = {}
        # their defaults
        self.defaults = {}
        # outputs of transform parameters
        self.parameters = {}
        # placeholders for constant parameters
        self.constants = {}

        self._validate()
        self._collect_nodes()

    def _validate(self):
        # e.g. check allowed magic here
        # or names and values
        raise NotImplementedError

    def _add_forward_inputs(self, args: Sequence[str]):
        # adds nodes to inputs and constants
        raise NotImplementedError

    def _func_to_edge(self, func) -> FunctionEdge:
        raise NotImplementedError

    def _add_private(self, name):
        assert is_private(name)
        if name not in self.parameters:
            self.constants[name] = Node(name)

    def _collect_nodes(self):
        # gather outputs, parameters and defaults
        for name, value in self.scope.items():
            if name.startswith('__'):
                continue

            if is_parameter(name, value):
                self.parameters[name] = Node(name)

            elif is_constant(name, value):
                self.constants[name] = Node(name)
                self.defaults[name] = value

            elif is_output(name, value):
                self.outputs[name] = Node(name)

            elif is_backward(name, value):
                self.backward_outputs[name] = Node(name)

            else:
                raise RuntimeError(name)

        # gather constants and inputs
        for name, value in self.scope.items():
            if is_parameter(name, value):
                value = unwrap_transform(value)
                for arg in extract_signature(value):
                    if is_private(arg):
                        self._add_private(arg)
                    else:
                        self.inputs[arg] = Node(arg)

            elif is_backward(name, value):
                value = unwrap_transform(value)
                first_arg, *args = extract_signature(value)
                # TODO: check first arg
                self.backward_inputs[name] = Node(name)
                for arg in args:
                    self._add_private(arg)

            elif is_output(name, value):
                value = unwrap_transform(value)
                self._add_forward_inputs(value)

        # gather defaults
        for name, value in self.scope.items():
            if name.startswith('__'):
                continue

            if name.startswith('_') and not isinstance(value, staticmethod):
                self.constants[name] = Node(name)
                self.defaults[name] = value

        # gather arguments
        if INIT_NAME in self.scope:
            assert not self.defaults
            params = list(inspect.signature(self.scope[INIT_NAME]).parameters.values())
            for param in params:
                assert param.kind == param.POSITIONAL_OR_KEYWORD

            for param in params[1:]:
                name = param.name
                self.arguments[name] = Node(name)
                self.defaults[name] = param.default

        else:
            for name in self.constants:
                name = name.lstrip('_')
                self.arguments[name] = Node(name)
                self.defaults.setdefault(name, inspect.Parameter.empty)

        assert not set(self.constants) & set(self.parameters)
        assert set(self.arguments) == set(self.defaults)

    def _get_constant_edges(self, arguments: dict, this: SelfMock):
        for name, value in arguments.items():
            yield ValueEdge(self.arguments[name], value)

        # if no __init__ was provided there is an identity edge from args to constants
        if this is None:
            assert INIT_NAME not in self.scope
            for name in self.constants:
                yield IdentityEdge(self.arguments[name], self.constants[name])

        # otherwise the constants are extracted from self
        else:
            self_node = Node('$self')
            init = self.scope[INIT_NAME]
            # TODO: the inputs must be sorted check it somewhere
            yield InitEdge(init, this, [self.arguments[k] for k in sorted(self.arguments)], self_node)
            for name in self.constants:
                yield AttrGetterEdge(name, self_node, self.constants[name])

    def get_init_signature(self):
        return inspect.Signature([inspect.Parameter('.0', inspect.Parameter.POSITIONAL_ONLY)] + [
            inspect.Parameter(name.lstrip('_'), inspect.Parameter.KEYWORD_ONLY, default=value)
            for name, value in self.defaults.items()
        ])

    def build(self, arguments: dict, this: SelfMock):
        edges = list(self._get_constant_edges(arguments, this))

        for name, value in self.scope.items():
            # TODO: generate edges
            ...
