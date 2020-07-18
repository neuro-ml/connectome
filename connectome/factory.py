import inspect
from functools import wraps
from typing import Callable

from connectome.engine.edges import FunctionEdge, ValueEdge, IdentityEdge, InitEdge, ItemGetterEdge
from .old_engine import Node
from .layers import CustomLayer
from .utils import extract_signature


class NodeStorage(dict):
    def __init__(self):
        super().__init__()
        self.frozen = False

    def add(self, name):
        assert isinstance(name, str)
        assert not self.frozen
        if name not in self:
            super().__setitem__(name, Node(name))

    def freeze(self):
        self.frozen = True

    def __getitem__(self, name):
        self.add(name)
        return super().__getitem__(name)

    def __setitem__(self, key, value):
        raise ValueError


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


def is_forward(name: str, value):
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
        self.edges = []
        # layer inputs
        self.inputs = NodeStorage()
        self.backward_inputs = NodeStorage()
        # layer outputs
        self.outputs = NodeStorage()
        self.backward_outputs = NodeStorage()
        # __init__ arguments
        self.arguments = NodeStorage()
        # their defaults
        self.defaults = NodeStorage()
        # outputs of transform parameters
        self.parameters = NodeStorage()
        # placeholders for constant parameters
        self.constants = NodeStorage()
        # storage for constants defined in __init__
        self.mock_storage = {}

        self._validate()
        self._collect_nodes()

    def _validate(self):
        # e.g. check allowed magic here
        # or names and values
        raise NotImplementedError

    def _process_parameter(self, name, value) -> FunctionEdge:
        raise NotImplementedError

    def _process_forward(self, name, value) -> FunctionEdge:
        raise NotImplementedError

    def _process_backward(self, name, value) -> FunctionEdge:
        value = unwrap_transform(value)
        first, *names = extract_signature(value)
        # TODO: check first name
        inputs = [self.backward_inputs[first]] + list(map(self._get_private, names))
        return FunctionEdge(value, inputs, self.backward_outputs[name])

    def _get_private(self, name):
        assert is_private(name)
        if name in self.parameters:
            return self.parameters[name]
        return self.constants[name]

    def _collect_nodes(self):
        # gather parameters
        for name, value in self.scope.items():
            if is_parameter(name, value):
                self.parameters.add(name)

        self.parameters.freeze()
        # gather, inputs, outputs and their edges
        for name, value in self.scope.items():
            # TODO: func
            if name.startswith('__'):
                continue

            if is_parameter(name, value):
                self.edges.append(self._process_parameter(name, value))

            elif is_forward(name, value):
                self.edges.append(self._process_forward(name, value))

            elif is_backward(name, value):
                self.edges.append(self._process_backward(name, value))

            elif is_constant(name, value):
                self.constants.add(name)
                self.defaults[name] = value

            else:
                raise RuntimeError(name)

        for x in [self.constants, self.inputs, self.outputs, self.backward_inputs, self.backward_outputs]:
            x.freeze()

        # gather arguments
        if self.has_init():
            assert not self.defaults
            params = list(inspect.signature(self.scope[INIT_NAME]).parameters.values())
            for param in params:
                # TODO: not necessarily
                assert param.kind == param.POSITIONAL_OR_KEYWORD

            for param in params[1:]:
                name = param.name
                self.arguments.add(name)
                self.defaults[name] = param.default

        else:
            for name in self.constants:
                name = name.lstrip('_')
                self.arguments.add(name)
                self.defaults.setdefault(name, inspect.Parameter.empty)

        self.arguments.freeze()
        assert not set(self.constants) & set(self.parameters)
        assert set(self.arguments) == set(self.defaults)

    def _get_constant_edges(self, arguments: dict):
        for name, value in arguments.items():
            yield ValueEdge(self.arguments[name], value)

        # if no __init__ was provided there is an identity edge from args to constants
        if not self.has_init():
            for name in self.constants:
                yield IdentityEdge(self.arguments[name], self.constants[name])

        # otherwise the constants are extracted from self
        else:
            self_node = Node('$self')
            init = self.scope[INIT_NAME]
            # TODO: the inputs must be sorted. check it somewhere
            yield InitEdge(init, self.mock_storage, [self.arguments[k] for k in sorted(self.arguments)], self_node)
            for name in self.constants:
                yield ItemGetterEdge(name, self_node, self.constants[name])

    def has_init(self):
        return INIT_NAME in self.scope

    def get_self(self):
        class SelfMock:
            def __getattribute__(self, name):
                return this.mock_storage[name]

            def __setattr__(self, name, value):
                assert name in this.constants
                this.mock_storage[name] = value

        assert self.has_init()
        this = self
        return SelfMock()

    def get_init_signature(self):
        return inspect.Signature([
            inspect.Parameter(name.lstrip('_'), inspect.Parameter.KEYWORD_ONLY, default=value)
            for name, value in self.defaults.items()
        ])

    def build(self, arguments: dict):
        if self.has_init():
            self.scope[INIT_NAME](self.get_self(), **arguments)
        self.edges.extend(self._get_constant_edges(arguments))

    def get_layer(self):
        return CustomLayer(
            list(self.inputs.values()), list(self.outputs.values()), self.edges,
            list(self.backward_inputs.values()), list(self.backward_outputs.values()),
        )


class SourceFactory(GraphFactory):
    def __init__(self, scope):
        super().__init__(scope)
        self.ID = self.inputs['id']
        self.inputs.freeze()

    def _validate(self):
        # TODO: ids, id, magic
        pass

    def _process_forward(self, name, value) -> FunctionEdge:
        value = unwrap_transform(value)
        first, *names = extract_signature(value)
        inputs = [self._get_private(first) if is_private(first) else self.ID]
        inputs.extend(map(self._get_private, names))
        return FunctionEdge(value, inputs, self.outputs[name])

    def _process_parameter(self, name, value) -> FunctionEdge:
        value = unwrap_transform(value)
        first, *names = extract_signature(value)
        inputs = [self._get_private(first) if is_private(first) else self.ID]
        inputs.extend(map(self._get_private, names))
        return FunctionEdge(value, inputs, self.parameters[name])


class TransformFactory(GraphFactory):
    def _validate(self):
        pass

    def _process_forward(self, name, value) -> FunctionEdge:
        value = unwrap_transform(value)
        first, *names = extract_signature(value)
        inputs = [self._get_private(first) if is_private(first) else self.inputs[name]]
        inputs.extend(map(self._get_private, names))
        return FunctionEdge(value, inputs, self.outputs[name])

    def _process_parameter(self, name, value) -> FunctionEdge:
        value = unwrap_transform(value)
        inputs = [
            self._get_private(arg) if is_private(arg) else self.inputs[arg]
            for arg in extract_signature(value)
        ]
        return FunctionEdge(value, inputs, self.parameters[name])
