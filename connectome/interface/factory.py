import inspect

from ..engine.edges import FunctionEdge, IdentityEdge, ValueEdge
from ..engine.base import Node, BoundEdge
from ..layers.base import EdgesBag, INHERIT_ALL
from ..utils import extract_signature, MultiDict
from .decorators import DecoratorAdapter, InverseDecoratorAdapter, OptionalDecoratorAdapter, InsertDecoratorAdapter


class NodeStorage(dict):
    def __init__(self):
        super().__init__()
        self.frozen = False

    def add(self, name):
        assert isinstance(name, str)
        if name not in self:
            assert not self.frozen
            super().__setitem__(name, Node(name))

    def freeze(self):
        self.frozen = True

    def __getitem__(self, name):
        self.add(name)
        return super().__getitem__(name)

    def __setitem__(self, key, value):
        raise ValueError


def is_private(name: str):
    return name.startswith('_')


def is_constant(name: str, value):
    return is_private(name) and not isinstance(value, staticmethod)


def is_parameter(name: str, value):
    return is_private(name) and isinstance(value, staticmethod)


def is_forward(name: str, value):
    return (
            not is_private(name)
            # TODO: enforce staticmethod
            and isinstance(value, staticmethod)
            and InverseDecoratorAdapter not in get_decorators(value)
    )


def is_backward(name: str, value):
    return (
            not is_private(name)
            and isinstance(value, staticmethod)
            and InverseDecoratorAdapter in get_decorators(value)
    )


def get_decorators(value):
    assert isinstance(value, staticmethod)
    value = value.__func__
    decorators = []
    while isinstance(value, DecoratorAdapter):
        decorators.append(type(value))
        value = value.__func__
    return decorators


def to_argument(name):
    assert name.startswith('_')
    return name[1:]


def unwrap_transform(value):
    assert isinstance(value, staticmethod)
    value = value.__func__
    while isinstance(value, DecoratorAdapter):
        value = value.__func__
    return value


INIT_NAME = '__init__'
ALLOWED_MAGIC = {'__module__', '__qualname__', '__inherit__'}


class GraphFactory:
    def __init__(self, scope: MultiDict):
        # TODO: add support
        assert INIT_NAME not in scope

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
        self.defaults = {}
        # outputs of transform parameters
        self.parameters = NodeStorage()
        # placeholders for constant parameters
        self.constants = NodeStorage()
        # names of optional nodes
        self.optional_node_names = []
        # names of inherited nodes
        self.inherited_node_names = []
        # names of persistent nodes
        # TODO move it somewhere
        self.persistent_node_names = ['ids', 'id']
        self._init()
        self._validate()
        self._collect_nodes()

    def _init(self):
        pass

    def _validate(self):
        # e.g. check allowed magic here
        # or names and values
        raise NotImplementedError

    def _process_inherit(self, value):
        raise NotImplementedError

    def _process_parameter(self, name, value) -> BoundEdge:
        raise NotImplementedError

    def _process_forward(self, name, value) -> BoundEdge:
        raise NotImplementedError

    def _process_backward(self, name, value) -> BoundEdge:
        value = unwrap_transform(value)
        first, *names = extract_signature(value)
        # TODO: check first name
        inputs = [self.backward_inputs[name]] + list(map(self._get_private, names))
        return BoundEdge(FunctionEdge(value, len(inputs)), inputs, self.backward_outputs[name])

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
            # TODO: move to validate?
            if name.startswith('__'):
                if name == '__inherit__':
                    self._process_inherit(value)

            elif is_parameter(name, value):
                self.edges.append(self._process_parameter(name, value))

            elif is_forward(name, value):
                self.edges.append(self._process_forward(name, value))

            elif is_backward(name, value):
                self.edges.append(self._process_backward(name, value))

            elif is_constant(name, value):
                self.constants.add(name)
                self.defaults[to_argument(name)] = value

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
                name = to_argument(name)
                self.arguments.add(name)
                self.defaults.setdefault(name, inspect.Parameter.empty)

        self.arguments.freeze()
        assert not set(self.constants) & set(self.parameters)
        assert set(self.arguments) == set(self.defaults), (self.arguments, self.defaults)

    def _get_constant_edges(self, arguments: dict):
        for name, value in arguments.items():
            yield BoundEdge(ValueEdge(value), [], self.arguments[name])

        # if no __init__ was provided there is an identity edge from args to constants
        assert not self.has_init()
        for name in self.constants:
            yield BoundEdge(IdentityEdge(), [self.arguments[to_argument(name)]], self.constants[name])

        # otherwise the constants are extracted from self
        # else:
        # self_node = Node('$self')
        # init = self.scope[INIT_NAME]
        # # TODO: the inputs must be sorted. check it somewhere
        # yield InitEdge(init, self.mock_storage, [self.arguments[k] for k in sorted(self.arguments)], self_node)
        # for name in self.constants:
        #     yield ItemGetterEdge(name, self_node, self.constants[name])

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
            inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, default=value)
            for name, value in self.defaults.items()
        ])

    def build(self, arguments: dict):
        diff = list(set(self.arguments) - set(arguments))
        if diff:
            raise ValueError(f'Missing required arguments: {diff}.')

        if self.has_init():
            self.scope[INIT_NAME](self.get_self(), **arguments)
        self.edges.extend(self._get_constant_edges(arguments))

    def get_layer(self):
        return EdgesBag(
            list(self.inputs.values()), list(self.outputs.values()), self.edges,
            list(self.backward_inputs.values()), list(self.backward_outputs.values()),
            self.optional_node_names,
            self.inherited_node_names,
            self.persistent_node_names
        )


class SourceFactory(GraphFactory):
    def _init(self):
        self.ID = self.inputs['id']
        self.inputs.freeze()
        self.edges.append(BoundEdge(IdentityEdge(), [self.ID], self.outputs['id']))

    def _validate(self):
        # TODO: ids, id, magic
        # require docstring
        pass

    def _process_inherit(self, value):
        raise ValueError

    def _process_forward(self, name, value) -> BoundEdge:
        value = unwrap_transform(value)
        names = extract_signature(value)

        inputs = []
        # FIXME: for now only ids is allowed to have no arguments
        if name != 'ids':
            first, *names = names
            inputs.append(self._get_private(first) if is_private(first) else self.ID)

        inputs.extend(map(self._get_private, names))
        return BoundEdge(FunctionEdge(value, len(inputs)), inputs, self.outputs[name])

    def _process_parameter(self, name, value) -> BoundEdge:
        value = unwrap_transform(value)
        first, *names = extract_signature(value)
        inputs = [self._get_private(first) if is_private(first) else self.ID]
        inputs.extend(map(self._get_private, names))
        return BoundEdge(FunctionEdge(value, len(inputs)), inputs, self.parameters[name])


class TransformFactory(GraphFactory):
    def _validate(self):
        pass

    def _process_inherit(self, value):
        if isinstance(value, str):
            value = [value]

        if value != INHERIT_ALL:
            for node_name in value:
                # TODO exception
                assert isinstance(node_name, str)

        self.inherited_node_names = value

    def _process_forward(self, name, value) -> BoundEdge:
        decorators = get_decorators(value)
        if OptionalDecoratorAdapter in decorators:
            self.optional_node_names.append(name)

        value = unwrap_transform(value)

        inputs = []
        names = extract_signature(value)
        if InsertDecoratorAdapter not in decorators:
            if not names:
                raise ValueError('Non-insert transform methods require at least one argument')

            inputs.append(self.inputs[name])
            names = names[1:]

        inputs.extend(
            self._get_private(arg) if is_private(arg) else self.inputs[arg]
            for arg in names
        )
        return BoundEdge(FunctionEdge(value, len(inputs)), inputs, self.outputs[name])

    def _process_parameter(self, name, value) -> BoundEdge:
        value = unwrap_transform(value)
        inputs = [
            self._get_private(arg) if is_private(arg) else self.inputs[arg]
            for arg in extract_signature(value)
        ]
        return BoundEdge(FunctionEdge(value, len(inputs)), inputs, self.parameters[name])
