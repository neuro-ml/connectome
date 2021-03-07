import inspect

from ..engine.edges import FunctionEdge, IdentityEdge, ConstantEdge
from ..engine.base import Node, BoundEdge
from ..layers.transform import TransformLayer
from ..utils import extract_signature, MultiDict
from .decorators import DecoratorAdapter, InverseDecoratorAdapter, OptionalDecoratorAdapter, InsertDecoratorAdapter, \
    PositionalDecoratorAdapter, PropertyDecoratorAdapter
from .utils import Local


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


def is_callable(value):
    return callable(value) or isinstance(value, DecoratorAdapter)


def is_constant(name: str, value):
    return is_private(name) and not is_callable(value)


def is_parameter(name: str, value):
    return is_private(name) and is_callable(value)


def is_forward(name: str, value):
    return (
            not is_private(name)
            and is_callable(value)
            and InverseDecoratorAdapter not in get_decorators(value)
    )


def is_backward(name: str, value):
    return (
            not is_private(name)
            and is_callable(value)
            and InverseDecoratorAdapter in get_decorators(value)
    )


def is_property(value):
    return PropertyDecoratorAdapter in get_decorators(value)


def is_local(annotation):
    return annotation is Local or isinstance(annotation, Local)


def get_decorators(value):
    decorators = []
    while isinstance(value, DecoratorAdapter):
        decorators.append(type(value))
        value = value.__func__
    return decorators


def to_argument(name):
    assert name.startswith('_')
    return name[1:]


def unwrap_transform(value):
    while isinstance(value, DecoratorAdapter):
        value = value.__func__
    return value


SILENT_MAGIC = {'__module__', '__qualname__', '__annotations__'}


class GraphFactory:
    def __init__(self, scope: MultiDict):
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
        self.optional_names = set()
        # names of inherited nodes
        self.inherited_names = set()
        # metadata
        self.property_names = set()
        # names of persistent nodes
        # TODO move it somewhere
        self.persistent_names = {'ids', 'id'}
        self.docstring = None
        self.magic_dispatch = {'__doc__': self._process_doc}
        self._init()
        self._collect_nodes()
        self._validate()

    @staticmethod
    def _remove_staticmethod(value):
        if isinstance(value, staticmethod):
            value = value.__func__
        return value

    def _init(self):
        pass

    def _validate(self):
        # e.g. check allowed magic here
        # or names and values
        pass

    def _process_parameter(self, name, value) -> BoundEdge:
        raise NotImplementedError

    def _process_forward(self, name, value) -> BoundEdge:
        raise NotImplementedError

    def _process_backward(self, name, value) -> BoundEdge:
        raise NotImplementedError

    def _process_doc(self, doc):
        self.docstring = doc

    def _get_private(self, name):
        assert is_private(name)
        if name in self.parameters:
            return self.parameters[name]
        return self.constants[name]

    def _collect_nodes(self):
        # gather parameters
        for name, value in self.scope.items():
            value = self._remove_staticmethod(value)
            if is_parameter(name, value):
                self.parameters.add(name)

        self.parameters.freeze()
        # gather, inputs, outputs and their edges
        for name, value in self.scope.items():
            value = self._remove_staticmethod(value)

            if name.startswith('__'):
                if name in SILENT_MAGIC:
                    continue
                if name not in self.magic_dispatch:
                    raise RuntimeError(f'Unrecognized magic method "{name}"')

                self.magic_dispatch[name](value)

            elif is_parameter(name, value):
                self.edges.append(self._process_parameter(name, value))
                if is_property(value):
                    raise TypeError(f'Parameters can\'t also be properties: "{name}".')

            elif is_forward(name, value):
                self.edges.append(self._process_forward(name, value))
                if is_property(value):
                    self.property_names.add(name)

            elif is_backward(name, value):
                self.edges.append(self._process_backward(name, value))
                if is_property(value):
                    self.property_names.add(name)

            elif is_constant(name, value):
                self.constants.add(name)
                self.defaults[to_argument(name)] = value

            else:
                raise RuntimeError(f'The type of the "{name}" edge cannot be determined.')

        for x in [self.constants, self.inputs, self.outputs, self.backward_inputs, self.backward_outputs]:
            x.freeze()

        # gather arguments
        for name in self.constants:
            name = to_argument(name)
            self.arguments.add(name)
            self.defaults.setdefault(name, inspect.Parameter.empty)

        self.arguments.freeze()
        assert not set(self.constants) & set(self.parameters)
        assert set(self.arguments) == set(self.defaults), (self.arguments, self.defaults)

    def _get_constant_edges(self, arguments: dict):
        for name, value in arguments.items():
            yield ConstantEdge(value).bind([], self.arguments[name])

        for name in self.constants:
            yield IdentityEdge().bind(self.arguments[to_argument(name)], self.constants[name])

    def get_init_signature(self):
        return inspect.Signature([
            inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, default=value)
            for name, value in self.defaults.items()
        ])

    @classmethod
    def make_scope(cls, namespace: MultiDict) -> dict:
        factory = cls(namespace)
        signature = factory.get_init_signature()

        def __init__(*args, **kwargs):
            assert args
            if len(args) > 1:
                raise TypeError('This constructor accepts only keyword arguments.')
            self = args[0]

            arguments = signature.bind_partial(**kwargs)
            arguments.apply_defaults()
            super(type(self), self).__init__(factory.build(arguments.arguments), factory.property_names)

        __init__.__signature__ = signature
        scope = {'__init__': __init__}
        if factory.docstring is not None:
            scope['__doc__'] = factory.docstring
        return scope

    def build(self, arguments: dict) -> TransformLayer:
        diff = list(set(self.arguments) - set(arguments))
        if diff:
            raise ValueError(f'Missing required arguments: {diff}.')

        return TransformLayer(
            list(self.inputs.values()), list(self.outputs.values()),
            self.edges + list(self._get_constant_edges(arguments)),
            list(self.backward_inputs.values()), list(self.backward_outputs.values()),
            optional_nodes=tuple(self.optional_names), inherit_nodes=self.inherited_names,
            persistent_nodes=tuple(self.persistent_names),
        )


class SourceFactory(GraphFactory):
    def _init(self):
        self.ID = self.inputs['id']
        self.inputs.freeze()
        self.edges.append(IdentityEdge().bind(self.ID, self.outputs['id']))
        self.property_names.add('ids')

    def _validate(self):
        pass

    def _get_first(self, name, annotations):
        if is_local(annotations[name]):
            # TODO: no need
            raise TypeError('The first argument cannot be local')
        if is_private(name):
            return self._get_private(name)
        return self.ID

    def _get_internal(self, name, annotations):
        if is_private(name):
            return self._get_private(name)
        if not is_local(annotations[name]):
            raise ValueError('Source arguments must be either local or private')
        return self.outputs[name]

    def _process_forward(self, name, value) -> BoundEdge:
        value = unwrap_transform(value)
        names, annotations = extract_signature(value)

        inputs = []
        # FIXME: for now only ids is allowed to have no arguments
        if name != 'ids':
            first, *names = names
            inputs.append(self._get_first(first, annotations))

        inputs.extend(self._get_internal(name, annotations) for name in names)
        return FunctionEdge(value, len(inputs)).bind(inputs, self.outputs[name])

    def _process_parameter(self, name, value) -> BoundEdge:
        value = unwrap_transform(value)
        (first, *names), annotations = extract_signature(value)
        inputs = [self._get_first(first, annotations)]
        inputs.extend(self._get_internal(name, annotations) for name in names)
        return FunctionEdge(value, len(inputs)).bind(inputs, self.parameters[name])


class TransformFactory(GraphFactory):
    def _init(self):
        self.magic_dispatch['__inherit__'] = self._process_inherit

    def _get_input(self, name, annotation, input_nodes):
        # 3 cases here:
        if is_private(name):
            # _private
            return self._get_private(name)
        if is_local(annotation):
            # x: Local
            return self.outputs[name]
        # just an input
        return input_nodes[name]

    def _get_inputs(self, name, value, input_nodes, decorators):
        # we have 2 situations here:
        #  1. all the arguments are positional-or-keyword -> it can be an insertion or a transformation
        #  2. the first argument is positional-only (or marked by a decorator) -> this is a transformation
        inputs = []
        signature = list(inspect.signature(value).parameters.values())
        positional = False
        if signature:
            parameter = signature[0]
            # second case
            assert parameter.default == parameter.empty, parameter
            positional = parameter.kind == parameter.POSITIONAL_ONLY or PositionalDecoratorAdapter in decorators
            if positional:
                signature = signature[1:]
                inputs.append(input_nodes[name])

        # TODO: deprecate
        if InsertDecoratorAdapter in decorators:
            if positional:
                raise ValueError(f"Can't insert using positional arguments.")
            if name in extract_signature(value)[0]:
                raise ValueError(f"Can't insert {name}, the name is already present.")

        for parameter in signature:
            assert parameter.default == parameter.empty, parameter
            assert parameter.kind == parameter.POSITIONAL_OR_KEYWORD, parameter
            inputs.append(self._get_input(parameter.name, parameter.annotation, input_nodes))

        return inputs

    def _process_inherit(self, value):
        if isinstance(value, str):
            value = [value]

        if isinstance(value, bool):
            # TODO exception
            assert value
        else:
            for node_name in value:
                # TODO exception
                assert isinstance(node_name, str)

            value = tuple(value)

        self.inherited_names = value

    def _process_forward(self, name, value) -> BoundEdge:
        decorators = get_decorators(value)
        value = unwrap_transform(value)
        if OptionalDecoratorAdapter in decorators:
            self.optional_names.add(name)

        inputs = self._get_inputs(name, value, self.inputs, decorators)
        return FunctionEdge(value, len(inputs)).bind(inputs, self.outputs[name])

    def _process_backward(self, name, value) -> BoundEdge:
        decorators = get_decorators(value)
        value = unwrap_transform(value)
        inputs = self._get_inputs(name, value, self.backward_inputs, decorators)
        return FunctionEdge(value, len(inputs)).bind(inputs, self.backward_outputs[name])

    def _process_parameter(self, name, value) -> BoundEdge:
        value = unwrap_transform(value)
        names, annotations = extract_signature(value)
        inputs = [self._get_input(arg, annotations[arg], self.inputs) for arg in names]
        return FunctionEdge(value, len(inputs)).bind(inputs, self.parameters[name])
