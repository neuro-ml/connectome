from typing import Dict, Any

from .base import CallableLayer
from ..engine.edges import FunctionEdge, IdentityEdge, ConstantEdge, ComputableHashEdge, ImpureFunctionEdge
from ..exceptions import GraphError, FieldError
from ..containers.transform import TransformContainer
from ..utils import extract_signature, MultiDict
from .prepared import ComputableHash, Prepared
from .decorators import Meta, Optional, RuntimeAnnotation, Impure
from .nodes import *

logger = logging.getLogger(__name__)


def unwrap_transform(value):
    decorators = []
    while isinstance(value, FactoryAnnotation):
        decorators.append(type(value))
        value = value.__func__

    if len(set(decorators)) != len(decorators):
        raise ValueError('Object has duplicated decorators')

    return value, set(decorators)


SILENT_MAGIC = {'__module__', '__qualname__', '__annotations__'}
DOC_MAGIC = '__doc__'


class FactoryLayer(CallableLayer):
    pass


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
        self.magic_dispatch: Dict[str, Callable[[Any], Any]] = {DOC_MAGIC: self._process_doc}
        self.prepared_dispatch: Dict[type, Callable] = {}
        self._nodes_dispatch = {
            Input: self.inputs, Output: self.outputs, Parameter: self.parameters,
            InverseInput: self.backward_inputs, InverseOutput: self.backward_outputs,
        }
        self._before_collect()
        self._collect_nodes()
        self._after_collect()

    def _before_collect(self):
        pass

    def _after_collect(self):
        pass

    def _validate_inputs(self, inputs: NodeTypes) -> NodeTypes:
        raise NotImplementedError

    def _process_doc(self, doc):
        self.docstring = doc

    def _type_to_node(self, x):
        name = x.name
        container = self._nodes_dispatch[type(x)]
        if container.frozen and name not in container:
            assert container == self.parameters
            raise FieldError(f'The parameter `{name}` is not defined.')

        return container[name]

    # TODO: need an adapters' preprocessor
    def _collect_nodes(self):
        # deal with magic
        private = {}
        for name, group in self.scope.groups():
            if name.startswith('__'):
                if len(group) > 1:
                    raise GraphError(f'Magic name "{name}" got multiple definitions')
                value, = group

                if name in SILENT_MAGIC:
                    continue
                if name not in self.magic_dispatch:
                    raise RuntimeError(f'Unrecognized magic method "{name}"')

                self.magic_dispatch[name](value)

            elif is_private(name):
                private[name] = list(group)

        # gather private fields from annotations
        # TODO: detect duplicates
        for name, value in self.scope.get('__annotations__', [{}])[0].items():
            if not is_private(name):
                raise FieldError(f'Only private fields can be defined via type annotations ({name})')
            if name not in private:
                private[name] = [inspect.Parameter.empty]
            # TODO: else make sure it's a constant parameter

        constants = set()
        for name, group in private.items():
            if len(group) > 1:
                raise GraphError(f'Private field "{name}" got multiple definitions')
            value, = group

            self.parameters.add(name)
            if not is_callable(value) or value is inspect.Parameter.empty:
                constants.add(name)
                arg_name = to_argument(name)
                self.edges.append(IdentityEdge().bind(self.arguments[arg_name], self.parameters[name]))
                self.defaults[arg_name] = value

        self.parameters.freeze()
        self.arguments.freeze()
        assert set(self.arguments) == set(self.defaults), (self.arguments, self.defaults)

        # gather, inputs, outputs and their edges
        for name, value in self.scope.items():
            if name.startswith('__') or name in constants:
                continue

            if isinstance(value, Prepared):
                kind = type(value)
                if kind not in self.prepared_dispatch:
                    raise RuntimeError(f'Unrecognized "prepared" method: "{name}"')
                self.edges.extend(self.prepared_dispatch[kind](name, value))
                continue

            if not is_callable(value):
                raise FieldError(f'The type of the "{name}" field cannot be determined.')

            func, decorators = unwrap_transform(value)
            inputs, output, decorators = infer_nodes(name, func, decorators)
            inputs = list(map(self._type_to_node, self._validate_inputs(inputs)))
            output = self._type_to_node(output)

            # runtime stuff
            if Meta in decorators:
                # TODO: check duplicates
                self.property_names.add(name)
                decorators.remove(Meta)
            if Optional in decorators:
                self.optional_names.add(name)
                decorators.remove(Optional)
            assert not any(issubclass(x, RuntimeAnnotation) for x in decorators)

            if Impure in decorators:
                edge = ImpureFunctionEdge(func, len(inputs))
                decorators.remove(Impure)
            else:
                edge = FunctionEdge(func, len(inputs))

            assert not decorators, decorators
            self.edges.append(edge.bind(inputs, output))

        for x in [self.inputs, self.outputs, self.backward_inputs, self.backward_outputs]:
            x.freeze()

        # TODO: each output node must be associated to exactly 1 edge

    def _get_constant_edges(self, arguments: dict):
        for name, value in arguments.items():
            yield ConstantEdge(value).bind([], self.arguments[name])

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

        def __repr__(self):
            kls = type(self)
            if hasattr(kls, '__qualname__'):
                args = ','.join(factory.arguments)
                return f'{kls.__qualname__}({args})'
            return super(type(self), self).__repr__()

        __init__.__signature__ = signature
        scope = {
            '__init__': __init__, '__signature__': signature, '__repr__': __repr__,
            DOC_MAGIC: factory.docstring,
        }
        qualname = namespace.get('__qualname__', [None])[0]
        if qualname is not None:
            scope['__qualname__'] = qualname
        module = namespace.get('__module__', [None])[0]
        if module is not None:
            scope['__module__'] = module
        return scope

    def build(self, arguments: dict) -> TransformContainer:
        diff = list(set(self.arguments) - set(arguments))
        if diff:
            raise ValueError(f'Missing required arguments: {diff}.')

        logger.info(
            'Compiling layer. Inputs: %s, Outputs: %s, BackwardInputs: %s, BackwardOutputs: %s',
            list(self.inputs), list(self.outputs), list(self.backward_inputs), list(self.backward_outputs),
        )
        return TransformContainer(
            list(self.inputs.values()), list(self.outputs.values()),
            self.edges + list(self._get_constant_edges(arguments)),
            list(self.backward_inputs.values()), list(self.backward_outputs.values()),
            optional_nodes=tuple(self.optional_names), virtual_nodes=self.inherited_names,
            persistent_nodes=tuple(self.persistent_names),
        )


class SourceFactory(GraphFactory):
    def _before_collect(self):
        self._key_name = 'id'
        if self._key_name in self.scope:
            raise FieldError(f'Cannot override the key attribute ({self._key_name})')

        self.ID = self.inputs[self._key_name]
        self.inputs.freeze()
        self.edges.append(IdentityEdge().bind(self.ID, self.outputs[self._key_name]))
        self.persistent_names.add(self._key_name)
        self.prepared_dispatch[ComputableHash] = self._process_precomputed

    def _after_collect(self):
        self.persistent_names.update(self.property_names)

    def _validate_inputs(self, inputs: NodeTypes) -> NodeTypes:
        ids = {x for x in inputs if isinstance(x, Input)}
        if not ids:
            return inputs
        if len(ids) > 1:
            raise TypeError(f'Trying to use multiple arguments as keys: {(x.name for x in ids)}')
        i, = ids
        return [x if x != i else Input(self._key_name) for x in inputs]

    def _process_precomputed(self, name, value: ComputableHash):
        inputs = signature_to_types(list(inspect.signature(value.precompute).parameters.values()), Input, name)
        assert len(extract_signature(value.func)[0]) == 1
        inputs = list(map(self._type_to_node, self._validate_inputs(inputs)))

        aux = Node('$aux')
        yield ComputableHashEdge(value.precompute, len(inputs)).bind(inputs, aux)
        yield FunctionEdge(value.func, 1).bind(aux, self.outputs[name])


class TransformFactory(GraphFactory):
    def _before_collect(self):
        self.magic_dispatch['__inherit__'] = self._process_inherit

    def _validate_inputs(self, inputs: NodeTypes) -> NodeTypes:
        return inputs

    def _process_inherit(self, value):
        if isinstance(value, str):
            value = [value]

        if isinstance(value, bool):
            invalid = not value
        else:
            value = tuple(value)
            invalid = not all(isinstance(node_name, str) for node_name in value)
        if invalid:
            raise ValueError(f'"__inherit__" can be either True, or a sequence of strings, got {value}')

        self.inherited_names = value
