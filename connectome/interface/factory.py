import inspect
import logging
from typing import Dict, Any, Iterable, Callable

from ..engine.base import Node
from ..containers.base import EdgesBag
from ..engine.edges import FunctionEdge, IdentityEdge, ConstantEdge, ComputableHashEdge
from ..exceptions import GraphError, FieldError
from ..containers.transform import TransformContainer, normalize_inherit
from ..utils import extract_signature, MultiDict
from .prepared import ComputableHash, Prepared
from .nodes import NodeStorage, Input, InverseInput, Parameter, InverseOutput, Output, NodeTypes
from .base import CallableLayer
from .factory_utils import add_quals, is_private, is_callable, to_argument, unwrap_transform, infer_nodes, \
    signature_to_types
from .decorators import Meta, Optional, RuntimeAnnotation, EdgeAnnotation

logger = logging.getLogger(__name__)


def add_from_mixins(namespace, mixins):
    # we have 2 scopes here:
    # 1. the `namespace`
    # 2. the __annotations__
    # TODO: make annotations a MultiDict
    annotations, = namespace.get(ANN_MAGIC, [{}])

    for mixin in mixins:
        # update without overwriting
        local = mixin.__methods__
        local_annotations, = local.get(ANN_MAGIC, [{}])

        intersection = (set(local) & set(namespace)) - OVERRIDABLE_MAGIC
        intersection |= set(annotations) & set(local_annotations)
        if intersection:
            raise RuntimeError(f'Trying to overwrite the names {intersection} from mixin {mixin}')

        for name in set(local) - OVERRIDABLE_MAGIC:
            for value in local.get(name):
                namespace[name] = value
        for name in local_annotations:
            annotations[name] = local_annotations[name]

    if ANN_MAGIC not in namespace:
        namespace[ANN_MAGIC] = annotations


DOC_MAGIC = '__doc__'
ANN_MAGIC = '__annotations__'
SILENT_MAGIC = {'__module__', '__qualname__', ANN_MAGIC}
OVERRIDABLE_MAGIC = SILENT_MAGIC | {DOC_MAGIC}
BUILTIN_DECORATORS = staticmethod, classmethod, property


class FactoryLayer(CallableLayer):
    def __init__(self, container: EdgesBag, properties: Iterable[str], special_methods: Iterable[str]):
        self._special_methods = set(special_methods)
        super().__init__(container, properties)

    def __getattribute__(self, name):
        if name in super().__getattribute__('_special_methods'):
            raise AttributeError(f'"{name}" is accessible only through the class object, not its instance')

        return super().__getattribute__(name)

    def __repr__(self):
        kls = type(self)
        if hasattr(kls, '__qualname__'):
            args = ', '.join(kls.__signature__.parameters if hasattr(kls, '__signature__') else [])
            return f'{kls.__qualname__}({args})'
        return super(type(self), self).__repr__()


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
        self.forward_inherit = set()
        self.backward_inherit = set()
        # metadata
        self.property_names = set()
        self.special_methods = {}
        # names of persistent nodes
        # TODO move it somewhere
        self.persistent_names = set()
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
        # TODO: each output node must be associated to exactly 1 edge
        for x in [self.inputs, self.outputs, self.backward_inputs, self.backward_outputs]:
            x.freeze()

    @classmethod
    def make_scope(cls, namespace: MultiDict) -> dict:
        factory = cls(namespace)
        signature = factory.get_init_signature()
        allow_positional = len(signature.parameters)

        def __init__(*args, **kwargs):
            assert args
            self, *args = args
            if (allow_positional and len(args) > 2) or (not allow_positional and len(args) > 1):
                raise TypeError('This constructor accepts only keyword arguments.')

            arguments = signature.bind_partial(*args, **kwargs)
            arguments.apply_defaults()
            FactoryLayer.__init__(
                self, factory.build(arguments.arguments),
                factory.property_names, factory.special_methods
            )

        __init__.__signature__ = signature
        scope = {
            '__init__': __init__, '__signature__': signature,
            DOC_MAGIC: factory.docstring,
            **factory.special_methods,
        }
        return add_quals(scope, namespace)

    @staticmethod
    def validate_before_mixins(namespace: MultiDict):
        pass

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
        for name, value in self.scope.get(ANN_MAGIC, [{}])[0].items():
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
            if isinstance(value, BUILTIN_DECORATORS):
                raise FieldError(f"{type(value).__name__} objects can't be private ({name}).")

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

            if isinstance(value, (classmethod, staticmethod)):
                if name in self.special_methods:
                    raise FieldError(f'"{name}" is already used')
                self.special_methods[name] = value
                continue
            if isinstance(value, property):
                raise FieldError(f'{name}: "property" objects are not supported, use the "meta" decorator instead')

            assert not isinstance(value, BUILTIN_DECORATORS)
            if not is_callable(value):
                raise FieldError(f'The type of the "{name}" field cannot be determined')

            # TODO: probably should return the decorators and not just their classes
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
            assert not any(issubclass(d, RuntimeAnnotation) for d in decorators)

            edge_annotations = {d for d in decorators if issubclass(d, EdgeAnnotation)}
            if len(edge_annotations) > 1:
                raise FieldError(f'The field "{name}" has too many "EdgeAnnotation" decorators')

            if edge_annotations:
                ann, = edge_annotations
                edge = ann.build(func, inputs, output)
                decorators.remove(ann)
            else:
                edge = FunctionEdge(func, len(inputs))

            assert not decorators, decorators
            self.edges.append(edge.bind(inputs, output))

    def _get_constant_edges(self, arguments: dict):
        for name, value in arguments.items():
            yield ConstantEdge(value).bind([], self.arguments[name])

    def get_init_signature(self):
        kind = inspect.Parameter.POSITIONAL_OR_KEYWORD if len(self.defaults) == 1 else inspect.Parameter.KEYWORD_ONLY
        return inspect.Signature([
            inspect.Parameter(name, kind, default=value)
            for name, value in self.defaults.items()
        ])

    def build(self, arguments: dict) -> TransformContainer:
        diff = list(set(self.arguments) - set(arguments))
        if diff:
            raise TypeError(f'Missing required arguments: {diff}.')

        logger.info(
            'Compiling layer. Inputs: %s, Outputs: %s, BackwardInputs: %s, BackwardOutputs: %s',
            list(self.inputs), list(self.outputs), list(self.backward_inputs), list(self.backward_outputs),
        )
        return TransformContainer(
            list(self.inputs.values()), list(self.outputs.values()),
            self.edges + list(self._get_constant_edges(arguments)),
            list(self.backward_inputs.values()), list(self.backward_outputs.values()),
            optional_nodes=self.optional_names, persistent_nodes=self.persistent_names,
            forward_virtual=self.forward_inherit, backward_virtual=self.backward_inherit,
        )


class SourceFactory(GraphFactory):
    @staticmethod
    def validate_before_mixins(namespace: MultiDict):
        duplicates = {name for name, values in namespace.groups() if len(values) > 1}
        if duplicates:
            raise TypeError(f'Duplicated methods found: {duplicates}')

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

    # TODO: move to class method
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

    def _after_collect(self):
        value, valid = normalize_inherit(self.forward_inherit, self.outputs)
        if not valid:
            raise ValueError(f'"__inherit__" can be either True, a string, or a sequence of strings, but got {value}')

        self.backward_inherit = self.forward_inherit = value

    def _validate_inputs(self, inputs: NodeTypes) -> NodeTypes:
        return inputs

    def _process_inherit(self, value):
        # save this value for final step
        self.forward_inherit = value
