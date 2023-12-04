import inspect
import logging
import warnings
from typing import Any, Callable, Dict, Iterable, Type

from ..layer import Layer
from ..containers.reversible import normalize_inherit
from ..engine import ConstantEdge, Details, IdentityEdge
from ..exceptions import FieldError, GraphError
# from ..layers import CallableLayer, Layer
from ..utils import AntiSet, MultiDict
from .decorators import Meta, Optional, RuntimeAnnotation
from .edges import EdgeFactory, Function
from .factory_utils import add_quals, to_argument
from .nodes import (
    NodeStorage, Input, InverseInput, Parameter, InverseOutput, Output, NodeTypes, NodeType, Default, Intermediate,
    FinalNodeType, is_private
)

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


def is_detectable(value):
    return callable(value) or isinstance(value, EdgeFactory)


DOC_MAGIC = '__doc__'
ANN_MAGIC = '__annotations__'
SILENT_MAGIC = {'__module__', '__qualname__', ANN_MAGIC}
OVERRIDABLE_MAGIC = SILENT_MAGIC | {DOC_MAGIC}
BUILTIN_DECORATORS = staticmethod, classmethod, property


class GraphFactory:
    layer_cls: Type[Layer]

    def __init__(self, layer: str, scope: MultiDict):
        self.name = layer
        details = Details(layer)
        backward_details = Details(f'{layer}(backward)')
        self.scope = scope
        self.edges = []
        # layer inputs
        self.inputs = NodeStorage(details)
        self.backward_inputs = NodeStorage(backward_details)
        # layer outputs
        self.outputs = NodeStorage(details)
        self.backward_outputs = NodeStorage(backward_details)
        # __init__ arguments
        self.arguments = NodeStorage(details)
        # their defaults
        self.defaults = {}
        # outputs of transform parameters
        self.parameters = NodeStorage(details)
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
        self._intermediate_dispatch: Dict[Intermediate, Parameter] = {}
        self._intermediate_counter = 0
        self.magic_dispatch: Dict[str, Callable[[Any], Any]] = {DOC_MAGIC: self._process_doc}
        self._nodes_dispatch = {
            Input: self.inputs, Output: self.outputs, Parameter: self.parameters,
            InverseInput: self.backward_inputs, InverseOutput: self.backward_outputs,
        }
        self._before_collect()
        self._collect_nodes()
        self._after_collect()
        # TODO: each output node must be associated to exactly 1 edge
        for x in [self.parameters, self.inputs, self.outputs, self.backward_inputs, self.backward_outputs]:
            x.freeze()

    def prepare_layer_arguments(self, arguments: dict):
        raise NotImplementedError

    @classmethod
    def make_scope(cls, layer: str, namespace: MultiDict) -> dict:
        factory = cls(layer, namespace)
        signature = factory.get_init_signature()
        allow_positional = len(signature.parameters)

        def __init__(*args, **kwargs):
            assert args
            self, *args = args
            if (allow_positional and len(args) > 2) or (not allow_positional and len(args) > 1):
                raise TypeError('This constructor accepts only keyword arguments.')

            arguments = signature.bind_partial(*args, **kwargs)
            arguments.apply_defaults()
            factory.layer_cls.__init__(
                self, *factory.prepare_layer_arguments(arguments.arguments)
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
        annotations = self.scope.get(ANN_MAGIC, [{}])[0]
        for name, value in annotations.items():
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
            # we have 2 edge cases here:
            is_argument = False
            if callable(value):
                #  1. a callable argument without annotation: x = some_func
                qualname = getattr(value, '__qualname__', '')
                # if we can't detect this better not to annoy the user
                inside_body = None
                if qualname.count('.') >= 1:
                    scope, func_name = qualname.rsplit('.', 1)
                    # lambdas are a special case. don't know what to do with them
                    if func_name != '<lambda>':
                        inside_body = scope.endswith(self.name)

                if name not in annotations:
                    if inside_body is not None and not inside_body:
                        warnings.warn(
                            f'The parameter {name} is defined outside of the class body. Are you trying to pass '
                            f'a default value for an argument? If so, add a type annotation: "{name}: Callable = ..."',
                            UserWarning,
                        )
                # a function defined inside the body, which also has a type annotation
                else:
                    is_argument = True
                    if inside_body is not None and inside_body:
                        warnings.warn(
                            f'The default value for the argument {name} is a function, defined inside of the '
                            f'class body. Did you forget to remove the type annotation?',
                            UserWarning,
                        )

            if is_argument or not is_detectable(value) or value is inspect.Parameter.empty:
                constants.add(name)
                arg_name = to_argument(name)
                self.edges.append(IdentityEdge().bind(self.arguments[arg_name], self.parameters[name]))
                self.defaults[arg_name] = value

        self.arguments.freeze()
        assert set(self.arguments) == set(self.defaults), (self.arguments, self.defaults)

        # gather special methods such as staticmethod
        for name, group in self.scope.groups():
            if name.startswith('__') or name in constants:
                continue
            if not any(isinstance(value, BUILTIN_DECORATORS) for value in group):
                continue

            if len(group) > 1:
                raise FieldError(f'Special method "{name}" has multiple definitions')
            value, = group
            if isinstance(value, property):
                raise FieldError(f'{name}: "property" objects are not supported, use the "meta" decorator instead')
            self.special_methods[name] = value

        # gather, inputs, outputs and their edges
        for name, value in self.scope.items():
            if name.startswith('__') or name in constants or name in self.special_methods:
                continue

            graph_annotations = []
            while isinstance(value, RuntimeAnnotation):
                graph_annotations.append(type(value))
                value = value.__func__

            # TODO: check duplicates
            if Meta in graph_annotations:
                self.property_names.add(name)
                graph_annotations.remove(Meta)
            if Optional in graph_annotations:
                self.optional_names.add(name)
                graph_annotations.remove(Optional)
            assert not graph_annotations, graph_annotations

            if not is_detectable(value):
                raise FieldError(f'The type of the "{name}" field cannot be determined: {type(value)}')

            if not isinstance(value, EdgeFactory):
                value = Function.decorate(value)

            for edge, inputs, output in value.build(name):
                # factory types
                inputs = [self._normalize_input(name, node) for node in inputs]
                output = self._normalize_output(name, output)
                # engine types
                inputs = list(map(self._type_to_node, self._validate_inputs(inputs)))
                output = self._type_to_node(output)
                # finally add a new edge
                self.edges.append(edge.bind(inputs, output))

    def _normalize_input(self, name, node: NodeType):
        if isinstance(node, Intermediate):
            node = self._get_intermediate(node)
        if isinstance(node, Default):
            node = Parameter(node.name) if is_private(node.name) else Input(node.name)
        if not isinstance(node, FinalNodeType):
            raise FieldError(f'Input node for "{name}" has incorrect type: {type(node)}')
        return node

    def _normalize_output(self, name, node: NodeType):
        if isinstance(node, Intermediate):
            node = self._get_intermediate(node)
        if isinstance(node, Default):
            node = Parameter(node.name) if is_private(node.name) else Output(node.name)
        if not isinstance(node, (Output, InverseOutput, Parameter)):
            raise FieldError(f'Output node for "{name}" has incorrect type: {type(node)}')
        return node

    def _get_intermediate(self, node):
        if node not in self._intermediate_dispatch:
            self._intermediate_counter += 1
            name = f'${self._intermediate_counter}'
            names = {x.name for x in self._intermediate_dispatch.values()}
            while name in names:
                self._intermediate_counter += 1
                name = f'${self._intermediate_counter}'

            self._intermediate_dispatch[node] = Parameter(name)

        return self._intermediate_dispatch[node]

    def _get_constant_edges(self, arguments: dict):
        for name, value in arguments.items():
            yield ConstantEdge(value).bind([], self.arguments[name])

    def get_init_signature(self):
        kind = inspect.Parameter.POSITIONAL_OR_KEYWORD if len(self.defaults) == 1 else inspect.Parameter.KEYWORD_ONLY
        return inspect.Signature([
            inspect.Parameter(name, kind, default=value)
            for name, value in self.defaults.items()
        ])


def items_to_container(items, layer_type: Type, factory_cls: Type[GraphFactory], **scope):
    if isinstance(items, dict):
        items = items.items()

    local = MultiDict()
    for name in scope:
        local[name] = scope[name]
    for name, value in items:
        if not is_detectable(value) and not isinstance(value, RuntimeAnnotation):
            raise TypeError(
                f'All arguments must be callable. "{name}" is not callable but {type(value)}'
            )

        local[name] = value

    factory = factory_cls(layer_type.__name__, local)
    if factory.special_methods:
        raise TypeError(f"This constructor doesn't accept special methods: {tuple(factory.special_methods)}")
    return factory.prepare_layer_arguments({})
