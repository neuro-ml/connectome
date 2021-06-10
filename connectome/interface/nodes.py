import inspect
import logging
from typing import Callable, Sequence, Set, Type

from ..engine.base import Node
from .decorators import NodeAnnotation, FactoryAnnotation, Inverse, Positional

logger = logging.getLogger(__name__)


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


class NodeType:
    __slots__ = 'name',

    def __init__(self, name: str):
        self.name = name


class Input(NodeType):
    pass


class Output(NodeType):
    pass


class InverseInput(NodeType):
    pass


class InverseOutput(NodeType):
    pass


class Parameter(NodeType):
    pass


# TODO: remove this
Local = Output
NodeTypes = Sequence[NodeType]


def is_private(name: str):
    return name.startswith('_')


def is_callable(value):
    return callable(value) or isinstance(value, FactoryAnnotation)


def infer_nodes(name: str, func: Callable, decorators: Set[Type[FactoryAnnotation]]):
    inputs = []

    # direction
    decorators = set(decorators)
    default_input = InverseInput if Inverse in decorators else Input
    if is_private(name):
        if Inverse in decorators:
            raise ValueError(f"Private fields ({name}) can't be inverse")
        output = Parameter(name)
    else:
        output = InverseOutput(name) if Inverse in decorators else Output(name)
    decorators.discard(Inverse)

    # first positional
    signature = list(inspect.signature(func).parameters.values())
    has_positional = Positional in decorators
    decorators.discard(Positional)
    if signature:
        parameter = signature[0]
        assert parameter.default == parameter.empty, parameter
        if parameter.kind == parameter.POSITIONAL_ONLY or has_positional:
            signature = signature[1:]
            inputs.append(default_input(name))
    else:
        if has_positional:
            raise ValueError('The "positional" can\'t be used with a function without arguments')

    # rest
    inputs.extend(signature_to_types(signature, default_input, name))

    assert not any(issubclass(x, NodeAnnotation) for x in decorators)
    return inputs, output, decorators


def signature_to_types(signature, default_input, field_name):
    inputs = []
    for parameter in signature:
        assert parameter.default == parameter.empty, parameter
        assert parameter.kind == parameter.POSITIONAL_OR_KEYWORD, parameter

        arg, annotation = parameter.name, parameter.annotation
        if isinstance(annotation, NodeType):
            raise ValueError(f'Invalid argument "{arg}" annotation ({annotation}) for field "{field_name}"')
        # need the `isinstance` part for faulty annotations, such as np.array
        if isinstance(annotation, type) and issubclass(annotation, NodeType):
            node = annotation(arg)
        elif is_private(arg):
            node = Parameter(arg)
        else:
            node = default_input(arg)

        inputs.append(node)

    return inputs


def to_argument(name):
    assert name.startswith('_')
    return name[1:]
