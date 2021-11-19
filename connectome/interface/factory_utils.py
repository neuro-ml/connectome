import inspect
from typing import Set, Type

from .decorators import *
from .nodes import *


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


def unwrap_transform(value):
    decorators = []
    while isinstance(value, FactoryAnnotation):
        decorators.append(type(value))
        value = value.__func__

    if len(set(decorators)) != len(decorators):
        raise ValueError('Object has duplicated decorators')

    return value, set(decorators)


def add_quals(scope, namespace):
    qualname = namespace.get('__qualname__', [None])[0]
    if qualname is not None:
        scope['__qualname__'] = qualname
    module = namespace.get('__module__', [None])[0]
    if module is not None:
        scope['__module__'] = module
    return scope
