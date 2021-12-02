from typing import Callable

from .nodes import NodeModifier


def format_arguments(blocks):
    if not blocks:
        return '()'

    args = ',\n'.join(map(str, blocks))
    args = '    '.join(args.splitlines(keepends=True))
    return f'(\n    {args}\n)'


def replace_annotation(func: Callable, annotation, *args, **kwargs):
    # unwrap
    modifiers = []
    while isinstance(annotation, NodeModifier):
        modifiers.append(type(annotation))
        annotation = annotation.node
    # need the `isinstance` part for faulty annotations, such as np.array
    if isinstance(annotation, type) and issubclass(annotation, NodeModifier):
        modifiers.append(annotation)
        annotation = None

    node = func(annotation, *args, **kwargs)

    # wrap back
    for modifier in modifiers:
        node = modifier(node)
    return node
