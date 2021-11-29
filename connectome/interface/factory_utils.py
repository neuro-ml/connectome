def to_argument(name):
    assert name.startswith('_')
    return name[1:]


def add_quals(scope, namespace):
    qualname = namespace.get('__qualname__', [None])[0]
    if qualname is not None:
        scope['__qualname__'] = qualname
    module = namespace.get('__module__', [None])[0]
    if module is not None:
        scope['__module__'] = module
    return scope
