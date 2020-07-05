from connectome.blocks import ValueEdge, FunctionEdge, CustomLayer, Pipeline
from connectome.engine import Node, Layer, Edge
from connectome.utils import extract_signature


class BaseBlock:
    _methods: dict
    _layer: Layer

    # just to silence some linters
    def __init__(self, **kwargs):
        pass

    def __getattr__(self, name):
        return self._methods[name]


class Chain(BaseBlock):
    def __init__(self, *layers: BaseBlock):
        super().__init__()
        self._layer = Pipeline(*(layer._layer for layer in layers))
        self._methods = {k.name: getattr(self._layer, k.name) for k in self._layer.outputs}


def collect_nodes(scope):
    allowed_magic = {'__module__', '__qualname__'}
    outputs, parameters, arguments = {}, {}, {}

    # gather nodes
    for name, value in scope.items():
        if name.startswith('__'):
            if name in allowed_magic:
                continue

            # TODO
            raise ValueError(name)

        if name.startswith('_'):
            if isinstance(value, staticmethod):
                parameters[name] = Node(name)
            else:
                arguments[name] = Node(name)
        else:
            assert isinstance(value, staticmethod)
            outputs[name] = Node(name)

    return outputs, parameters, arguments


def make_init(inputs, outputs, edges, arguments):
    # TODO: signature
    def __init__(self, **kwargs):
        _edges = tuple(edges + [ValueEdge(arguments[k], v) for k, v in kwargs.items()])
        _layer = CustomLayer(inputs, list(outputs.values()), _edges)
        self._layer = _layer
        self._methods = {k: getattr(_layer, k) for k in outputs}

    return __init__


class SourceBase(type):
    def __new__(mcs, class_name, bases, namespace):
        # TODO: check magic, duplicates

        edges = []
        identifier = Node('id')
        outputs, parameters, arguments = collect_nodes(namespace)

        def under(n: str):
            if n in parameters:
                return parameters
            return arguments[n]

        # TODO: detect cycles, unused nodes etc

        for name, value in namespace.items():
            if name not in parameters and name not in outputs:
                continue

            assert isinstance(value, staticmethod)
            value = value.__func__
            # TODO: check signature
            names = extract_signature(value)

            # TODO: this is a mess
            if name.startswith('_'):
                out = parameters[name]
                in_ = []
                if names and not names[0].startswith('_'):
                    in_.append(identifier)
                    names = names[1:]

                in_.extend([under(n) for n in names])

            else:
                assert len(names) >= 1
                out = outputs[name]
                in_ = [identifier] + [under(n) for n in names[1:]]

            edges.append(FunctionEdge(value, in_, out))

        scope = {'__init__': make_init([identifier], outputs, edges, arguments)}
        return super().__new__(mcs, class_name, bases, scope)


class TransformBase(type):
    def __new__(mcs, class_name, bases, namespace):
        def get_input(n: str):
            if n.startswith('_'):
                if n in parameters:
                    return parameters[n]
                return arguments[n]
            if n not in inputs:
                inputs[n] = Node(n)
            return inputs[n]

        edges = []
        inputs = {}
        outputs, parameters, arguments = collect_nodes(namespace)

        # TODO: detect cycles, unused parameter-funcs

        for name, value in namespace.items():
            if name not in parameters and name not in outputs:
                continue

            assert isinstance(value, staticmethod)
            value = value.__func__
            # TODO: check signature
            names = extract_signature(value)

            if name.startswith('_'):
                out = parameters[name]
                in_ = list(map(get_input, names))

            else:
                out = outputs[name]
                # TODO: more flexibility
                in_ = [get_input(name)] + list(map(get_input, names[1:]))

            edges.append(FunctionEdge(value, in_, out))

        scope = {'__init__': make_init(list(inputs.values()), outputs, edges, arguments)}
        return super().__new__(mcs, class_name, bases, scope)


class Source(BaseBlock, metaclass=SourceBase):
    pass


class Transform(BaseBlock, metaclass=TransformBase):
    pass
