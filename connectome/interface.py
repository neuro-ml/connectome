from connectome.blocks import ValueEdge, FunctionEdge, CustomLayer, Pipeline
from connectome.engine import Node, Layer
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
        # TODO replace by property
        self._methods = self._layer.get_output_node_methods()


def is_argument(name: str, value):
    if name.startswith('_') and not isinstance(value, staticmethod):
        return True
    else:
        return False


def is_parameter(name: str, value):
    if name.startswith('_') and isinstance(value, staticmethod):
        return True
    else:
        return False


def is_output(name: str, value):
    if not name.startswith('_') and isinstance(value, staticmethod):
        return True
    else:
        return False


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

        if is_parameter(name, value):
            parameters[name] = Node(name)
        elif is_argument(name, value):
            arguments[name] = Node(name)
        elif is_output(name, value):
            outputs[name] = Node(name)
        else:
            raise RuntimeError

    return outputs, parameters, arguments


def make_init(inputs, outputs, edges, arguments, parameters):
    # TODO: signature
    def __init__(self, **kwargs):
        kwargs = {k if k.startswith('_') else f'_{k}': v for k, v in kwargs.items()}
        _edges = tuple(edges + [ValueEdge(arguments[k], v) for k, v in kwargs.items()])
        _layer = CustomLayer(inputs, list(outputs.values()), _edges)
        self._layer = _layer
        self._methods = _layer.get_output_node_methods()

    return __init__


class SourceBase(type):
    def __new__(mcs, class_name, bases, namespace):
        # TODO: check magic, duplicates

        edges = []
        identifier = Node('id')
        outputs, parameters, arguments = collect_nodes(namespace)

        def get_related_nodes(name: str):
            if name in parameters:
                return parameters[name]
            else:
                return arguments[name]

        # TODO: detect cycles, unused nodes etc

        for attr_name, attr_value in namespace.items():
            if attr_name not in parameters and attr_name not in outputs:
                continue

            # TODO: check signature
            attr_func = attr_value.__func__
            func_input_names = extract_signature(attr_func)

            if is_parameter(attr_name, attr_value):
                out_node = parameters[attr_name]
                input_nodes = []
                if func_input_names and not func_input_names[0].startswith('_'):
                    input_nodes.append(identifier)
                    func_input_names = func_input_names[1:]

                input_nodes.extend([get_related_nodes(name) for name in func_input_names])

            elif is_output(attr_name, attr_value):
                assert len(func_input_names) >= 1
                out_node = outputs[attr_name]
                input_nodes = [identifier] + [get_related_nodes(n) for n in func_input_names[1:]]
            else:
                raise RuntimeError

            edges.append(FunctionEdge(attr_func, input_nodes, out_node))

        scope = {'__init__': make_init([identifier], outputs, edges, arguments, parameters)}
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

        scope = {'__init__': make_init(list(inputs.values()), outputs, edges, arguments, parameters)}
        return super().__new__(mcs, class_name, bases, scope)


class Source(BaseBlock, metaclass=SourceBase):
    pass


class Transform(BaseBlock, metaclass=TransformBase):
    pass
