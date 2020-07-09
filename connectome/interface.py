from .edges import ValueEdge, FunctionEdge
from .layers import PipelineLayer, CustomLayer, MuxLayer
from .engine import Node, Layer
from .utils import extract_signature


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
        self._layer = PipelineLayer(*(layer._layer for layer in layers))
        self._methods = self._layer.get_output_node_methods()


class FromLayer(BaseBlock):
    def __init__(self, layer):
        super().__init__()
        self._layer = layer
        self._methods = self._layer.get_output_node_methods()


def check_pattern(name: str):
    return name.startswith('_')


def is_argument(name: str, value):
    return check_pattern(name) and not isinstance(value, staticmethod)


def is_parameter(name: str, value):
    return check_pattern(name) and isinstance(value, staticmethod)


def is_output(name: str, value):
    return not check_pattern(name) and isinstance(value, staticmethod)


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


def make_init(inputs, outputs, edges, arguments):
    # TODO: signature
    def __init__(self, **kwargs):
        kwargs = {k if check_pattern(k) else f'_{k}': v for k, v in kwargs.items()}
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
        forbidden_methods = ['id']
        ids_param_name = '_ids_arg'

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
                if func_input_names and not check_pattern(func_input_names[0]):
                    input_nodes.append(identifier)
                    func_input_names = func_input_names[1:]

                input_nodes.extend([get_related_nodes(name) for name in func_input_names])

            elif is_output(attr_name, attr_value):
                # TODO replace by exceptions + add more information
                if attr_name is 'ids':
                    assert len(func_input_names) == 1, func_input_names[0] == ids_param_name
                    input_nodes = [get_related_nodes(func_input_names[0])]
                elif attr_name in forbidden_methods:
                    raise RuntimeError(f"'{attr_name}' can not be used as name of method")
                else:
                    assert len(func_input_names) >= 1
                    input_nodes = [identifier] + [get_related_nodes(n) for n in func_input_names[1:]]

                out_node = outputs[attr_name]
            else:
                raise RuntimeError

            edges.append(FunctionEdge(attr_func, input_nodes, out_node))

        # check for required nodes:
        if 'ids' not in outputs:
            raise RuntimeError("'ids' method is required")

        scope = {'__init__': make_init([identifier], outputs, edges, arguments)}
        return super().__new__(mcs, class_name, bases, scope)


class TransformBase(type):
    def __new__(mcs, class_name, bases, namespace):
        def get_related_nodes(name: str):
            if check_pattern(name):
                if name in parameters:
                    return parameters[name]
                else:
                    return arguments[name]
            if name not in inputs:
                inputs[name] = Node(name)
            return inputs[name]

        edges = []
        inputs = {}
        outputs, parameters, arguments = collect_nodes(namespace)

        # TODO: detect cycles, unused parameter-funcs

        for attr_name, attr_value in namespace.items():
            if attr_name not in parameters and attr_name not in outputs:
                continue

            # TODO: check signature
            attr_func = attr_value.__func__
            names = extract_signature(attr_func)

            if is_parameter(attr_name, attr_value):
                output_node = parameters[attr_name]
                input_nodes = list(map(get_related_nodes, names))
            elif is_output(attr_name, attr_value):
                output_node = outputs[attr_name]
                # TODO: more flexibility
                input_nodes = [get_related_nodes(attr_name)] + list(map(get_related_nodes, names[1:]))
            else:
                raise RuntimeError

            edges.append(FunctionEdge(attr_func, input_nodes, output_node))

        scope = {'__init__': make_init(list(inputs.values()), outputs, edges, arguments)}
        return super().__new__(mcs, class_name, bases, scope)


class Transform(BaseBlock, metaclass=TransformBase):
    pass


# TODO add inheritance
class Source(BaseBlock, metaclass=SourceBase):
    _ids_arg = None

    @staticmethod
    def ids(_ids_arg):
        return ()


class Merge(BaseBlock):
    def __init__(self, first_ds: Source, second_ds: Source):
        super().__init__()

        self.first_ds = first_ds
        self.second_ds = second_ds

        self.first_layer = first_ds._layer
        self.second_layer = second_ds._layer

        ids_intersection = set(first_ds.ids()).intersection(set(second_ds.ids()))
        if len(ids_intersection) > 0:
            raise RuntimeError('Datasets have same indices')

        # TODO remove this trash

        def index_selector(idx):
            if idx in first_ds.ids():
                return 0
            return 1

        self._layer = MuxLayer(index_selector, self.first_layer, self.second_layer)
        self._methods = self._layer.get_output_node_methods()
