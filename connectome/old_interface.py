from typing import Sequence, Callable
from functools import wraps

from .engine.edges import FunctionEdge
from .layers import PipelineLayer, MuxLayer, Layer, EdgesBag, SwitchLayer
from .utils import MultiDict, extract_signature, node_to_dict
from .factory import SourceFactory, TransformFactory


def collect_nodes(scope):
    allowed_magic = {'__module__', '__qualname__'}
    outputs, parameters, arguments, defaults, backwards = {}, {}, {}, {}, {}

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
            defaults[name] = value
        elif is_output(name, value):
            outputs[name] = Node(name)
        else:
            raise RuntimeError

    return outputs, parameters, arguments, defaults


class SourceBase(type):
    def __new__(mcs, class_name, bases, namespace):
        # TODO: check magic, duplicates

        edges = []
        identifier = Node('id')
        forbidden_methods = ['id']

        outputs, parameters, arguments, defaults = collect_nodes(namespace)

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
                if attr_name in forbidden_methods:
                    raise RuntimeError(f"'{attr_name}' can not be used as name of method")
                else:
                    assert len(func_input_names) >= 1
                    if attr_name == 'ids':
                        input_nodes = [get_related_nodes(n) for n in func_input_names]
                    else:
                        input_nodes = [identifier] + [get_related_nodes(n) for n in func_input_names[1:]]

                out_node = outputs[attr_name]
            else:
                raise RuntimeError

            edges.append(FunctionEdge(attr_func, input_nodes, out_node))

        # check for required nodes:
        if 'ids' not in outputs:
            raise RuntimeError("'ids' method is required")

        namespace = {'__init__': make_init([identifier], list(outputs.values()), edges, arguments, defaults)}
        return super().__new__(mcs, class_name, bases, namespace)


def process_methods(scope):
    allowed_magic = {'__module__', '__qualname__'}

    arguments = {}
    parameters = {}
    backward_methods = {}
    forward_methods = {}

    for name, value in scope.items():

        if name.startswith('__'):
            assert name in allowed_magic

        elif is_parameter(name, value):
            func = value.__func__
            parameters[name] = (func, extract_signature(func))

        elif is_argument(name, value):
            arguments[name] = value

        elif is_output(name, value):
            func = value.__func__
            forward_methods[name] = (func, extract_signature(func))

        elif is_backward(name, value):
            func = value.__func__
            backward_methods[name] = (func, extract_signature(func))

        elif isinstance(value, list):
            value_forwards = []
            value_backwards = []

            for func in value:
                if is_backward(name, func):
                    value_backwards.append(func)
                elif is_output(name, func):
                    value_forwards.append(func)
                else:
                    raise RuntimeError

            assert len(value_forwards) == 1
            func = value_forwards[0].__func__
            forward_methods[name] = (func, extract_signature(func))

            if len(value_backwards) == 1:
                func = value_backwards[0].__func__
                backward_methods[name] = (func, extract_signature(func))
            else:
                # TODO replace by exception
                assert len(value_backwards) == 0
        else:
            # TODO add more information
            raise RuntimeError

    # TODO check that there is no intersection in parameters and arguments names
    return forward_methods, backward_methods, parameters, arguments


def build_transform_namespace(namespace):
    forward_methods, backward_methods, parameters, arguments = process_methods(namespace)

    edges = []
    argument_values = {key: value for key, value in arguments.items()}

    def get_nodes_name_map(dct: dict):
        return {n: Node(n) for n, _ in dct.items()}

    argument_nodes = get_nodes_name_map(arguments)
    parameter_nodes = get_nodes_name_map(parameters)

    # sequence must have equal length and equal set of names
    inputs = get_nodes_name_map(forward_methods)
    outputs = get_nodes_name_map(forward_methods)

    backward_inputs = get_nodes_name_map(backward_methods)
    backward_outputs = get_nodes_name_map(backward_methods)

    def get_related_nodes(key: str, backward=False):
        if check_pattern(key):
            if key in parameter_nodes:
                return parameter_nodes[key]
            else:
                return argument_nodes[key]
        else:
            if backward:
                return backward_inputs[key]
            else:
                return inputs[key]

    for func_name, (func, attr_names) in forward_methods.items():
        output_node = outputs[func_name]
        cur_inputs = [get_related_nodes(func_name)] + list(map(get_related_nodes, attr_names[1:]))
        edges.append(FunctionEdge(func, cur_inputs, output_node))

    for func_name, (func, attr_names) in parameters.items():
        output_node = parameter_nodes[func_name]
        cur_inputs = list(map(get_related_nodes, attr_names))
        edges.append(FunctionEdge(func, cur_inputs, output_node))

    for func_name, (func, attr_names) in backward_methods.items():
        output_node = backward_outputs[func_name]
        cur_inputs = [get_related_nodes(func_name, backward=True)]
        cur_inputs += [get_related_nodes(n, backward=True) for n in attr_names[1:]]
        edges.append(FunctionEdge(func, cur_inputs, output_node))

    print(argument_nodes)
    scope = {
        '__init__': make_init(list(inputs.values()), list(outputs.values()), edges, argument_nodes,
                              arguments, list(backward_inputs.values()), list(backward_outputs.values()))}
    return scope