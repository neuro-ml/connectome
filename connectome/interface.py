from connectome.engine import Node, Block, FunctionEdge, ValueEdge
from connectome.utils import extract_signature


class SourceBase(type):
    def __new__(mcs, class_name, bases, namespace):
        # TODO: check magic, duplicates

        def __init__(self, **kwargs):
            self._edges = edges + [ValueEdge(arguments[k], v) for k, v in kwargs.items()]
            self._inputs = [identifier]
            self._outputs = list(outputs.values())

        scope = {'__init__': __init__}
        edges = []
        identifier = Node('id')
        inputs, outputs, parameters, arguments = {}, {}, {}, {}

        # gather parameter-functions
        for name, value in namespace.items():
            if name.startswith('__'):
                # TODO
                continue

            if name.startswith('_'):
                if isinstance(value, staticmethod):
                    parameters[name] = Node(name)
                else:
                    arguments[name] = Node(name)
            else:
                outputs[name] = Node(name)

        def under(n: str):
            if n in parameters:
                return parameters
            return arguments[n]

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

        return super().__new__(mcs, class_name, bases, scope)


class TransformBase(type):
    def __new__(mcs, class_name, bases, namespace):
        def __init__(self):
            self._edges = edges
            self._inputs = list(inputs.values())
            self._outputs = list(outputs.values())

        def get_input(n: str):
            if n.startswith('_'):
                return parameters[n]
            if n not in inputs:
                inputs[n] = Node(n)
            return inputs[n]

        scope = {'__init__': __init__}
        edges = []
        inputs, outputs, parameters = {}, {}, {}

        # gather parameter-functions
        for name, value in namespace.items():
            if name.startswith('__'):
                # TODO
                continue

            if name.startswith('_'):
                parameters[name] = Node(name)
            else:
                outputs[name] = Node(name)

        # TODO: detect cycles, unused parameter-funcs

        for name, value in namespace.items():
            if name.startswith('__'):
                # TODO
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
                in_ = [get_input(name)] + list(map(get_input, names[1:]))

            edges.append(FunctionEdge(value, in_, out))

        return super().__new__(mcs, class_name, bases, scope)


class Source(Block, metaclass=SourceBase):
    pass


class Transform(Block, metaclass=TransformBase):
    pass
