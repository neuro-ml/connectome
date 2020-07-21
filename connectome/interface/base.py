from typing import Callable

from ..engine.edges import FunctionEdge
from ..layers import PipelineLayer, Layer, EdgesBag, SwitchLayer
from ..utils import MultiDict, node_to_dict
from .factory import SourceFactory, TransformFactory


class BaseBlock:
    _layer: Layer


class CallableBlock(BaseBlock):
    _layer: EdgesBag

    def __getattr__(self, name):
        return self._layer.get_forward_method(name)

    def wrap_predict(self, predict: Callable, forward_output_names, backward_input_name):
        outputs = node_to_dict(self._layer.get_outputs())
        backward_inputs = node_to_dict(self._layer.get_backward_inputs())
        backward_outputs = node_to_dict(self._layer.get_backward_outputs())

        cross_pipe_edge = FunctionEdge(predict, [outputs[name] for name in forward_output_names],
                                       backward_inputs[backward_input_name])

        caller = Graph().compile_graph([backward_outputs[backward_input_name]], self._layer.get_inputs(),
                                       list(self._layer.get_edges()) + [cross_pipe_edge])

        return caller


class Chain(CallableBlock):
    def __init__(self, head: CallableBlock, *tail: BaseBlock):
        super().__init__()
        self._layer: PipelineLayer = PipelineLayer(head._layer, *(layer._layer for layer in tail))

    def __getitem__(self, index):
        if isinstance(index, slice):
            # TODO exception
            assert index.step in [1, None]

            return FromLayer(self._layer.slice(index.start, index.stop))

        return FromLayer(self._layer.slice(index.start, index.stop))


class FromLayer(BaseBlock):
    def __init__(self, layer):
        super().__init__()
        self._layer = layer


class SourceBase(type):
    def __new__(mcs, class_name, bases, namespace):
        def __init__(*args, **kwargs):
            # TODO: error message
            self, = args
            # TODO: split into two objects: the first one holds the scope
            #  the second one compiles the layer
            factory = SourceFactory(namespace)
            signature = factory.get_init_signature()
            kwargs = signature.bind(**kwargs).kwargs
            # TODO: should only build if not called from super
            factory.build(kwargs)
            self._layer = factory.get_layer()

        return super().__new__(mcs, class_name, bases, {'__init__': __init__})


class TransformBase(type):
    @classmethod
    def __prepare__(mcs, *args):
        return MultiDict()

    def __new__(mcs, class_name, bases, namespace):
        def __init__(*args, **kwargs):
            # TODO: error message
            self, = args
            # TODO: split into two objects: the first one holds the scope
            #  the second one compiles the layer
            factory = TransformFactory(namespace)
            signature = factory.get_init_signature()
            kwargs = signature.bind(**kwargs).kwargs
            factory.build(kwargs)
            self._layer = factory.get_layer()

        return super().__new__(mcs, class_name, bases, {'__init__': __init__})


class Transform(CallableBlock, metaclass=TransformBase):
    pass


# TODO add inheritance
class Source(CallableBlock, metaclass=SourceBase):
    pass


class Merge(CallableBlock):
    def __init__(self, *blocks: CallableBlock):
        super().__init__()

        # FIXME: this won't work if there are more than 2 blocks
        idx_intersection = set.intersection(*[set(layer.ids()) for layer in blocks])
        if len(idx_intersection) > 0:
            raise RuntimeError('Datasets have same indices')

        def branch_selector(identifier):
            for idx, ds in enumerate(blocks):
                if identifier in ds.ids():
                    return idx

            raise ValueError(identifier)

        self._layer = SwitchLayer(branch_selector, *(s._layer for s in blocks))
